package index

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/michmich112/conduit-plugin/listing"
)

func lexicalListing(id, pubkey, d, title, body, geohash, status string, created int64) listing.Listing {
	l := listing.Listing{
		Coord:     listing.CoordOf(listing.KindProduct, pubkey, d),
		EventID:   id,
		Kind:      listing.KindProduct,
		PubKey:    pubkey,
		DTag:      d,
		Title:     title,
		Body:      body,
		Status:    status,
		CreatedAt: created,
	}
	if geohash != "" {
		l.HasGeo = true
		l.Geohash = geohash
	}
	l.TextHash = l.ComputeTextHash()
	return l
}

func insertLexicalFlood(tb testing.TB, s *sqlStore, count int) string {
	tb.Helper()
	ctx := context.Background()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		tb.Fatal(err)
	}
	defer tx.Rollback()
	for i := 0; i < count; i++ {
		coord := listing.CoordOf(listing.KindProduct, pk, fmt.Sprintf("flood-%05d", i))
		_, err := tx.ExecContext(ctx, `INSERT INTO listings
(coord, event_id, kind, pubkey, d_tag, status, title, body, text_hash, created_at, updated_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, coord, fmt.Sprintf("flood-id-%05d", i), listing.KindProduct,
			pk, fmt.Sprintf("flood-%05d", i), listing.StatusActive, "Copper kettle", "Copper kitchen item", "hash",
			int64(100000+i), int64(100000+i))
		if err != nil {
			tb.Fatal(err)
		}
	}
	other := "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	otherCoord := listing.CoordOf(listing.KindProduct, other, "other")
	_, err = tx.ExecContext(ctx, `INSERT INTO listings
(coord, event_id, kind, pubkey, d_tag, status, title, body, text_hash, created_at, updated_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, otherCoord, "other-id", listing.KindProduct,
		other, "other", listing.StatusActive, "Copper kettle", "Copper kitchen item", "hash", int64(1), int64(1))
	if err != nil {
		tb.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		tb.Fatal(err)
	}
	return otherCoord
}

func TestTursoLexicalFloodRetainsOtherAuthorAndFills(t *testing.T) {
	ctx := context.Background()
	st, err := OpenTurso(ctx, filepath.Join(t.TempDir(), "flood.db"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	s := st.(*sqlStore)
	otherCoord := insertLexicalFlood(t, s, 10000)
	start := time.Now()
	got, err := s.lexicalCandidates(ctx, Query{Search: "copper"}, 5)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("10,001 matching rows, lexical retrieval %s", time.Since(start))
	if len(got) != 6 {
		t.Fatalf("want five global hits plus second author, got %d: %+v", len(got), got)
	}
	if got[5].EventID != "other-id" || got[5].Title == "" {
		t.Fatalf("other author omitted behind flood: %+v", got)
	}
	if err := st.MarkInactive(ctx, "", nil, []string{otherCoord}); err != nil {
		t.Fatal(err)
	}
	got, err = s.lexicalCandidates(ctx, Query{Search: "copper"}, 5)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 5 {
		t.Fatalf("single author should fill global candidates, got %d", len(got))
	}
}

func BenchmarkTursoLexicalFlood10k(b *testing.B) {
	ctx := context.Background()
	st, err := OpenTurso(ctx, filepath.Join(b.TempDir(), "flood-bench.db"), nil)
	if err != nil {
		b.Fatal(err)
	}
	defer st.Close()
	s := st.(*sqlStore)
	insertLexicalFlood(b, s, 10000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := s.lexicalCandidates(ctx, Query{Search: "copper"}, 20); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(10001, "matching-rows/op")
}

func BenchmarkTursoLexicalManyAuthors10k(b *testing.B) {
	ctx := context.Background()
	st, err := OpenTurso(ctx, filepath.Join(b.TempDir(), "many-authors-bench.db"), nil)
	if err != nil {
		b.Fatal(err)
	}
	defer st.Close()
	s := st.(*sqlStore)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < 10000; i++ {
		author := fmt.Sprintf("%064x", i%100+1)
		d := fmt.Sprintf("item-%05d", i)
		coord := listing.CoordOf(listing.KindProduct, author, d)
		_, err := tx.ExecContext(ctx, `INSERT INTO listings
(coord, event_id, kind, pubkey, d_tag, status, title, body, text_hash, created_at, updated_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, coord, d, listing.KindProduct, author, d,
			listing.StatusActive, "Copper kettle", "Copper kitchen item", "hash", int64(i+1), int64(i+1))
		if err != nil {
			b.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := s.lexicalCandidates(ctx, Query{Search: "copper"}, 20); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(10000, "matching-rows/op")
}

func TestTursoLexicalRetrievalAndEligibility(t *testing.T) {
	ctx := context.Background()
	st, err := OpenTurso(ctx, filepath.Join(t.TempDir(), "lexical.db"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	s := st.(*sqlStore)
	other := "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	items := []listing.Listing{
		lexicalListing("old-match", pk, "old", "Vintage copper kettle", "Handmade for tea", "9q8yy", listing.StatusActive, 100),
		lexicalListing("new-unrelated", pk, "new", "Running shoes", "Blue trainers", "9q8yy", listing.StatusActive, 900),
		lexicalListing("wrong-author", other, "other", "Copper pan", "Heavy duty", "9q8yy", listing.StatusActive, 500),
		lexicalListing("wrong-geo", pk, "far", "Copper lamp", "Desk lamp", "dr5ru", listing.StatusActive, 600),
		lexicalListing("inactive", pk, "inactive", "Copper cup", "Cup", "9q8yy", listing.StatusInactive, 700),
	}
	for _, item := range items {
		if err := st.Upsert(ctx, item); err != nil {
			t.Fatal(err)
		}
	}

	q := Query{Search: "copper", Kinds: []int{listing.KindProduct}, Authors: []string{pk}, GeoEnabled: true,
		GeoPrefixes: []string{"9q8"}, GeoMinPrefixLen: 2}
	got, err := s.lexicalCandidates(ctx, q, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].EventID != "old-match" || got[0].Rank != 1 || got[0].Score <= 0 {
		t.Fatalf("expected old query match only, got %+v", got)
	}
	q.Search = `"running" OR copper`
	got, err = s.lexicalCandidates(ctx, q, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("query syntax should be treated as terms, got %+v", got)
	}
	q.Search = "wordnotpresent"
	got, err = s.lexicalCandidates(ctx, q, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("unmatched query returned %+v", got)
	}

	newer := lexicalListing("replacement", pk, "old", "Vintage silver kettle", "Handmade for tea", "9q8yy", listing.StatusActive, 1000)
	if err := st.Upsert(ctx, newer); err != nil {
		t.Fatal(err)
	}
	q.Search = "copper"
	got, err = s.lexicalCandidates(ctx, q, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("replaced text still searchable: %+v", got)
	}
	q.Search = "silver"
	got, err = s.lexicalCandidates(ctx, q, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].EventID != "replacement" {
		t.Fatalf("replacement text missing: %+v", got)
	}
	if err := st.MarkInactive(ctx, pk, nil, []string{newer.Coord}); err != nil {
		t.Fatal(err)
	}
	got, err = s.lexicalCandidates(ctx, q, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("inactive listing returned: %+v", got)
	}
}

func TestTursoLexicalMigrationBackfillsExistingListings(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "legacy.db")
	db, err := sql.Open("libsql", "file:"+path)
	if err != nil {
		t.Fatal(err)
	}
	if err := execStatements(ctx, db, schemaSQL); err != nil {
		t.Fatal(err)
	}
	old := lexicalListing("legacy", pk, "legacy", "Bronze kettle", "For tea", "", listing.StatusActive, 10)
	_, err = db.ExecContext(ctx, `INSERT INTO listings
(coord, event_id, kind, pubkey, d_tag, status, title, body, text_hash, created_at, updated_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, old.Coord, old.EventID, old.Kind, old.PubKey, old.DTag,
		old.Status, old.Title, old.Body, old.TextHash, old.CreatedAt, old.CreatedAt)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	st, err := OpenTurso(ctx, path, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	got, err := st.(*sqlStore).lexicalCandidates(ctx, Query{Search: "bronze"}, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].EventID != "legacy" {
		t.Fatalf("legacy row not indexed: %+v", got)
	}
}

func TestTursoConcurrentReadersAndWriter(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	st, err := OpenTurso(ctx, filepath.Join(t.TempDir(), "concurrent.db"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	s := st.(*sqlStore)
	if s.db.Stats().MaxOpenConnections != tursoMaxOpenConns {
		t.Fatalf("pool max = %d", s.db.Stats().MaxOpenConnections)
	}
	connections := make([]*sql.Conn, 0, tursoMaxOpenConns)
	for i := 0; i < tursoMaxOpenConns; i++ {
		conn, err := s.db.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		connections = append(connections, conn)
		var journal string
		var busy, foreign int
		if err := conn.QueryRowContext(ctx, `PRAGMA journal_mode`).Scan(&journal); err != nil {
			t.Fatal(err)
		}
		if err := conn.QueryRowContext(ctx, `PRAGMA busy_timeout`).Scan(&busy); err != nil {
			t.Fatal(err)
		}
		if err := conn.QueryRowContext(ctx, `PRAGMA foreign_keys`).Scan(&foreign); err != nil {
			t.Fatal(err)
		}
		if journal != "wal" || busy != 5000 || foreign != 1 {
			t.Fatalf("connection %d pragmas: journal=%q busy=%d foreign=%d", i, journal, busy, foreign)
		}
	}
	for _, conn := range connections {
		if err := conn.Close(); err != nil {
			t.Fatal(err)
		}
	}
	current := lexicalListing("version-0", pk, "current", "Copper kettle", "Copper kitchen item", "", listing.StatusActive, 1)
	if err := st.Upsert(ctx, current); err != nil {
		t.Fatal(err)
	}
	var workers sync.WaitGroup
	errors := make(chan error, tursoMaxOpenConns+1)
	for i := 0; i < tursoMaxOpenConns; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for j := 0; j < 20; j++ {
				found, err := s.lexicalCandidates(ctx, Query{Search: "copper"}, 5)
				if err != nil {
					errors <- err
					return
				}
				if len(found) == 0 {
					errors <- fmt.Errorf("active copper listing disappeared")
					return
				}
			}
		}()
	}
	workers.Add(1)
	go func() {
		defer workers.Done()
		for i := 1; i <= 20; i++ {
			replacement := lexicalListing(fmt.Sprintf("version-%d", i), pk, "current", "Copper kettle", "Copper kitchen item", "", listing.StatusActive, int64(i+1))
			if err := st.Upsert(ctx, replacement); err != nil {
				errors <- err
				return
			}
		}
	}()
	workers.Wait()
	close(errors)
	for err := range errors {
		t.Error(err)
	}
	latest, ok, err := st.Get(ctx, current.Coord)
	if err != nil || !ok || latest.EventID != "version-20" {
		t.Fatalf("final current listing: %+v, ok=%t, err=%v", latest, ok, err)
	}
}
