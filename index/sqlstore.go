package index

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/michmich112/conduit-plugin/embed"
	"github.com/michmich112/conduit-plugin/listing"
	_ "github.com/tursodatabase/go-libsql"
)

type sqlStore struct {
	db       *sql.DB
	pool     *pgxpool.Pool
	backend  string
	ph       func(n int) string
	embedder embed.Embedder
	ann      *ANN
	qcache   *queryCache
	writeMu  sync.Mutex
}

// OpenTurso opens a single-writer libSQL file at path (MaxOpenConns=1).
func OpenTurso(ctx context.Context, path string, e embed.Embedder) (Store, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil && !os.IsExist(err) {
		if filepath.Dir(path) != "." {
			return nil, err
		}
	}
	dsn := path
	if !strings.HasPrefix(path, "file:") {
		dsn = "file:" + path
	}
	db, err := sql.Open("libsql", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	for _, pragma := range []string{
		`PRAGMA busy_timeout = 5000`,
		`PRAGMA journal_mode = WAL`,
		`PRAGMA foreign_keys = ON`,
	} {
		rows, err := db.QueryContext(ctx, pragma)
		if err != nil {
			_ = db.Close()
			return nil, err
		}
		_ = rows.Close()
	}
	if err := execStatements(ctx, db, schemaSQL); err != nil {
		_ = db.Close()
		return nil, err
	}
	s := &sqlStore{
		db:       db,
		backend:  "turso",
		ph:       func(n int) string { return "?" },
		embedder: ensureEmbedder(e),
		ann:      NewANN(),
		qcache:   newQueryCache(256),
	}
	if err := s.rebuildANN(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

// OpenPostgres opens a pgx pool. vector column is BYTEA (pgvector optional later).
func OpenPostgres(ctx context.Context, url string, e embed.Embedder) (Store, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, err
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	db := stdlib.OpenDBFromPool(pool)
	if err := execStatements(ctx, db, pgSchemaSQL); err != nil {
		pool.Close()
		_ = db.Close()
		return nil, err
	}
	s := &sqlStore{
		db:       db,
		pool:     pool,
		backend:  "postgres",
		ph:       func(n int) string { return fmt.Sprintf("$%d", n) },
		embedder: ensureEmbedder(e),
		ann:      NewANN(),
		qcache:   newQueryCache(256),
	}
	if err := s.rebuildANN(ctx); err != nil {
		s.Close()
		return nil, err
	}
	return s, nil
}

func execStatements(ctx context.Context, db *sql.DB, sqlText string) error {
	for _, stmt := range strings.Split(sqlText, ";") {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}

func (s *sqlStore) runWrite(fn func() error) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return fn()
}

func (s *sqlStore) Close() error {
	if s.pool != nil {
		s.pool.Close()
	}
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

func (s *sqlStore) Ping(ctx context.Context) error {
	return s.db.PingContext(ctx)
}

func (s *sqlStore) Upsert(ctx context.Context, l listing.Listing) error {
	return s.upsert(ctx, l, true)
}

func (s *sqlStore) ReplaceCanonical(ctx context.Context, l listing.Listing) error {
	return s.upsert(ctx, l, false)
}

func (s *sqlStore) upsert(ctx context.Context, l listing.Listing, enforceOrder bool) error {
	if l.IsDeletion {
		return s.MarkInactive(ctx, l.PubKey, l.DeleteEventIDs, l.DeleteCoords)
	}
	if l.Coord == "" {
		return nil
	}
	return s.runWrite(func() error {
		var existingCreated int64
		var existingID, existingHash, existingModel string
		row := s.db.QueryRowContext(ctx, `SELECT created_at, event_id, text_hash FROM listings WHERE coord = `+s.ph(1), l.Coord)
		if err := row.Scan(&existingCreated, &existingID, &existingHash); err != nil && err != sql.ErrNoRows {
			return err
		}
		if enforceOrder && existingID != "" {
			// NIP-01 retains the lowest event ID when timestamps tie.
			if l.CreatedAt < existingCreated || (l.CreatedAt == existingCreated && l.EventID > existingID) {
				return nil
			}
		}
		var vec []float32
		if l.Status == listing.StatusActive && s.embedder != nil {
			var existingDim int
			if existingHash == l.TextHash {
				var blob []byte
				if err := s.db.QueryRowContext(ctx, `SELECT model, dim, vector FROM listing_embeddings WHERE coord = `+s.ph(1), l.Coord).Scan(&existingModel, &existingDim, &blob); err == nil && existingModel == s.embedder.ModelID() && existingDim == s.embedder.Dim() {
					vec = bytesToFloats(blob)
				}
			}
			if vec == nil {
				var err error
				vec, err = s.embedder.Embed(ctx, l.EmbedText())
				if err != nil {
					return err
				}
			}
		}
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		now := nowUnix()
		_, err = tx.ExecContext(ctx, `INSERT INTO listings (coord, event_id, kind, pubkey, d_tag, stall_id, status, inactive_reason, title, body, text_hash, created_at, updated_at)
VALUES (`+placeholders(s, 13)+`)
ON CONFLICT(coord) DO UPDATE SET
 event_id=excluded.event_id, kind=excluded.kind, pubkey=excluded.pubkey, d_tag=excluded.d_tag,
 stall_id=excluded.stall_id, status=excluded.status, inactive_reason=excluded.inactive_reason,
 title=excluded.title, body=excluded.body, text_hash=excluded.text_hash,
 created_at=excluded.created_at, updated_at=excluded.updated_at`,
			l.Coord, l.EventID, l.Kind, l.PubKey, l.DTag, nullStr(l.StallID), l.Status, nullStr(l.InactiveReason),
			nullStr(l.Title), nullStr(l.Body), l.TextHash, l.CreatedAt, now)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM listing_geo WHERE coord = `+s.ph(1), l.Coord); err != nil {
			return err
		}
		if l.HasGeo {
			_, err = tx.ExecContext(ctx, `INSERT INTO listing_geo (coord, geohash, lat, lon) VALUES (`+placeholders(s, 4)+`)`,
				l.Coord, l.Geohash, l.Lat, l.Lon)
			if err != nil {
				return err
			}
		}
		if l.Status == listing.StatusActive && vec != nil {
			_, err = tx.ExecContext(ctx, `INSERT INTO listing_embeddings (coord, model, dim, vector) VALUES (`+placeholders(s, 4)+`)
ON CONFLICT(coord) DO UPDATE SET model=excluded.model, dim=excluded.dim, vector=excluded.vector`,
				l.Coord, s.embedder.ModelID(), s.embedder.Dim(), floatsToBytes(vec))
			if err != nil {
				return err
			}
		} else if _, err := tx.ExecContext(ctx, `DELETE FROM listing_embeddings WHERE coord = `+s.ph(1), l.Coord); err != nil {
			return err
		}
		if err := tx.Commit(); err != nil {
			return err
		}
		if l.Status == listing.StatusActive && vec != nil {
			s.ann.Upsert(l.Coord, l.EventID, l.CreatedAt, vec)
		} else {
			s.ann.Delete(l.Coord)
		}
		return nil
	})
}

func (s *sqlStore) MarkInactive(ctx context.Context, pubkey string, eventIDs, coords []string) error {
	return s.runWrite(func() error {
		for _, id := range eventIDs {
			if id == "" {
				continue
			}
			q := `SELECT coord FROM listings WHERE event_id = ` + s.ph(1)
			args := []any{id}
			if pubkey != "" {
				q += ` AND pubkey = ` + s.ph(2)
				args = append(args, pubkey)
			}
			rows, err := s.db.QueryContext(ctx, q, args...)
			if err != nil {
				return err
			}
			var found []string
			for rows.Next() {
				var c string
				if err := rows.Scan(&c); err != nil {
					_ = rows.Close()
					return err
				}
				found = append(found, c)
			}
			_ = rows.Close()
			for _, c := range found {
				if err := s.markCoordInactive(ctx, c); err != nil {
					return err
				}
			}
		}
		for _, c := range coords {
			if err := s.markCoordInactive(ctx, c); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *sqlStore) DeleteCoord(ctx context.Context, coord string) error {
	return s.runWrite(func() error {
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		for _, table := range []string{"listing_embeddings", "listing_geo", "listings"} {
			if _, err := tx.ExecContext(ctx, `DELETE FROM `+table+` WHERE coord = `+s.ph(1), coord); err != nil {
				return err
			}
		}
		if err := tx.Commit(); err != nil {
			return err
		}
		s.ann.Delete(coord)
		return nil
	})
}

func (s *sqlStore) CoordsForEventIDs(ctx context.Context, pubkey string, ids []string) ([]string, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	phs := make([]string, len(ids))
	args := make([]any, 0, len(ids)+1)
	for i, id := range ids {
		phs[i] = s.ph(i + 1)
		args = append(args, id)
	}
	q := `SELECT coord FROM listings WHERE event_id IN (` + strings.Join(phs, ",") + `)`
	if pubkey != "" {
		q += ` AND pubkey = ` + s.ph(len(args)+1)
		args = append(args, pubkey)
	}
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var coords []string
	for rows.Next() {
		var coord string
		if err := rows.Scan(&coord); err != nil {
			return nil, err
		}
		coords = append(coords, coord)
	}
	return coords, rows.Err()
}

func (s *sqlStore) CoordsAfter(ctx context.Context, after string, limit int) ([]string, error) {
	if limit <= 0 {
		return nil, nil
	}
	rows, err := s.db.QueryContext(ctx, `SELECT coord FROM listings WHERE coord > `+s.ph(1)+` ORDER BY coord LIMIT `+s.ph(2), after, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var coords []string
	for rows.Next() {
		var coord string
		if err := rows.Scan(&coord); err != nil {
			return nil, err
		}
		coords = append(coords, coord)
	}
	return coords, rows.Err()
}

func (s *sqlStore) markCoordInactive(ctx context.Context, coord string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	_, err = tx.ExecContext(ctx, `UPDATE listings SET status = `+s.ph(1)+`, inactive_reason = `+s.ph(2)+`, updated_at = `+s.ph(3)+` WHERE coord = `+s.ph(4),
		listing.StatusInactive, listing.ReasonDeleted, nowUnix(), coord)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM listing_embeddings WHERE coord = `+s.ph(1), coord); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	s.ann.Delete(coord)
	return nil
}

func (s *sqlStore) Get(ctx context.Context, coord string) (listing.Listing, bool, error) {
	var l listing.Listing
	err := s.db.QueryRowContext(ctx, `SELECT coord, event_id, kind, pubkey, d_tag, COALESCE(stall_id,''), status, COALESCE(inactive_reason,''), COALESCE(title,''), COALESCE(body,''), text_hash, created_at
FROM listings WHERE coord = `+s.ph(1), coord).Scan(
		&l.Coord, &l.EventID, &l.Kind, &l.PubKey, &l.DTag, &l.StallID, &l.Status, &l.InactiveReason, &l.Title, &l.Body, &l.TextHash, &l.CreatedAt)
	if err == sql.ErrNoRows {
		return ListingZero(), false, nil
	}
	if err != nil {
		return ListingZero(), false, err
	}
	var gh sql.NullString
	var lat, lon sql.NullFloat64
	err = s.db.QueryRowContext(ctx, `SELECT geohash, lat, lon FROM listing_geo WHERE coord = `+s.ph(1), coord).Scan(&gh, &lat, &lon)
	if err == nil && gh.Valid {
		l.Geohash = gh.String
		l.Lat = lat.Float64
		l.Lon = lon.Float64
		l.HasGeo = true
	}
	return l, true, nil
}

func ListingZero() listing.Listing { return listing.Listing{} }

func (s *sqlStore) Meta(ctx context.Context, key string) (string, error) {
	var v string
	err := s.db.QueryRowContext(ctx, `SELECT value FROM index_meta WHERE key = `+s.ph(1), key).Scan(&v)
	if err == sql.ErrNoRows {
		return "", nil
	}
	return v, err
}

func (s *sqlStore) SetMeta(ctx context.Context, key, value string) error {
	return s.runWrite(func() error {
		_, err := s.db.ExecContext(ctx, `INSERT INTO index_meta (key, value) VALUES (`+s.ph(1)+`,`+s.ph(2)+`)
ON CONFLICT(key) DO UPDATE SET value=excluded.value`, key, value)
		return err
	})
}

func (s *sqlStore) Stats(ctx context.Context) (Stats, error) {
	st := Stats{Backend: s.backend}
	_ = s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM listings WHERE status = 'active'`).Scan(&st.Active)
	_ = s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM listings WHERE status != 'active'`).Scan(&st.Inactive)
	_ = s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM listing_embeddings`).Scan(&st.Embeddings)
	if s.embedder != nil {
		_ = s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM listings l LEFT JOIN listing_embeddings e ON l.coord = e.coord
WHERE l.status = 'active' AND (e.coord IS NULL OR e.model != `+s.ph(1)+` OR e.dim != `+s.ph(2)+`)`, s.embedder.ModelID(), s.embedder.Dim()).Scan(&st.EmbeddingMismatch)
	}
	return st, nil
}

func (s *sqlStore) rebuildANN(ctx context.Context) error {
	rows, err := s.db.QueryContext(ctx, `SELECT l.coord, l.event_id, l.created_at, e.vector
FROM listings l JOIN listing_embeddings e ON l.coord = e.coord
WHERE l.status = 'active'`)
	if err != nil {
		return err
	}
	defer rows.Close()
	s.ann.Replace(nil)
	items := map[string]annItem{}
	for rows.Next() {
		var coord, eventID string
		var created int64
		var blob []byte
		if err := rows.Scan(&coord, &eventID, &created, &blob); err != nil {
			return err
		}
		items[coord] = annItem{Coord: coord, EventID: eventID, CreatedAt: created, Vec: bytesToFloats(blob)}
	}
	s.ann.Replace(items)
	return rows.Err()
}

func (s *sqlStore) PurgeKindsNotIn(ctx context.Context, keep []int) error {
	if len(keep) == 0 {
		return nil
	}
	return s.runWrite(func() error {
		phs := make([]string, len(keep))
		args := make([]any, len(keep))
		for i, k := range keep {
			phs[i] = s.ph(i + 1)
			args[i] = k
		}
		q := `SELECT coord FROM listings WHERE kind NOT IN (` + strings.Join(phs, ",") + `)`
		rows, err := s.db.QueryContext(ctx, q, args...)
		if err != nil {
			return err
		}
		var coords []string
		for rows.Next() {
			var c string
			if err := rows.Scan(&c); err != nil {
				_ = rows.Close()
				return err
			}
			coords = append(coords, c)
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return err
		}
		_ = rows.Close()
		for _, coord := range coords {
			if _, err := s.db.ExecContext(ctx, `DELETE FROM listing_embeddings WHERE coord = `+s.ph(1), coord); err != nil {
				return err
			}
			if _, err := s.db.ExecContext(ctx, `DELETE FROM listing_geo WHERE coord = `+s.ph(1), coord); err != nil {
				return err
			}
			if _, err := s.db.ExecContext(ctx, `DELETE FROM listings WHERE coord = `+s.ph(1), coord); err != nil {
				return err
			}
			s.ann.Delete(coord)
		}
		return nil
	})
}

func placeholders(s *sqlStore, n int) string {
	parts := make([]string, n)
	for i := 0; i < n; i++ {
		parts[i] = s.ph(i + 1)
	}
	return strings.Join(parts, ",")
}

func nullStr(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func (s *sqlStore) Search(ctx context.Context, q Query) ([]string, error) {
	return searchSQL(ctx, s, q)
}

type queryCache struct {
	mu    sync.Mutex
	cap   int
	order []string
	m     map[string][]float32
}

func newQueryCache(n int) *queryCache {
	return &queryCache{cap: n, m: map[string][]float32{}}
}

func (c *queryCache) get(key string) ([]float32, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	v, ok := c.m[key]
	return v, ok
}

func (c *queryCache) put(key string, v []float32) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.m[key]; !ok && len(c.order) >= c.cap && c.cap > 0 {
		old := c.order[0]
		c.order = c.order[1:]
		delete(c.m, old)
	}
	c.m[key] = v
	c.order = append(c.order, key)
}
