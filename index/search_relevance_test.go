package index

import (
	"context"
	"fmt"
	"path/filepath"
	"slices"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"github.com/michmich112/conduit-plugin/embed"
	"github.com/michmich112/conduit-plugin/listing"
)

type relevanceEmbedder struct{}

func (relevanceEmbedder) ModelID() string { return "relevance-test" }
func (relevanceEmbedder) Dim() int        { return 2 }
func (relevanceEmbedder) Embed(context.Context, string) ([]float32, error) {
	return []float32{1, 0}, nil
}

func relevanceStore(t testing.TB, vector bool) *sqlStore {
	t.Helper()
	var e embed.Embedder
	if vector {
		e = relevanceEmbedder{}
	}
	store, err := OpenTurso(context.Background(), filepath.Join(t.TempDir(), "search.db"), e)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store.(*sqlStore)
}

func addSearchRow(t testing.TB, s *sqlStore, id, author, title string, created int64, vec []float32) {
	t.Helper()
	coord := listing.CoordOf(listing.KindProduct, author, id)
	_, err := s.db.ExecContext(context.Background(), `INSERT INTO listings
(coord,event_id,kind,pubkey,d_tag,status,title,body,text_hash,created_at,updated_at)
VALUES (?,?,?,?,?,?,?,?,?,?,?)`, coord, id, listing.KindProduct, author, id,
		listing.StatusActive, title, title, id, created, created)
	if err != nil {
		t.Fatal(err)
	}
	if vec != nil {
		s.ann.Upsert(coord, id, created, vec)
	}
}

func TestSearchFullIndexFindsOldRelevantItem(t *testing.T) {
	s := relevanceStore(t, true)
	now := time.Now().Unix()
	old := now - 8*30*24*60*60
	addSearchRow(t, s, "old-lamp", "old-seller", "artisan brass floor lamp", old, []float32{1, 0})
	for i := range 2100 {
		addSearchRow(t, s, fmt.Sprintf("new-%04d", i), "flood", fmt.Sprintf("new unrelated item %d", i), now-int64(i), []float32{0.1, 0.995})
	}
	ids, err := s.Search(context.Background(), Query{
		Search: "lamp", Kinds: []int{listing.KindProduct}, Limit: 20,
		VectorEnabled: true, SearchCandidateCap: 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) == 0 || ids[0] != "old-lamp" {
		t.Fatalf("old relevant product must survive the newest 2,000 and win: %v", ids)
	}
	if len(ids) != 1 {
		t.Fatalf("weakly similar unrelated products should not be returned: %v", ids)
	}
}

func TestSearchFreshnessTieAndActiveEligibility(t *testing.T) {
	s := relevanceStore(t, true)
	now := time.Now().Unix()
	old := now - 8*30*24*60*60
	addSearchRow(t, s, "old", "seller-old", "brass lamp", old, []float32{1, 0})
	addSearchRow(t, s, "new", "seller-new", "brass lamp", now, []float32{1, 0})
	addSearchRow(t, s, "tie-a", "seller-a", "brass lamp", now, []float32{1, 0})
	addSearchRow(t, s, "tie-b", "seller-b", "brass lamp", now, []float32{1, 0})
	addSearchRow(t, s, "inactive", "seller-inactive", "brass lamp", now+1, []float32{1, 0})
	if _, err := s.db.Exec(`UPDATE listings SET status = ? WHERE event_id = ?`, listing.StatusInactive, "inactive"); err != nil {
		t.Fatal(err)
	}
	ids, err := s.Search(context.Background(), Query{Search: "lamp", Kinds: []int{listing.KindProduct}, Limit: 10, VectorEnabled: true})
	if err != nil {
		t.Fatal(err)
	}
	if slices.Contains(ids, "inactive") || slices.Index(ids, "old") < slices.Index(ids, "new") {
		t.Fatalf("active eligibility or freshness failed: %v", ids)
	}
	if slices.Index(ids, "tie-b") > slices.Index(ids, "tie-a") {
		t.Fatalf("same-time event ID tie should be deterministic: %v", ids)
	}
}

func TestSearchCurrentLexicalRevisionSurvivesStaleVector(t *testing.T) {
	s := relevanceStore(t, true)
	now := time.Now().Unix()
	addSearchRow(t, s, "old", "seller", "coffee mug", now-1, []float32{1, 0})
	coord := listing.CoordOf(listing.KindProduct, "seller", "old")
	if _, err := s.db.Exec(`UPDATE listings SET event_id = ?, title = ?, body = ?, created_at = ? WHERE coord = ?`,
		"current", "bicycle helmet", "bicycle helmet", now, coord); err != nil {
		t.Fatal(err)
	}
	ids, err := s.Search(context.Background(), Query{Search: "bicycle", Kinds: []int{listing.KindProduct}, Limit: 10, VectorEnabled: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 1 || ids[0] != "current" {
		t.Fatalf("current lexical revision should survive stale vector cache: %v", ids)
	}
}

func TestSearchGeoAndAuthorEligibility(t *testing.T) {
	s := relevanceStore(t, true)
	now := time.Now().Unix()
	addSearchRow(t, s, "inside", "seller-inside", "mountain bicycle", now, []float32{1, 0})
	addSearchRow(t, s, "outside", "seller-outside", "mountain bicycle", now+1, []float32{1, 0})
	inside := listing.CoordOf(listing.KindProduct, "seller-inside", "inside")
	outside := listing.CoordOf(listing.KindProduct, "seller-outside", "outside")
	for _, row := range []struct{ coord, geo string }{{inside, "9q8yy"}, {outside, "dr5ru"}} {
		if _, err := s.db.Exec(`INSERT INTO listing_geo (coord, geohash) VALUES (?, ?)`, row.coord, row.geo); err != nil {
			t.Fatal(err)
		}
	}
	ids, err := s.Search(context.Background(), Query{
		Search: "bicycle", Kinds: []int{listing.KindProduct}, GeoEnabled: true,
		GeoPrefixes: []string{"9q8"}, GeoMinPrefixLen: 2, Limit: 10, VectorEnabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 1 || ids[0] != "inside" {
		t.Fatalf("geo eligibility failed: %v", ids)
	}
	ids, err = s.Search(context.Background(), Query{
		Search: "bicycle", Kinds: []int{listing.KindProduct}, Authors: []string{"seller-outside"},
		Limit: 10, VectorEnabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 1 || ids[0] != "outside" {
		t.Fatalf("author eligibility failed: %v", ids)
	}
}

func TestSameAuthorNearDuplicatesKeepMaterialVariants(t *testing.T) {
	common := "artisan brass adjustable reading floor lamp handmade durable warm glow elegant shade wooden base living room bedroom studio office workshop gift vintage classic restored"
	items := make([]searchCandidate, 0, 201)
	for i := range 200 {
		items = append(items, searchCandidate{
			EventID: fmt.Sprintf("copy-%03d", i), PubKey: "publisher", Title: "Brass reading lamp",
			Body: common + " serial" + fmt.Sprint(i), Score: 1 - float64(i)/10000,
		})
	}
	items = append(items, searchCandidate{
		EventID: "material-variant", PubKey: "publisher", Title: "Brass reading lamp",
		Body: "solar powered portable folding rechargeable bicycle light with a different mount battery and weather sealed case", Score: 0.90,
	})
	ids := diversifiedIDs(items, 20)
	if len(ids) != 2 || ids[0] != "copy-000" || ids[1] != "material-variant" {
		t.Fatalf("suppress near duplicates, retain distinct product variant: %v", ids)
	}
}

func BenchmarkSearchTenThousandFlood(b *testing.B) {
	s := relevanceStore(b, true)
	now := time.Now().Unix()
	for i := range 10000 {
		addSearchRow(b, s, fmt.Sprintf("flood-%05d", i), "flood", fmt.Sprintf("lamp item %d", i), now-int64(i), []float32{1, 0})
	}
	addSearchRow(b, s, "other", "other", "brass reading lamp", now, []float32{0.98, 0.2})
	query := Query{Search: "lamp", Kinds: []int{listing.KindProduct}, Limit: 20, VectorEnabled: true}
	b.ResetTimer()
	for range b.N {
		if _, err := s.Search(context.Background(), query); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSearchTenThousandAuthors(b *testing.B) {
	s := relevanceStore(b, true)
	now := time.Now().Unix()
	for i := range 10000 {
		addSearchRow(b, s, fmt.Sprintf("item-%05d", i), fmt.Sprintf("author-%05d", i),
			fmt.Sprintf("lamp item %d", i), now-int64(i), []float32{1, 0})
	}
	query := Query{Search: "lamp", Kinds: []int{listing.KindProduct}, Limit: 20, VectorEnabled: true}
	b.ResetTimer()
	for range b.N {
		if _, err := s.Search(context.Background(), query); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSearchTenThousandFloodParallel(b *testing.B) {
	s := relevanceStore(b, true)
	now := time.Now().Unix()
	for i := range 10000 {
		addSearchRow(b, s, fmt.Sprintf("flood-%05d", i), "flood", fmt.Sprintf("lamp item %d", i), now-int64(i), []float32{1, 0})
	}
	addSearchRow(b, s, "other", "other", "brass reading lamp", now, []float32{0.98, 0.2})
	for _, scenario := range []struct {
		name     string
		cap      int
		distinct bool
	}{
		{"cap100-repeated", 100, false},
		{"cap1000-repeated", 1000, false},
		{"cap2000-repeated", 2000, false},
		{"cap100-distinct", 100, true},
		{"cap1000-distinct", 1000, true},
		{"cap2000-distinct", 2000, true},
	} {
		b.Run(scenario.name, func(b *testing.B) {
			query := Query{Search: "lamp", Kinds: []int{listing.KindProduct}, Limit: 20,
				VectorEnabled: true, SearchCandidateCap: scenario.cap}
			var next atomic.Int64
			durations := make([]int64, b.N)
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					idx := int(next.Add(1)) - 1
					q := query
					if scenario.distinct {
						q.Search = fmt.Sprintf("lamp variantquery%d", idx)
					}
					started := time.Now()
					_, err := s.Search(context.Background(), q)
					if err != nil {
						b.Error(err)
					}
					durations[idx] = time.Since(started).Nanoseconds()
				}
			})
			sort.Slice(durations, func(i, j int) bool { return durations[i] < durations[j] })
			b.ReportMetric(float64(durations[(len(durations)-1)/2])/1e6, "p50_ms")
			b.ReportMetric(float64(durations[(95*len(durations)+99)/100-1])/1e6, "p95_ms")
			b.ReportMetric(float64(durations[len(durations)-1])/1e6, "max_ms")
		})
	}
}

func TestSearchAuthorFloodAndDuplicateTitles(t *testing.T) {
	s := relevanceStore(t, true)
	now := time.Now().Unix()
	for i := range 10000 {
		addSearchRow(t, s, fmt.Sprintf("flood-%05d", i), "flood", fmt.Sprintf("lamp model %d", i), now-int64(i), []float32{1, 0})
	}
	addSearchRow(t, s, "another-seller", "other", "brass reading lamp", now-10, []float32{0.98, 0.2})
	addSearchRow(t, s, "dupe-a", "dupe", "Antique Brass Lamp", now+1, []float32{1, 0})
	addSearchRow(t, s, "dupe-b", "dupe", "antique brass lamp!", now+2, []float32{1, 0})
	ids, err := s.Search(context.Background(), Query{
		Search: "lamp", Kinds: []int{listing.KindProduct}, Limit: 20,
		VectorEnabled: true, SearchCandidateCap: 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	if p := slices.Index(ids, "another-seller"); p < 0 || p > 4 {
		t.Fatalf("another relevant seller lost under publisher flood: rank %d, %v", p, ids)
	}
	if slices.Contains(ids, "dupe-a") && slices.Contains(ids, "dupe-b") {
		t.Fatalf("same-author duplicate titles not suppressed: %v", ids)
	}
}

func TestSearchLexicalFallbackAndIndependentUnion(t *testing.T) {
	s := relevanceStore(t, true)
	now := time.Now().Unix()
	addSearchRow(t, s, "semantic", "one", "human powered transport", now, []float32{1, 0})
	addSearchRow(t, s, "lexical", "two", "bicycle road bike", now, []float32{0, 1})
	ids, err := s.Search(context.Background(), Query{
		Search: "bicycle", Kinds: []int{listing.KindProduct}, Limit: 10,
		VectorEnabled: true, SearchCandidateCap: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(ids, "semantic") || !slices.Contains(ids, "lexical") {
		t.Fatalf("lexical and semantic candidates should both survive union: %v", ids)
	}

	withoutVectors := relevanceStore(t, false)
	addSearchRow(t, withoutVectors, "match", "three", "bicycle helmet", now, nil)
	addSearchRow(t, withoutVectors, "unrelated", "four", "coffee mug", now+10, nil)
	addSearchRow(t, withoutVectors, "extension-word", "five", "spam stickers", now+11, nil)
	ids, err = withoutVectors.Search(context.Background(), Query{
		Search: "bicycle", Kinds: []int{listing.KindProduct}, Limit: 10,
		VectorEnabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 1 || ids[0] != "match" {
		t.Fatalf("lexical fallback should include only matches: %v", ids)
	}
	ids, err = withoutVectors.Search(context.Background(), Query{
		Search: "bicycle include:spam", Kinds: []int{listing.KindProduct}, Limit: 10,
		VectorEnabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 1 || ids[0] != "match" {
		t.Fatalf("unsupported NIP-50 extension became a search term: %v", ids)
	}
	ids, err = withoutVectors.Search(context.Background(), Query{
		Search: "snowboard", Kinds: []int{listing.KindProduct}, Limit: 10,
		VectorEnabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 0 {
		t.Fatalf("unmatched query should return no products: %v", ids)
	}
}
