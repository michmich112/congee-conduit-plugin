package index

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/michmich112/conduit-plugin/embed"
	"github.com/michmich112/conduit-plugin/listing"
)

// BenchmarkSearchWarm measures the complete plugin index query, including SQL
// filtering and vector ranking. Set CONDUIT_BENCH_DOCS to 100000 or 1000000
// for larger corpora and use go test -bench ... -cpu 1,8,32 for contention.
func BenchmarkSearchWarm(b *testing.B) {
	count := 10000
	if raw := os.Getenv("CONDUIT_BENCH_DOCS"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed <= 0 {
			b.Fatalf("invalid CONDUIT_BENCH_DOCS %q", raw)
		}
		count = parsed
	}
	ctx := context.Background()
	store, err := OpenTurso(ctx, b.TempDir()+"/search-bench.db", embed.Fake{})
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()
	base := time.Now().Unix()
	for i := 0; i < count; i++ {
		pubkey := fmt.Sprintf("%064x", i%100)
		product := "bicycle parts"
		if i%3 == 0 {
			product = "camping supplies"
		}
		l := listing.Listing{
			Coord: fmt.Sprintf("30402:%s:%d", pubkey, i), EventID: fmt.Sprintf("%064x", i+1),
			Kind: listing.KindClassified, PubKey: pubkey, DTag: strconv.Itoa(i),
			Status: listing.StatusActive, Title: product, Body: product,
			TextHash: fmt.Sprintf("%x", i), CreatedAt: base - int64(i),
		}
		if err := store.Upsert(ctx, l); err != nil {
			b.Fatalf("seed %d: %v", i, err)
		}
	}
	q := Query{Search: "bicycle", Kinds: []int{listing.KindClassified}, Limit: 20,
		ActiveOnly: true, VectorEnabled: true, SearchCandidateCap: 1000}
	if _, err := store.Search(ctx, q); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := store.Search(ctx, q); err != nil {
				b.Error(err)
				return
			}
		}
	})
}
