package index

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/michmich112/conduit-plugin/embed"
	"github.com/michmich112/conduit-plugin/listing"
)

type blockedEmbed struct {
	embed.Fake
	entered, release chan struct{}
}

func (e blockedEmbed) Embed(ctx context.Context, text string) ([]float32, error) {
	if strings.Contains(text, "blocked") {
		e.entered <- struct{}{}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-e.release:
		}
	}
	return e.Fake.Embed(ctx, text)
}
func concurrencyListing(d, body string) listing.Listing {
	l, _ := listing.FromEvent(listing.Event{ID: d, PubKey: pk, Kind: listing.KindClassified, CreatedAt: 10, Content: body, Tags: [][]string{{"d", d}, {"g", "9q8yy"}}}, false)
	return l
}
func TestEmbeddingDoesNotBlockWritesOrResurrectDeletion(t *testing.T) {
	for _, backend := range []string{"turso", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx := context.Background()
			e := blockedEmbed{Fake: embed.Fake{}, entered: make(chan struct{}, 1), release: make(chan struct{})}
			var st Store
			var err error
			if backend == "postgres" {
				dsn := os.Getenv("TEST_POSTGRES_DSN")
				if dsn == "" {
					t.Skip("set TEST_POSTGRES_DSN")
				}
				st, err = OpenPostgres(ctx, dsn, e)
			} else {
				st, err = OpenTurso(ctx, filepath.Join(t.TempDir(), "index.db"), e)
			}
			if err != nil {
				t.Fatal(err)
			}
			defer st.Close()
			target := concurrencyListing("blocked-"+backend, "blocked bicycle")
			other := concurrencyListing("other-"+backend, "pizza")
			defer st.DeleteCoord(ctx, target.Coord)
			defer st.DeleteCoord(ctx, other.Coord)
			done := make(chan error, 1)
			go func() { done <- st.Upsert(ctx, target) }()
			select {
			case <-e.entered:
			case <-time.After(3 * time.Second):
				t.Fatal("embedding did not start")
			}
			short, cancel := context.WithTimeout(ctx, 2*time.Second)
			defer cancel()
			if err := st.Upsert(short, other); err != nil {
				t.Fatal("unrelated write blocked:", err)
			}
			if err := st.DeleteCoord(short, target.Coord); err != nil {
				t.Fatal("deletion blocked:", err)
			}
			close(e.release)
			if err := <-done; !errors.Is(err, ErrSuperseded) {
				t.Fatalf("obsolete embedding accepted: %v", err)
			}
			if _, ok, err := st.Get(ctx, target.Coord); err != nil || ok {
				t.Fatalf("deleted row resurrected: %v %v", ok, err)
			}
			for _, x := range st.(*sqlStore).ann.Snapshot(nil) {
				if x.Coord == target.Coord {
					t.Fatal("ANN resurrected deletion")
				}
			}
		})
	}
}
