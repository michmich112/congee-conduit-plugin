package index

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/michmich112/conduit-plugin/listing"
	"github.com/michmich112/conduit-plugin/nip85"
)

func TestNIP85ReplacementAndProviderIsolation(t *testing.T) {
	s := relevanceStore(t, false)
	ctx := context.Background()
	p1, p2, target := strings.Repeat("a", 64), strings.Repeat("b", 64), strings.Repeat("c", 64)
	put := func(provider, id string, at int64, rank int) {
		t.Helper()
		if err := s.UpsertUserRank(ctx, nip85.UserRank{Provider: provider, Target: target, EventID: id, CreatedAt: at, Rank: rank}); err != nil {
			t.Fatal(err)
		}
	}
	put(p1, strings.Repeat("1", 64), 100, 80)
	put(p1, strings.Repeat("0", 64), 99, 5)
	put(p1, strings.Repeat("2", 64), 100, 90)
	put(p1, strings.Repeat("1", 64), 100, 10)
	put(p2, strings.Repeat("3", 64), 101, 20)
	for _, tc := range []struct {
		provider string
		want     int
	}{{p1, 90}, {p2, 20}} {
		scores, err := s.userRankScores(ctx, tc.provider, []string{target}, 0)
		if err != nil || scores[target] != tc.want {
			t.Fatalf("provider %s: %v %v", tc.provider, scores, err)
		}
	}
	scores, err := s.userRankScores(ctx, p1, []string{target}, 101)
	if err != nil || len(scores) != 0 {
		t.Fatalf("stale score should be neutral: %v %v", scores, err)
	}
}

func TestNIP85BoundedSearchRerank(t *testing.T) {
	s := relevanceStore(t, true)
	ctx := context.Background()
	now := time.Now().Unix()
	provider, other := strings.Repeat("a", 64), strings.Repeat("b", 64)
	strong, close, weak, missing := strings.Repeat("c", 64), strings.Repeat("d", 64), strings.Repeat("e", 64), strings.Repeat("f", 64)
	addSearchRow(t, s, "strong", strong, "brass lamp", now, []float32{1, 0})
	addSearchRow(t, s, "close", close, "brass lamp", now, []float32{0.99, 0.14})
	addSearchRow(t, s, "weak", weak, "brass lamp", now, []float32{0.8, 0.6})
	addSearchRow(t, s, "missing", missing, "brass lamp", now, []float32{0.999, 0.045})
	q := Query{Search: "lamp", Kinds: []int{listing.KindProduct}, Limit: 4, VectorEnabled: true, NIP85Provider: provider, NIP85MaxAgeDays: 14}
	search := func() []string {
		t.Helper()
		ids, err := s.Search(ctx, q)
		if err != nil {
			t.Fatal(err)
		}
		return ids
	}
	if ids := search(); ids[0] != "strong" || !slices.Contains(ids, "missing") {
		t.Fatalf("unasserted merchants must be searchable: %v", ids)
	}
	for _, rank := range []nip85.UserRank{
		{Provider: provider, Target: close, EventID: strings.Repeat("1", 64), CreatedAt: now, Rank: 100},
		{Provider: provider, Target: weak, EventID: strings.Repeat("2", 64), CreatedAt: now, Rank: 100},
		{Provider: other, Target: strong, EventID: strings.Repeat("3", 64), CreatedAt: now, Rank: 100},
	} {
		if err := s.UpsertUserRank(ctx, rank); err != nil {
			t.Fatal(err)
		}
	}
	if ids := search(); ids[0] != "close" || slices.Index(ids, "weak") < slices.Index(ids, "strong") {
		t.Fatalf("rank should break a close match, not overwhelm relevance: %v", ids)
	}
	if err := s.UpsertUserRank(ctx, nip85.UserRank{Provider: provider, Target: strong, EventID: strings.Repeat("4", 64), CreatedAt: now, Rank: 0}); err != nil {
		t.Fatal(err)
	}
	if ids := search(); slices.Index(ids, "strong") < slices.Index(ids, "missing") {
		t.Fatalf("low asserted rank should fall below a comparable unasserted seller: %v", ids)
	}
	q.NIP85Provider = other
	if ids := search(); ids[0] != "strong" {
		t.Fatalf("provider selection ignored: %v", ids)
	}
	q.NIP85Provider = provider
	q.NIP85MaxAgeDays = 1
	if _, err := s.db.ExecContext(ctx, `UPDATE nip85_user_ranks SET created_at = ? WHERE provider = ?`, now-2*24*60*60, provider); err != nil {
		t.Fatal(err)
	}
	if ids := search(); ids[0] != "strong" {
		t.Fatalf("stale assertion should be neutral: %v", ids)
	}
}
