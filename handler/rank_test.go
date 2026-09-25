package handler

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/michmich112/conduit-plugin/embed"
	"github.com/michmich112/conduit-plugin/index"
	"github.com/michmich112/conduit-plugin/nip85"
	sdk "github.com/michmich112/congee/sdk/plugin"
)

type rankHost struct {
	events  []sdk.Event
	filter  sdk.Filter
	listing *sdk.Event
}

func (h *rankHost) QueryEvents(_ context.Context, filters []sdk.Filter) ([]sdk.Event, error) {
	h.filter = filters[0]
	if h.listing != nil && !containsKind(filters[0].Kinds, nip85.KindUserAssertion) {
		return []sdk.Event{*h.listing}, nil
	}
	return h.events, nil
}
func (*rankHost) GetEventsByIDs(context.Context, []string) ([]sdk.Event, error) { return nil, nil }
func (*rankHost) Log(context.Context, string, string, map[string]string) error  { return nil }

func TestNIP85StoredEventAndMerchantBackfill(t *testing.T) {
	ctx := context.Background()
	store, err := index.OpenTurso(ctx, filepath.Join(t.TempDir(), "rank.db"), embed.Fake{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	provider := defaultNIP85Provider
	target := strings.Repeat("b", 64)
	assertion := sdk.Event{
		ID: strings.Repeat("1", 64), PubKey: provider, Kind: nip85.KindUserAssertion,
		CreatedAt: time.Now().Unix(), Tags: [][]string{{"d", target}, {"rank", "85"}},
	}
	host := &rankHost{events: []sdk.Event{assertion}}
	h := New(t.TempDir(), embed.Selection{})
	h.SetHost(host)
	h.store = store
	h.settings = defaultSettings()
	wrong := assertion
	wrong.PubKey = strings.Repeat("a", 64)
	if err := h.OnStoredEvent(ctx, wrong, true); err != nil {
		t.Fatal(err)
	}
	if err := h.OnStoredEvent(ctx, assertion, false); err != nil {
		t.Fatal(err)
	}
	if err := h.OnStoredEvent(ctx, assertion, true); err != nil {
		t.Fatal(err)
	}
	stats, err := store.Stats(ctx)
	if err != nil || stats.NIP85Assertions != 0 {
		t.Fatalf("wrong, unstored, or nonmerchant assertion accepted: %+v %v", stats, err)
	}
	listing := sdk.Event{
		ID: strings.Repeat("2", 64), PubKey: target, Kind: 30402, CreatedAt: time.Now().Unix(),
		Tags: [][]string{{"d", "sku"}, {"title", "Brass lamp"}}, Content: "Brass lamp",
	}
	if err := h.OnStoredEvent(ctx, listing, true); err != nil {
		t.Fatal(err)
	}
	stats, err = store.Stats(ctx)
	if err != nil || stats.NIP85Assertions != 1 {
		t.Fatalf("merchant backfill missing: %+v %v", stats, err)
	}
	newer := assertion
	newer.ID = strings.Repeat("3", 64)
	newer.CreatedAt++
	newer.Tags = [][]string{{"d", target}, {"rank", "90"}}
	if err := h.OnStoredEvent(ctx, newer, true); err != nil {
		t.Fatal(err)
	}
	if len(host.filter.Authors) != 1 || host.filter.Authors[0] != provider ||
		len(host.filter.Kinds) != 1 || host.filter.Kinds[0] != nip85.KindUserAssertion ||
		len(host.filter.Tags["d"]) != 1 || host.filter.Tags["d"][0] != target {
		t.Fatalf("backfill not scoped to provider and merchant: %+v", host.filter)
	}
}

func TestNIP85ProviderSubscriptionAndSettings(t *testing.T) {
	st := defaultSettings()
	if !containsKind(subscriptionsFor(st)[0].Kinds, nip85.KindUserAssertion) {
		t.Fatal("selected provider must subscribe to assertions")
	}
	st, err := parseSettings([]byte(`{"nip85_provider_pubkey":"","nip85_max_age_days":7}`))
	if err != nil || st.NIP85ProviderPubkey != "" || st.NIP85MaxAgeDays != 7 {
		t.Fatalf("provider disable or age setting: %+v %v", st, err)
	}
	if containsKind(subscriptionsFor(st)[0].Kinds, nip85.KindUserAssertion) {
		t.Fatal("disabled provider still subscribed")
	}
	if _, err := parseSettings([]byte(`{"nip85_provider_pubkey":"invalid"}`)); err == nil {
		t.Fatal("invalid provider accepted")
	}
}

func TestNIP85BackfillAfterInitialListings(t *testing.T) {
	ctx := context.Background()
	store, err := index.OpenTurso(ctx, filepath.Join(t.TempDir(), "initial.db"), embed.Fake{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	target := strings.Repeat("b", 64)
	listing := sdk.Event{
		ID: strings.Repeat("2", 64), PubKey: target, Kind: 30402, CreatedAt: time.Now().Unix(),
		Tags: [][]string{{"d", "sku"}, {"title", "Brass lamp"}}, Content: "Brass lamp",
	}
	h := New(t.TempDir(), embed.Selection{})
	h.SetHost(&rankHost{listing: &listing, events: []sdk.Event{{
		ID: strings.Repeat("1", 64), PubKey: defaultNIP85Provider, Kind: nip85.KindUserAssertion,
		CreatedAt: time.Now().Unix(), Tags: [][]string{{"d", target}, {"rank", "85"}},
	}}})
	h.store = store
	h.settings = defaultSettings()
	h.runBackfill(ctx)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		stats, err := store.Stats(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if stats.Active == 1 && stats.NIP85Assertions == 1 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("initial listing backfill did not recover its existing assertion")
}

func containsKind(kinds []int, want int) bool {
	for _, kind := range kinds {
		if kind == want {
			return true
		}
	}
	return false
}
