package handler

import (
	"context"
	"errors"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/michmich112/conduit-plugin/embed"
	"github.com/michmich112/conduit-plugin/index"
	"github.com/michmich112/conduit-plugin/listing"
	sdk "github.com/michmich112/congee/sdk/plugin"
)

const testAuthor = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

type canonicalHost struct {
	mu      sync.Mutex
	events  map[string]sdk.Event
	readErr error
}

type unavailableEmbed struct{}

func (unavailableEmbed) ModelID() string { return "unavailable" }
func (unavailableEmbed) Dim() int        { return 8 }
func (unavailableEmbed) Embed(context.Context, string) ([]float32, error) {
	return nil, errors.New("embedding failed")
}

func newCanonicalHost() *canonicalHost { return &canonicalHost{events: map[string]sdk.Event{}} }

func (h *canonicalHost) put(ev sdk.Event) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.events[coordFromHint(ev, defaultSettings())] = ev
}

func (h *canonicalHost) remove(coord string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.events, coord)
}

func (h *canonicalHost) QueryEvents(_ context.Context, filters []sdk.Filter) ([]sdk.Event, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.readErr != nil {
		return nil, h.readErr
	}
	var out []sdk.Event
	for _, ev := range h.events {
		for _, f := range filters {
			if matchesCanonical(ev, f) {
				out = append(out, ev)
				break
			}
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].CreatedAt != out[j].CreatedAt {
			return out[i].CreatedAt > out[j].CreatedAt
		}
		return out[i].ID < out[j].ID
	})
	if len(filters) == 1 && filters[0].Limit != nil && *filters[0].Limit > 0 && len(out) > *filters[0].Limit {
		out = out[:*filters[0].Limit]
	}
	return out, nil
}

func matchesCanonical(ev sdk.Event, f sdk.Filter) bool {
	if len(f.Kinds) > 0 {
		found := false
		for _, k := range f.Kinds {
			found = found || k == ev.Kind
		}
		if !found {
			return false
		}
	}
	if len(f.Authors) > 0 {
		found := false
		for _, a := range f.Authors {
			found = found || a == ev.PubKey
		}
		if !found {
			return false
		}
	}
	if f.Until != nil && ev.CreatedAt > *f.Until {
		return false
	}
	for key, vals := range f.Tags {
		found := false
		for _, tag := range ev.Tags {
			for _, v := range vals {
				found = found || len(tag) >= 2 && tag[0] == key && tag[1] == v
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func (h *canonicalHost) GetEventsByIDs(ctx context.Context, ids []string) ([]sdk.Event, error) {
	evs, err := h.QueryEvents(ctx, []sdk.Filter{{}})
	if err != nil {
		return nil, err
	}
	var out []sdk.Event
	for _, id := range ids {
		for _, ev := range evs {
			if ev.ID == id {
				out = append(out, ev)
			}
		}
	}
	return out, nil
}

func (*canonicalHost) Log(context.Context, string, string, map[string]string) error { return nil }

func product(id, d string, at int64) sdk.Event {
	return sdk.Event{ID: strings.Repeat(id, 64), PubKey: testAuthor, Kind: listing.KindClassified,
		CreatedAt: at, Content: "bicycle " + id, Tags: [][]string{{"d", d}, {"title", "Bicycle " + id}}}
}

func deletion(id, d string, at int64, target sdk.Event) sdk.Event {
	return sdk.Event{ID: strings.Repeat(id, 64), PubKey: testAuthor, Kind: listing.KindDeletion,
		CreatedAt: at, Tags: [][]string{{"a", listing.CoordOf(target.Kind, target.PubKey, d)}, {"e", target.ID}}}
}

func testHandler(t *testing.T, path string, host *canonicalHost) (*Handler, index.Store) {
	t.Helper()
	store, err := index.OpenTurso(context.Background(), path, embed.Fake{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	h := New(t.TempDir(), embed.Selection{Embedder: embed.Fake{}})
	h.store, h.host, h.ready = store, host, true
	return h, store
}

func assertCanonical(t *testing.T, h *Handler, store index.Store, coord, wantID string) {
	t.Helper()
	ctx := context.Background()
	l, ok, err := store.Get(ctx, coord)
	if err != nil {
		t.Fatal(err)
	}
	if wantID == "" {
		if ok {
			t.Fatalf("stale row remains: %+v", l)
		}
	} else if !ok || l.EventID != wantID || l.Status != listing.StatusActive {
		t.Fatalf("listing got %+v, exists %v; want %s", l, ok, wantID)
	}
	emb, err := store.ListEmbeddings(ctx, index.ListQuery{Limit: 20})
	if err != nil {
		t.Fatal(err)
	}
	if wantID == "" {
		if emb.Total != 0 {
			t.Fatalf("stale embedding remains: %+v", emb)
		}
	} else if emb.Total != 1 || len(emb.Items) != 1 || emb.Items[0].EventID != wantID {
		t.Fatalf("embedding identity %+v; want %s", emb, wantID)
	}
	res, err := h.InterceptREQ(ctx, sdk.Req{Filters: []sdk.Filter{{Kinds: []int{listing.KindClassified}, Search: "bicycle"}}})
	if err != nil {
		t.Fatal(err)
	}
	if res.Action != sdk.InterceptRespond {
		t.Fatalf("NIP-50 action %v", res.Action)
	}
	if wantID == "" && len(res.EventIDs) != 0 || wantID != "" && (len(res.EventIDs) != 1 || res.EventIDs[0] != wantID) {
		t.Fatalf("NIP-50 IDs %v; want %s", res.EventIDs, wantID)
	}
}

func TestCanonicalCallbacksAndRevisionTie(t *testing.T) {
	ctx := context.Background()
	host := newCanonicalHost()
	h, store := testHandler(t, filepath.Join(t.TempDir(), "index.db"), host)
	a := product("b", "bike", 10)
	b := product("a", "bike", 10) // NIP-01: lower ID wins at equal timestamp.
	coord := listing.CoordOf(a.Kind, a.PubKey, "bike")
	host.put(a)
	if err := h.OnStoredEvent(ctx, a, true); err != nil {
		t.Fatal(err)
	}
	assertCanonical(t, h, store, coord, a.ID)
	host.put(b)
	if err := h.OnStoredEvent(ctx, b, true); err != nil {
		t.Fatal(err)
	}
	if err := h.OnStoredEvent(ctx, a, true); err != nil { // delayed A callback
		t.Fatal(err)
	}
	assertCanonical(t, h, store, coord, b.ID)
}

func TestRejectedStaleImportWithoutCallback(t *testing.T) {
	ctx := context.Background()
	host := newCanonicalHost()
	h, store := testHandler(t, filepath.Join(t.TempDir(), "index.db"), host)
	a := product("a", "bike", 10)
	b := product("b", "bike", 20)
	coord := listing.CoordOf(a.Kind, a.PubKey, "bike")
	host.put(b)
	if err := h.OnStoredEvent(ctx, b, true); err != nil {
		t.Fatal(err)
	}
	// Congee rejects the late A import: host state stays B and sends no callback.
	if err := h.reconcileLiveBatch(ctx); err != nil {
		t.Fatal(err)
	}
	assertCanonical(t, h, store, coord, b.ID)
}

func TestCanonicalDeletionBeforeAfterAndNewerRevision(t *testing.T) {
	ctx := context.Background()
	host := newCanonicalHost()
	h, store := testHandler(t, filepath.Join(t.TempDir(), "index.db"), host)
	a := product("a", "bike", 10)
	b := product("b", "bike", 20)
	coord := listing.CoordOf(a.Kind, a.PubKey, "bike")
	del := deletion("d", "bike", 11, a)
	// Deletion lands before the delayed A notification: A must never be indexed.
	host.remove(coord)
	if err := h.OnStoredEvent(ctx, del, true); err != nil {
		t.Fatal(err)
	}
	if err := h.OnStoredEvent(ctx, a, true); err != nil {
		t.Fatal(err)
	}
	assertCanonical(t, h, store, coord, "")
	// A is indexed, then deleted through an e/a request.
	host.put(a)
	if err := h.OnStoredEvent(ctx, a, true); err != nil {
		t.Fatal(err)
	}
	host.remove(coord)
	eOnly := sdk.Event{ID: strings.Repeat("e", 64), PubKey: testAuthor, Kind: listing.KindDeletion,
		CreatedAt: 11, Tags: [][]string{{"e", a.ID}}}
	if err := h.OnStoredEvent(ctx, eOnly, true); err != nil {
		t.Fatal(err)
	}
	assertCanonical(t, h, store, coord, "")
	// A valid newer B survives even a late deletion callback for A.
	host.put(b)
	if err := h.OnStoredEvent(ctx, b, true); err != nil {
		t.Fatal(err)
	}
	if err := h.OnStoredEvent(ctx, del, true); err != nil {
		t.Fatal(err)
	}
	assertCanonical(t, h, store, coord, b.ID)
}

func TestMissedCallbacksRepairedByBoundedPass(t *testing.T) {
	ctx := context.Background()
	host := newCanonicalHost()
	h, store := testHandler(t, filepath.Join(t.TempDir(), "index.db"), host)
	a := product("a", "bike", 10)
	b := product("b", "bike", 20)
	coord := listing.CoordOf(a.Kind, a.PubKey, "bike")
	host.put(a)
	if err := h.OnStoredEvent(ctx, a, true); err != nil {
		t.Fatal(err)
	}
	host.put(b) // B callback is dropped.
	if err := h.reconcileLiveBatch(ctx); err != nil {
		t.Fatal(err)
	}
	assertCanonical(t, h, store, coord, b.ID)
	host.remove(coord) // Deletion callback is dropped.
	if err := h.reconcileLiveBatch(ctx); err != nil {
		t.Fatal(err)
	}
	assertCanonical(t, h, store, coord, "")
}

func TestRestartAndBackfillPruneStalePersistedRevision(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "index.db")
	host := newCanonicalHost()
	a := product("a", "bike", 10)
	b := product("b", "bike", 20)
	coord := listing.CoordOf(a.Kind, a.PubKey, "bike")
	h1, store1 := testHandler(t, path, host)
	host.put(a)
	if err := h1.OnStoredEvent(ctx, a, true); err != nil {
		t.Fatal(err)
	}
	if err := store1.Close(); err != nil {
		t.Fatal(err)
	}
	host.put(b)
	h2, store2 := testHandler(t, path, host)
	h2.runBackfill(ctx)
	if h2.backfillState != "complete" || !h2.ready {
		t.Fatalf("reconciliation status %q ready=%v", h2.backfillState, h2.ready)
	}
	assertCanonical(t, h2, store2, coord, b.ID)
	host.remove(coord)
	h2.runBackfill(ctx)
	assertCanonical(t, h2, store2, coord, "")
	// A stale backfill hint cannot resurrect the deleted row.
	if err := h2.OnStoredEvent(ctx, a, true); err != nil {
		t.Fatal(err)
	}
	assertCanonical(t, h2, store2, coord, "")
}

func TestExplicitRebuildReconcilesPersistedRevision(t *testing.T) {
	ctx := context.Background()
	host := newCanonicalHost()
	h, store := testHandler(t, filepath.Join(t.TempDir(), "index.db"), host)
	a := product("a", "bike", 10)
	b := product("b", "bike", 20)
	coord := listing.CoordOf(a.Kind, a.PubKey, "bike")
	host.put(a)
	if err := h.OnStoredEvent(ctx, a, true); err != nil {
		t.Fatal(err)
	}
	host.put(b)
	if _, err := h.AdminAction(ctx, "rebuild", nil); err != nil {
		t.Fatal(err)
	}
	deadline := time.After(3 * time.Second)
	for {
		h.mu.RLock()
		ready, state := h.ready, h.backfillState
		h.mu.RUnlock()
		if ready {
			if state != "complete" {
				t.Fatalf("rebuild state %q", state)
			}
			break
		}
		select {
		case <-deadline:
			t.Fatalf("rebuild did not complete: %q", state)
		case <-time.After(10 * time.Millisecond):
		}
	}
	assertCanonical(t, h, store, coord, b.ID)
}

func TestReconciliationReadFailureIsNotComplete(t *testing.T) {
	host := newCanonicalHost()
	h, _ := testHandler(t, filepath.Join(t.TempDir(), "index.db"), host)
	host.readErr = errors.New("source unavailable")
	h.runBackfill(context.Background())
	if h.ready || !strings.HasPrefix(h.backfillState, "error:") {
		t.Fatalf("failure reported as success: %q ready=%v", h.backfillState, h.ready)
	}
}

func TestCallbackReadFailureInvalidatesReadyState(t *testing.T) {
	ctx := context.Background()
	host := newCanonicalHost()
	h, _ := testHandler(t, filepath.Join(t.TempDir(), "index.db"), host)
	a := product("a", "bike", 10)
	host.readErr = errors.New("source unavailable")
	if err := h.OnStoredEvent(ctx, a, true); err == nil {
		t.Fatal("expected source read failure")
	}
	if h.ready || !strings.HasPrefix(h.backfillState, "error:") {
		t.Fatalf("callback failure reported as ready: %q ready=%v", h.backfillState, h.ready)
	}
}

func TestReconciliationIndexFailureIsNotComplete(t *testing.T) {
	ctx := context.Background()
	host := newCanonicalHost()
	a := product("a", "bike", 10)
	host.put(a)
	store, err := index.OpenTurso(ctx, filepath.Join(t.TempDir(), "index.db"), unavailableEmbed{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	h := New(t.TempDir(), embed.Selection{Embedder: unavailableEmbed{}})
	h.store, h.host = store, host
	h.runBackfill(ctx)
	if h.ready || !strings.HasPrefix(h.backfillState, "error:") {
		t.Fatalf("index failure reported as success: %q ready=%v", h.backfillState, h.ready)
	}
	if _, ok, err := store.Get(ctx, listing.CoordOf(a.Kind, a.PubKey, "bike")); err != nil || ok {
		t.Fatalf("failed index write persisted a listing: exists=%v err=%v", ok, err)
	}
}
