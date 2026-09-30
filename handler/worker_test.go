package handler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/michmich112/conduit-plugin/embed"
	"github.com/michmich112/conduit-plugin/index"
	"github.com/michmich112/conduit-plugin/listing"
	sdk "github.com/michmich112/congee/sdk/plugin"
)

func legacyProduct(n int) sdk.Event {
	return sdk.Event{ID: fmt.Sprintf("%064x", n+1), PubKey: testAuthor, Kind: listing.KindProduct, CreatedAt: 10,
		Content: fmt.Sprintf(`{"id":"product-%04d","name":"Bicycle","description":"bicycle %d"}`, n, n)}
}

func TestDiscoveryCrossesSameSecondAndFindsOldMissedCoordinate(t *testing.T) {
	host := newCanonicalHost()
	h, store := testHandler(t, filepath.Join(t.TempDir(), "index.db"), host)
	for i := 0; i < 451; i++ {
		ev := product("a", fmt.Sprint(i), 10)
		ev.ID = fmt.Sprintf("%064x", i+1)
		host.put(ev)
	}
	old := product("f", "old-missed", 1)
	host.put(old)
	if err := h.reconcileLiveBatch(context.Background()); err != nil {
		t.Fatal(err)
	}
	coords, err := store.CoordsAfter(context.Background(), "", 1000)
	if err != nil || len(coords) != 452 {
		t.Fatalf("discovered=%d err=%v", len(coords), err)
	}
	if _, ok, err := store.Get(context.Background(), coordFromHint(old, h.settings)); err != nil || !ok {
		t.Fatalf("old missed coordinate: %v %v", ok, err)
	}
}

func TestLegacyScanResumesWithoutPruningAndHasNoThousandEventCeiling(t *testing.T) {
	ctx := context.Background()
	host := newCanonicalHost()
	path := filepath.Join(t.TempDir(), "index.db")
	h, store := testHandler(t, path, host)
	q := store.(index.ReconcileStore)
	for i := 0; i < 1103; i++ {
		host.put(legacyProduct(i))
	}
	obsolete := legacyProduct(2000)
	l, _ := listing.FromEvent(toListingEvent(obsolete), false)
	if err := store.Upsert(ctx, l); err != nil {
		t.Fatal(err)
	}
	j := jobForCoord(l.Coord)
	if err := q.Invalidate(ctx, j, true); err != nil {
		t.Fatal(err)
	}
	jobs, err := q.Jobs(ctx, time.Now().UnixMilli(), 4)
	if err != nil || len(jobs) != 1 {
		t.Fatalf("jobs %v %v", jobs, err)
	}
	if err := h.processJob(ctx, h.settings, store, q, jobs[0]); err != nil {
		t.Fatal(err)
	}
	jobs, _ = q.Jobs(ctx, time.Now().UnixMilli(), 4)
	if jobs[0].Cursor == "" || jobs[0].Phase != "scan" {
		t.Fatal("scan was not checkpointed")
	}
	host.readErr = errors.New("interrupted source")
	if err := h.processJob(ctx, h.settings, store, q, jobs[0]); err == nil {
		t.Fatal("expected source failure")
	}
	if _, ok, err := store.Get(ctx, l.Coord); err != nil || !ok {
		t.Fatalf("partial scan pruned existing row: %v %v", ok, err)
	}
	if err := q.RetryJob(ctx, jobs[0], host.readErr); err != nil {
		t.Fatal(err)
	}
	host.readErr = nil
	if err := h.Close(); err != nil {
		t.Fatal(err)
	}
	h, store = testHandler(t, path, host)
	q = store.(index.ReconcileStore)
	jobs, err = q.Jobs(ctx, time.Now().Add(time.Hour).UnixMilli(), 4)
	if err != nil || len(jobs) != 1 || jobs[0].Cursor == "" || jobs[0].Attempts != 1 {
		t.Fatalf("durable retry %v %v", jobs, err)
	}
	// Advance only the persisted merchant job, preserving its checkpoint.
	for i := 0; i < 200; i++ {
		jobs, err = q.Jobs(ctx, time.Now().Add(time.Hour).UnixMilli(), 4)
		if err != nil {
			t.Fatal(err)
		}
		if len(jobs) == 0 {
			break
		}
		if err := h.processJob(ctx, h.settings, store, q, jobs[0]); err != nil && !errors.Is(err, index.ErrMoreWork) {
			t.Fatal(err)
		}
	}
	stats, _ := q.JobStats(ctx)
	if stats["pending"].(int64) != 0 {
		t.Fatalf("did not converge: %v", stats)
	}
	coords, err := store.CoordsAfter(ctx, "", 2000)
	if err != nil || len(coords) != 1103 {
		t.Fatalf("retained %d err=%v", len(coords), err)
	}
	if _, ok, _ := store.Get(ctx, l.Coord); ok {
		t.Fatal("complete scan did not prune absent coordinate")
	}
	targets, err := q.TargetsAfter(ctx, "", 100)
	if err != nil || len(targets) != 1 {
		t.Fatalf("merchant scans should be grouped: %v %v", targets, err)
	}
}

type waitingEmbed struct {
	embed.Fake
	entered, release chan struct{}
}

func (e waitingEmbed) Embed(ctx context.Context, text string) ([]float32, error) {
	select {
	case e.entered <- struct{}{}:
	default:
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-e.release:
		return e.Fake.Embed(ctx, text)
	}
}

func TestSourceChangeDuringEmbeddingWithoutCallbackStaysDirty(t *testing.T) {
	ctx := context.Background()
	e := waitingEmbed{Fake: embed.Fake{}, entered: make(chan struct{}, 1), release: make(chan struct{})}
	store, err := index.OpenTurso(ctx, filepath.Join(t.TempDir(), "index.db"), e)
	if err != nil {
		t.Fatal(err)
	}
	h := New(t.TempDir(), embed.Selection{Embedder: e})
	host := newCanonicalHost()
	h.store, h.host, h.ready = store, host, true
	t.Cleanup(func() { h.Close() })
	a, b := product("a", "bike", 10), product("b", "bike", 20)
	host.put(a)
	if err := h.OnStoredEvent(ctx, a, true); err != nil {
		t.Fatal(err)
	}
	q := store.(index.ReconcileStore)
	jobs, _ := q.Jobs(ctx, time.Now().UnixMilli(), 4)
	done := make(chan error, 1)
	go func() { done <- h.processJob(ctx, h.settings, store, q, jobs[0]) }()
	select {
	case <-e.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("embedding did not start")
	}
	host.put(b)
	close(e.release)
	if err := <-done; !errors.Is(err, index.ErrSuperseded) {
		t.Fatalf("stale source: %v", err)
	}
	ids, err := store.Search(ctx, index.Query{Search: "bicycle", Kinds: []int{listing.KindClassified}, Limit: 10, ActiveOnly: true})
	if err != nil || len(ids) != 0 {
		t.Fatalf("dirty revision exposed: %v %v", ids, err)
	}
	if err := h.reconcileLiveBatch(ctx); err != nil {
		t.Fatal(err)
	}
	row, ok, err := store.Get(ctx, coordFromHint(b, h.settings))
	if err != nil || !ok || row.EventID != b.ID {
		t.Fatalf("latest revision %+v %v %v", row, ok, err)
	}
}

type cancelHost struct {
	*canonicalHost
	entered           chan struct{}
	active, maxActive atomic.Int32
}

func (h *cancelHost) QueryEventsPage(ctx context.Context, _ sdk.Filter, _ *sdk.EventCursor, _ int) (sdk.EventPage, error) {
	n := h.active.Add(1)
	defer h.active.Add(-1)
	for {
		old := h.maxActive.Load()
		if n <= old || h.maxActive.CompareAndSwap(old, n) {
			break
		}
	}
	h.entered <- struct{}{}
	<-ctx.Done()
	return sdk.EventPage{}, ctx.Err()
}
func TestRebuildAndSettingsWaitForCancellationBeforeReplacingStore(t *testing.T) {
	t.Setenv("CONDUIT_EMBEDDER", "fake")
	ctx := context.Background()
	base := newCanonicalHost()
	h, _ := testHandler(t, filepath.Join(t.TempDir(), "index.db"), base)
	host := &cancelHost{canonicalHost: base, entered: make(chan struct{}, 10)}
	h.SetHost(host)
	h.startBackfill(ctx)
	select {
	case <-host.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("worker did not start")
	}
	if _, err := h.AdminAction(ctx, "rebuild", nil); err != nil {
		t.Fatal(err)
	}
	select {
	case <-host.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("replacement worker did not start")
	}
	// Calls leasing the old store must finish before settings closes it.
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 20; i++ {
			h.Status(ctx)
			h.InterceptREQ(ctx, sdk.Req{Filters: []sdk.Filter{{Kinds: []int{listing.KindClassified}, Search: "bicycle"}}})
		}
	}()
	if _, err := h.ApplySettings(ctx, nil); err != nil {
		t.Fatal(err)
	}
	<-done
	if err := h.Close(); err != nil {
		t.Fatal(err)
	}
	if host.active.Load() != 0 || host.maxActive.Load() != 1 {
		t.Fatalf("workers overlapped: active=%d max=%d", host.active.Load(), host.maxActive.Load())
	}
}

type oneFailureEmbed struct{ embed.Fake }

func (e oneFailureEmbed) Embed(ctx context.Context, text string) ([]float32, error) {
	if strings.Contains(text, "poison") {
		return nil, errors.New("temporary embedding failure")
	}
	return e.Fake.Embed(ctx, text)
}
func TestWorkerRetriesOneCoordinateWhileUnrelatedSearchRemainsAvailable(t *testing.T) {
	ctx := context.Background()
	e := oneFailureEmbed{}
	store, err := index.OpenTurso(ctx, filepath.Join(t.TempDir(), "index.db"), e)
	if err != nil {
		t.Fatal(err)
	}
	h := New(t.TempDir(), embed.Selection{Embedder: e})
	host := newCanonicalHost()
	h.store, h.host = store, host
	t.Cleanup(func() { h.Close() })
	bad, good := product("a", "bad", 10), product("b", "good", 10)
	bad.Content = "poison bicycle"
	host.put(bad)
	host.put(good)
	if err := h.OnStoredEvent(ctx, bad, true); err != nil {
		t.Fatal(err)
	}
	if err := h.OnStoredEvent(ctx, good, true); err != nil {
		t.Fatal(err)
	}
	h.startBackfill(ctx)
	deadline := time.Now().Add(3 * time.Second)
	for {
		status, err := h.Status(ctx)
		if err != nil {
			t.Fatal(err)
		}
		var stats struct {
			Reconciliation struct{ Pending, Failed int } `json:"reconciliation"`
		}
		if err := json.Unmarshal(status.JSON, &stats); err != nil {
			t.Fatal(err)
		}
		if status.Ready && stats.Reconciliation.Pending == 1 && stats.Reconciliation.Failed == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("retry did not isolate failure: %s", status.JSON)
		}
		time.Sleep(10 * time.Millisecond)
	}
	res, err := h.InterceptREQ(ctx, sdk.Req{Filters: []sdk.Filter{{Kinds: []int{listing.KindClassified}, Search: "bicycle"}}})
	if err != nil || res.Action != sdk.InterceptRespond || len(res.EventIDs) != 1 || res.EventIDs[0] != good.ID {
		t.Fatalf("unrelated search unavailable: %+v %v", res, err)
	}
	jobs, err := store.(index.ReconcileStore).Jobs(ctx, time.Now().Add(time.Hour).UnixMilli(), 4)
	if err != nil || len(jobs) != 1 || jobs[0].Attempts < 1 || jobs[0].LastError == "" {
		t.Fatalf("retry not durable: %+v %v", jobs, err)
	}
}

func TestUnindexableCanonicalEventDoesNotRetryForever(t *testing.T) {
	ctx := context.Background()
	host := newCanonicalHost()
	h, store := testHandler(t, filepath.Join(t.TempDir(), "index.db"), host)
	ev := product("a", "custom", 10)
	ev.Kind = 42
	host.put(ev)
	// Custom indexed kinds can contain events the marketplace parser ignores.
	h.settings.ProductKinds = []int{42}
	old := listing.Listing{Coord: coordFromHint(ev, h.settings), EventID: "old", Kind: 42, PubKey: testAuthor, DTag: "custom", Status: listing.StatusActive, Body: "bicycle", CreatedAt: 1}
	old.TextHash = old.ComputeTextHash()
	if err := store.Upsert(ctx, old); err != nil {
		t.Fatal(err)
	}
	if err := h.OnStoredEvent(ctx, ev, true); err != nil {
		t.Fatal(err)
	}
	q := store.(index.ReconcileStore)
	jobs, err := q.Jobs(ctx, time.Now().UnixMilli(), 4)
	if err != nil || len(jobs) != 1 {
		t.Fatalf("jobs %v %v", jobs, err)
	}
	if err := h.processJob(ctx, h.settings, store, q, jobs[0]); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := store.Get(ctx, old.Coord); err != nil || ok {
		t.Fatalf("unindexable canonical event left stale row: %v %v", ok, err)
	}
	stats, _ := q.JobStats(ctx)
	if stats["pending"].(int64) != 0 {
		t.Fatalf("unindexable event keeps retrying: %v", stats)
	}
}
