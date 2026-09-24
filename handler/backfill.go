package handler

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/michmich112/conduit-plugin/index"
	sdk "github.com/michmich112/congee/sdk/plugin"
)

const (
	backfillPage = 200
	livePage     = 100
	liveInterval = time.Minute
)

func (h *Handler) startBackfill(ctx context.Context) {
	h.mu.Lock()
	if h.backfillStarted {
		h.mu.Unlock()
		return
	}
	h.backfillStarted = true
	h.backfillState = "running"
	h.ready = false
	h.mu.Unlock()
	go h.runBackfill(ctx)
}

func (h *Handler) startPeriodic(ctx context.Context) {
	h.periodicOnce.Do(func() { go h.periodicReconcile(ctx) })
}

func (h *Handler) runBackfill(ctx context.Context) {
	gen := h.backfillGen.Load()
	h.mu.RLock()
	store, st := h.store, h.settings
	h.mu.RUnlock()
	if h.host == nil || store == nil {
		h.finishBackfill(gen, "error: canonical host or index unavailable", fmt.Errorf("canonical host or index unavailable"))
		return
	}
	if err := h.reconcileStored(ctx, gen, st, store); err != nil {
		h.finishBackfill(gen, "error: "+err.Error(), err)
		return
	}
	partial, err := h.discoverCanonical(ctx, gen, st, store)
	if err != nil {
		h.finishBackfill(gen, "error: "+err.Error(), err)
		return
	}
	state := "complete"
	if partial {
		state = "partial: host time-only paging may skip same-second events"
	}
	h.finishBackfill(gen, state, nil)
}

func (h *Handler) reconcileStored(ctx context.Context, gen int64, st Settings, store index.Store) error {
	after := ""
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if h.backfillGen.Load() != gen {
			return context.Canceled
		}
		coords, err := store.CoordsAfter(ctx, after, backfillPage)
		if err != nil {
			return fmt.Errorf("list persisted coordinates: %w", err)
		}
		for _, coord := range coords {
			if err := h.reconcileCoord(ctx, coord, st, store); err != nil {
				return err
			}
			h.backfillScanned.Add(1)
			if h.backfillGen.Load() != gen {
				return context.Canceled
			}
		}
		if len(coords) < backfillPage {
			return nil
		}
		after = coords[len(coords)-1]
	}
}

func (h *Handler) discoverCanonical(ctx context.Context, gen int64, st Settings, store index.Store) (bool, error) {
	var until *int64
	partial := false
	for {
		if err := ctx.Err(); err != nil {
			return partial, err
		}
		if h.backfillGen.Load() != gen {
			return partial, context.Canceled
		}
		lim := backfillPage
		evs, err := h.host.QueryEvents(ctx, []sdk.Filter{{Kinds: st.allIndexKinds(), Limit: &lim, Until: until}})
		if err != nil {
			return partial, fmt.Errorf("canonical discovery: %w", err)
		}
		if len(evs) == 0 {
			return partial, nil
		}
		var minCreated int64
		for i, ev := range evs {
			if err := h.reconcileHint(ctx, ev, st, store); err != nil {
				return partial, fmt.Errorf("index canonical event %s: %w", ev.ID, err)
			}
			h.backfillIndexed.Add(1)
			if i == 0 || ev.CreatedAt < minCreated {
				minCreated = ev.CreatedAt
			}
		}
		h.backfillScanned.Add(int64(len(evs)))
		if len(evs) < backfillPage {
			return partial, nil
		}
		// No (created_at, ID) cursor exists in the current host API.
		partial = true
		next := minCreated - 1
		until = &next
	}
}

func (h *Handler) periodicReconcile(ctx context.Context) {
	ticker := time.NewTicker(liveInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			h.mu.RLock()
			state := h.backfillState
			h.mu.RUnlock()
			if state == "running" {
				continue
			}
			if strings.HasPrefix(state, "error:") {
				h.startBackfill(ctx)
				continue
			}
			if err := h.reconcileLiveBatch(ctx); err != nil {
				h.setBackfill("error: " + err.Error())
				h.log(ctx, "error", "periodic index reconciliation", map[string]string{"error": err.Error()})
			}
		}
	}
}

// reconcileLiveBatch bounds each pass while repairing missed callbacks for
// persisted coordinates and recently stored new coordinates.
func (h *Handler) reconcileLiveBatch(ctx context.Context) error {
	h.mu.RLock()
	store, st, after := h.store, h.settings, h.liveCursor
	h.mu.RUnlock()
	if h.host == nil || store == nil {
		return fmt.Errorf("canonical host or index unavailable")
	}
	coords, err := store.CoordsAfter(ctx, after, livePage)
	if err != nil {
		return err
	}
	for _, coord := range coords {
		if err := h.reconcileCoord(ctx, coord, st, store); err != nil {
			return err
		}
	}
	lim := livePage
	evs, err := h.host.QueryEvents(ctx, []sdk.Filter{{Kinds: st.allIndexKinds(), Limit: &lim}})
	if err != nil {
		return fmt.Errorf("recent canonical events: %w", err)
	}
	for _, ev := range evs {
		if err := h.reconcileHint(ctx, ev, st, store); err != nil {
			return err
		}
	}
	h.mu.Lock()
	if len(coords) == livePage {
		h.liveCursor = coords[len(coords)-1]
	} else {
		h.liveCursor = ""
	}
	h.mu.Unlock()
	return nil
}

func (h *Handler) finishBackfill(gen int64, state string, err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.backfillGen.Load() != gen {
		return
	}
	h.backfillState = state
	h.backfillStarted = err == nil
	h.ready = err == nil
	if err != nil {
		h.lastErr = err.Error()
	} else {
		h.lastErr = ""
	}
}

func (h *Handler) setBackfill(s string) {
	h.mu.Lock()
	h.backfillState = s
	h.ready = false
	h.backfillStarted = false
	h.lastErr = s
	h.mu.Unlock()
}
