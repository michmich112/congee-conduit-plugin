package handler

import (
	"context"
	"strconv"

	"github.com/michmich112/conduit-plugin/listing"
	sdk "github.com/michmich112/congee/sdk/plugin"
)

const backfillPage = 200
const metaWatermark = "backfill_until"

func (h *Handler) startBackfill(ctx context.Context) {
	h.mu.Lock()
	if h.backfillStarted {
		h.mu.Unlock()
		return
	}
	h.backfillStarted = true
	h.mu.Unlock()
	go h.runBackfill(ctx)
}

func (h *Handler) runBackfill(ctx context.Context) {
	myGen := h.backfillGen.Load()
	if h.host == nil || h.store == nil {
		if h.backfillGen.Load() == myGen {
			h.setBackfill("idle")
		}
		return
	}
	h.setBackfill("running")
	wm, _ := h.store.Meta(ctx, metaWatermark)
	var until *int64
	if wm != "" {
		if n, err := strconv.ParseInt(wm, 10, 64); err == nil {
			until = &n
		}
	}
	kinds := h.settings.allIndexKinds()
	var since int64
	for {
		if ctx.Err() != nil {
			if h.backfillGen.Load() == myGen {
				h.setBackfill("stopped")
			}
			return
		}
		if h.backfillGen.Load() != myGen {
			return
		}
		lim := backfillPage
		f := sdk.Filter{Kinds: kinds, Limit: &lim}
		if until != nil {
			u := *until - 1
			f.Until = &u
		}
		evs, err := h.host.QueryEvents(ctx, []sdk.Filter{f})
		if err != nil {
			if h.backfillGen.Load() == myGen {
				h.setBackfill("error: " + err.Error())
			}
			return
		}
		if len(evs) == 0 {
			break
		}
		h.backfillScanned.Add(int64(len(evs)))
		var minCreated int64
		for i, ev := range evs {
			l, ok := listing.FromEvent(listing.Event{
				ID: ev.ID, PubKey: ev.PubKey, CreatedAt: ev.CreatedAt, Kind: ev.Kind, Tags: ev.Tags, Content: ev.Content,
			}, h.settings.IndexDrafts)
			if ok {
				if err := h.store.Upsert(ctx, l); err != nil {
					h.log(ctx, "error", "backfill upsert", map[string]string{"error": err.Error()})
				} else {
					h.backfillIndexed.Add(1)
				}
			}
			if i == 0 || ev.CreatedAt < minCreated {
				minCreated = ev.CreatedAt
			}
		}
		until = &minCreated
		_ = h.store.SetMeta(ctx, metaWatermark, strconv.FormatInt(minCreated, 10))
		if len(evs) < backfillPage {
			break
		}
		if since > 0 && minCreated <= since {
			break
		}
	}
	if h.backfillGen.Load() == myGen {
		h.setBackfill("complete")
		h.startRankBackfill(ctx)
	}
}

func (h *Handler) setBackfill(s string) {
	h.mu.Lock()
	h.backfillState = s
	h.mu.Unlock()
}
