package handler

import (
	"context"

	"github.com/michmich112/conduit-plugin/index"
	"github.com/michmich112/conduit-plugin/nip85"
	sdk "github.com/michmich112/congee/sdk/plugin"
)

const rankBackfillBatch = 100

// Backfill makes the index recoverable when assertions arrived before a
// listing or a plugin notification was dropped. The host owns signed events.
func (h *Handler) startRankBackfill(ctx context.Context) {
	h.mu.RLock()
	store, provider := h.store, h.settings.NIP85ProviderPubkey
	h.mu.RUnlock()
	if h.host == nil || store == nil || provider == "" {
		return
	}
	go func() {
		targets, err := store.MerchantPubkeys(ctx)
		if err == nil {
			for start := 0; start < len(targets); start += rankBackfillBatch {
				end := start + rankBackfillBatch
				if end > len(targets) {
					end = len(targets)
				}
				if err = h.backfillRankTargets(ctx, store, provider, targets[start:end]); err != nil {
					break
				}
			}
		}
		if err != nil {
			h.log(ctx, "warn", "NIP-85 backfill failed", map[string]string{"error": err.Error()})
		}
	}()
}

func (h *Handler) backfillRankTargets(ctx context.Context, store index.Store, provider string, targets []string) error {
	if h.host == nil || provider == "" || len(targets) == 0 {
		return nil
	}
	evs, err := h.host.QueryEvents(ctx, []sdk.Filter{{
		Kinds:   []int{nip85.KindUserAssertion},
		Authors: []string{provider},
		Tags:    map[string][]string{"d": targets},
	}})
	if err != nil {
		return err
	}
	allowed := make(map[string]bool, len(targets))
	for _, target := range targets {
		allowed[target] = true
	}
	for _, ev := range evs {
		assertion, ok := nip85.ParseUserRank(ev)
		if !ok || assertion.Provider != provider || !allowed[assertion.Target] {
			continue
		}
		if err := store.UpsertUserRank(ctx, assertion); err != nil {
			return err
		}
	}
	return nil
}
