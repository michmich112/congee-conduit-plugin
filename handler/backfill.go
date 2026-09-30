package handler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/michmich112/conduit-plugin/index"
	"github.com/michmich112/conduit-plugin/listing"
	sdk "github.com/michmich112/congee/sdk/plugin"
	"sync"
	"time"
)

const (
	backfillPage = 200
	livePage     = 100
	liveInterval = time.Minute
	workerCount  = 4
)

type storeFailure struct{ error }

func (h *Handler) stopWork() { h.workMu.Lock(); defer h.workMu.Unlock(); h.stopWorkLocked() }
func (h *Handler) stopWorkLocked() {
	if h.workCancel != nil {
		h.workCancel()
		<-h.workDone
		h.workCancel = nil
		h.workDone = nil
	}
}

func (h *Handler) startBackfill(parent context.Context) {
	h.workMu.Lock()
	defer h.workMu.Unlock()
	h.stopWorkLocked()
	ctx, cancel := context.WithCancel(parent)
	done := make(chan struct{})
	h.mu.Lock()
	gen := h.backfillGen.Add(1)
	store, st := h.store, h.settings
	h.backfillStarted = true
	h.backfillState = "running"
	h.backfillScanned.Store(0)
	h.backfillIndexed.Store(0)
	h.mu.Unlock()
	h.workCancel, h.workDone = cancel, done
	go func() { defer close(done); h.workLoop(ctx, gen, st, store) }()
}

// Close cancels work and waits before closing the store. Serve's caller owns this lifetime.
func (h *Handler) Close() error {
	h.applyMu.Lock()
	defer h.applyMu.Unlock()
	h.stopWork()
	h.lifecycleMu.Lock()
	defer h.lifecycleMu.Unlock()
	h.mu.Lock()
	store := h.store
	h.store = nil
	h.ready = false
	h.mu.Unlock()
	if store != nil {
		return store.Close()
	}
	return nil
}

func (h *Handler) workLoop(ctx context.Context, gen int64, st Settings, store index.Store) {
	var source *sdk.EventCursor
	after := ""
	sourceDone, persistedDone := false, false
	var restart time.Time
	for ctx.Err() == nil && h.backfillGen.Load() == gen {
		h.lifecycleMu.RLock()
		worked, err := h.workStep(ctx, st, store, &source, &after, &sourceDone, &persistedDone)
		h.lifecycleMu.RUnlock()
		if ctx.Err() != nil {
			return
		}
		h.mu.Lock()
		if h.backfillGen.Load() == gen {
			if err != nil {
				h.lastErr = err.Error()
				var failure storeFailure
				if errors.As(err, &failure) {
					h.ready = false
				}
				h.backfillState = "retrying: " + err.Error()
			} else if sourceDone && persistedDone {
				h.backfillState = "sweep complete"
				h.lastErr = ""
				h.ready = true
			} else {
				h.backfillState = "running"
			}
		}
		h.mu.Unlock()
		if sourceDone && persistedDone {
			if restart.IsZero() {
				restart = time.Now().Add(liveInterval)
			}
			if !time.Now().Before(restart) {
				source = nil
				after = ""
				sourceDone = false
				persistedDone = false
				restart = time.Time{}
			}
		}
		delay := 250 * time.Millisecond
		if !worked || err != nil {
			delay = time.Second
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func (h *Handler) workStep(ctx context.Context, st Settings, store index.Store, source **sdk.EventCursor, after *string, sourceDone, persistedDone *bool) (bool, error) {
	q, ok := store.(index.ReconcileStore)
	if !ok || h.host == nil {
		return false, fmt.Errorf("canonical host or durable index unavailable")
	}
	jobs, err := q.Jobs(ctx, time.Now().UnixMilli(), workerCount)
	if err != nil {
		return false, storeFailure{err}
	}
	var wg sync.WaitGroup
	var mu sync.Mutex
	var fatal error
	for _, job := range jobs {
		wg.Add(1)
		go func(j index.ReconcileJob) {
			defer wg.Done()
			jobCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			defer cancel()
			err := h.processJob(jobCtx, st, store, q, j)
			if err != nil && !errors.Is(err, index.ErrSuperseded) && !errors.Is(err, index.ErrMoreWork) && ctx.Err() == nil {
				if e := q.RetryJob(ctx, j, err); e != nil {
					mu.Lock()
					fatal = e
					mu.Unlock()
				}
			}
		}(job)
	}
	wg.Wait()
	if fatal != nil {
		return true, storeFailure{fatal}
	}
	if !*persistedDone {
		coords, err := q.TargetsAfter(ctx, *after, livePage)
		if err != nil {
			return true, storeFailure{err}
		}
		for _, coord := range coords {
			if err := q.Invalidate(ctx, jobForCoord(coord), false); err != nil {
				return true, storeFailure{err}
			}
		}
		if len(coords) < livePage {
			*persistedDone = true
		} else {
			*after = coords[len(coords)-1]
		}
	}
	if !*sourceDone {
		p, err := h.page(ctx, sdk.Filter{Kinds: st.allIndexKinds()}, *source, backfillPage)
		if err != nil {
			return true, err
		}
		for _, ev := range p.Events {
			l, valid := listing.FromEvent(toListingEvent(ev), st.IndexDrafts)
			if valid && !l.IsDeletion {
				existing, found, err := store.Get(ctx, l.Coord)
				if err != nil {
					return true, storeFailure{err}
				}
				if found && existing.EventID == ev.ID {
					continue
				}
			}
			if err := h.enqueueHint(ctx, ev, st, store, false); err != nil {
				return true, storeFailure{err}
			}
		}
		*source = p.Next
		*sourceDone = p.Next == nil
		h.backfillScanned.Add(int64(len(p.Events)))
	}
	return len(jobs) > 0 || !*sourceDone || !*persistedDone, nil
}

func (h *Handler) page(ctx context.Context, f sdk.Filter, c *sdk.EventCursor, size int) (sdk.EventPage, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	host, ok := h.host.(sdk.PagedHost)
	if !ok {
		return sdk.EventPage{}, fmt.Errorf("Congee host requires cursor paging SDK support")
	}
	p, err := host.QueryEventsPage(ctx, f, c, size)
	if err == nil && len(p.Events) > size {
		return sdk.EventPage{}, fmt.Errorf("host exceeded page bound")
	}
	if err == nil && p.Next != nil && c != nil && p.Next.CreatedAt == c.CreatedAt && p.Next.ID == c.ID {
		return sdk.EventPage{}, fmt.Errorf("host cursor did not advance")
	}
	return p, err
}

func (h *Handler) processJob(ctx context.Context, st Settings, store index.Store, q index.ReconcileStore, j index.ReconcileJob) error {
	if j.Coord != "" {
		kind, author, d, ok := splitCoord(j.Coord)
		if !ok {
			return fmt.Errorf("invalid coordinate %q", j.Coord)
		}
		p, err := h.page(ctx, sdk.Filter{Kinds: []int{kind}, Authors: []string{author}, Tags: map[string][]string{"d": {d}}}, nil, 1)
		if err != nil {
			return err
		}
		var winner *listing.Listing
		for _, ev := range p.Events {
			l, ok := listing.FromEvent(toListingEvent(ev), st.IndexDrafts)
			if ok && !l.IsDeletion && l.Coord == j.Coord {
				winner = &l
			}
		}
		if err := q.ApplyJob(ctx, j, winner); err != nil {
			return err
		}
		// Embedding can take seconds. Keep the coordinate hidden until a fresh
		// source read confirms the committed revision is still current.
		latest, err := h.page(ctx, sdk.Filter{Kinds: []int{kind}, Authors: []string{author}, Tags: map[string][]string{"d": {d}}}, nil, 1)
		if err != nil {
			return err
		}
		id := ""
		if len(p.Events) > 0 {
			id = p.Events[0].ID
		}
		latestID := ""
		if len(latest.Events) > 0 {
			latestID = latest.Events[0].ID
		}
		if latestID != id {
			if err := q.Invalidate(ctx, j, true); err != nil {
				return err
			}
			return index.ErrSuperseded
		}
		h.backfillIndexed.Add(1)
		return q.FinishJob(ctx, j)
	}
	if j.Phase == "cleanup" {
		return q.FinishJob(ctx, j)
	}
	if j.Phase == "scan" {
		var cursor *sdk.EventCursor
		if j.Cursor != "" {
			cursor = new(sdk.EventCursor)
			if err := json.Unmarshal([]byte(j.Cursor), cursor); err != nil {
				return err
			}
		}
		p, err := h.page(ctx, sdk.Filter{Kinds: []int{j.Kind}, Authors: []string{j.Author}}, cursor, backfillPage)
		if err != nil {
			return err
		}
		var ls []listing.Listing
		for _, ev := range p.Events {
			l, ok := listing.FromEvent(toListingEvent(ev), st.IndexDrafts)
			if ok && !l.IsDeletion {
				ls = append(ls, l)
			}
		}
		next, phase := "", "apply"
		if p.Next != nil {
			b, err := json.Marshal(p.Next)
			if err != nil {
				return err
			}
			next = string(b)
			phase = "scan"
		}
		return q.Stage(ctx, j, ls, next, phase)
	}
	ls, err := q.Staged(ctx, j, 10)
	if err != nil {
		return err
	}
	for _, l := range ls {
		if err := q.ApplyJob(ctx, j, &l); err != nil {
			return err
		}
		h.backfillIndexed.Add(1)
	}
	if len(ls) > 0 {
		return q.Stage(ctx, j, nil, ls[len(ls)-1].Coord, "apply")
	}
	return q.FinishJob(ctx, j)
}
