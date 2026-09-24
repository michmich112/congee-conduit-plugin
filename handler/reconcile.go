package handler

import (
	"context"
	"fmt"
	"hash/fnv"
	"sort"
	"strconv"
	"strings"

	"github.com/michmich112/conduit-plugin/index"
	"github.com/michmich112/conduit-plugin/listing"
	sdk "github.com/michmich112/congee/sdk/plugin"
)

const legacyCoordReadLimit = 1001

// reconcileCoord treats a notification as an invalidation of one coordinate.
// Its payload is never the index source of truth.
func (h *Handler) reconcileCoord(ctx context.Context, coord string, st Settings, store index.Store) error {
	lock := &h.coordLocks[coordStripe(coord)%uint32(len(h.coordLocks))]
	lock.Lock()
	defer lock.Unlock()
	if h.host == nil {
		return fmt.Errorf("canonical event host unavailable")
	}
	kind, author, d, ok := splitCoord(coord)
	if !ok {
		return fmt.Errorf("invalid coordinate %q", coord)
	}
	f := sdk.Filter{Kinds: []int{kind}, Authors: []string{author}}
	legacy := kind == listing.KindProduct || kind == listing.KindStall
	if legacy {
		lim := legacyCoordReadLimit
		f.Limit = &lim
	} else if d != "" {
		f.Tags = map[string][]string{"d": {d}}
		lim := 1
		f.Limit = &lim
	} // Legacy product/stall events may identify themselves only in JSON content.
	evs, err := h.host.QueryEvents(ctx, []sdk.Filter{f})
	if err != nil {
		return fmt.Errorf("canonical read %s: %w", coord, err)
	}
	if legacy && len(evs) >= legacyCoordReadLimit {
		return fmt.Errorf("canonical read %s exceeds bounded legacy scan; Congee coordinate lookup is required", coord)
	}
	var winner listing.Listing
	found := false
	for _, ev := range evs {
		l, ok := listing.FromEvent(toListingEvent(ev), st.IndexDrafts)
		if !ok || l.IsDeletion || l.Coord != coord {
			continue
		}
		if !found || newer(l, winner) {
			winner, found = l, true
		}
	}
	if !found {
		return store.DeleteCoord(ctx, coord)
	}
	return store.ReplaceCanonical(ctx, winner)
}

func newer(a, b listing.Listing) bool {
	return a.CreatedAt > b.CreatedAt || (a.CreatedAt == b.CreatedAt && a.EventID < b.EventID)
}

func (h *Handler) reconcileHint(ctx context.Context, ev sdk.Event, st Settings, store index.Store) error {
	if ev.Kind == listing.KindDeletion {
		deletion, ok := listing.FromEvent(toListingEvent(ev), st.IndexDrafts)
		if !ok {
			return nil
		}
		coords := make(map[string]struct{}, len(deletion.DeleteCoords))
		for _, coord := range deletion.DeleteCoords {
			coords[coord] = struct{}{}
		}
		byID, err := store.CoordsForEventIDs(ctx, ev.PubKey, deletion.DeleteEventIDs)
		if err != nil {
			return err
		}
		for _, coord := range byID {
			coords[coord] = struct{}{}
		}
		ordered := make([]string, 0, len(coords))
		for coord := range coords {
			ordered = append(ordered, coord)
		}
		sort.Strings(ordered)
		for _, coord := range ordered {
			if err := h.reconcileCoord(ctx, coord, st, store); err != nil {
				return err
			}
		}
		return nil
	}
	coord := coordFromHint(ev, st)
	if coord == "" {
		return nil
	}
	return h.reconcileCoord(ctx, coord, st, store)
}

func coordFromHint(ev sdk.Event, st Settings) string {
	l, ok := listing.FromEvent(toListingEvent(ev), st.IndexDrafts)
	if ok && !l.IsDeletion {
		return l.Coord
	}
	for _, tag := range ev.Tags {
		if len(tag) >= 2 && tag[0] == "d" && tag[1] != "" {
			return listing.CoordOf(ev.Kind, ev.PubKey, tag[1])
		}
	}
	return ""
}

func toListingEvent(ev sdk.Event) listing.Event {
	return listing.Event{ID: ev.ID, PubKey: ev.PubKey, CreatedAt: ev.CreatedAt, Kind: ev.Kind, Tags: ev.Tags, Content: ev.Content}
}

func splitCoord(coord string) (int, string, string, bool) {
	parts := strings.SplitN(coord, ":", 3)
	if len(parts) != 3 || parts[1] == "" {
		return 0, "", "", false
	}
	kind, err := strconv.Atoi(parts[0])
	return kind, parts[1], parts[2], err == nil
}

func coordStripe(coord string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(coord))
	return h.Sum32()
}
