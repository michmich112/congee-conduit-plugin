package handler

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/michmich112/conduit-plugin/index"
	"github.com/michmich112/conduit-plugin/listing"
	sdk "github.com/michmich112/congee/sdk/plugin"
)

func jobForCoord(coord string) index.ReconcileJob {
	kind, author, _, _ := splitCoord(coord)
	j := index.ReconcileJob{Key: coord, Coord: coord, Author: author, Kind: kind}
	if kind == listing.KindProduct || kind == listing.KindStall {
		j.Key = fmt.Sprintf("%d:%s:*", kind, author)
		j.Coord = ""
	}
	return j
}

func (h *Handler) enqueueHint(ctx context.Context, ev sdk.Event, st Settings, store index.Store, force bool) error {
	q, ok := store.(index.ReconcileStore)
	if !ok {
		return fmt.Errorf("durable reconciliation store unavailable")
	}

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
			if err := q.Invalidate(ctx, jobForCoord(coord), force); err != nil {
				return err
			}
		}
		return nil
	}
	coord := coordFromHint(ev, st)
	if coord == "" {
		return nil
	}
	return q.Invalidate(ctx, jobForCoord(coord), force)
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
