package index

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/michmich112/conduit-plugin/listing"
)

func searchSQL(ctx context.Context, s *sqlStore, q Query) ([]string, error) {
	limit := clampLimit(q.Limit, 0)
	capN := q.SearchCandidateCap
	if capN <= 0 {
		capN = 2000
	}
	minPrefix := q.GeoMinPrefixLen
	if minPrefix <= 0 {
		minPrefix = 2
	}
	wantSearch := q.VectorEnabled && s.embedder != nil && strings.TrimSpace(q.Search) != ""
	needGeoJoin := q.GeoEnabled && len(q.GeoPrefixes) > 0
	sortByProximity := needGeoJoin && !wantSearch

	where := []string{"1=1"}
	args := []any{}
	n := 1
	add := func(cond string, v ...any) {
		where = append(where, cond)
		args = append(args, v...)
		n += len(v)
	}

	if q.ActiveOnly {
		add("l.status = "+s.ph(n), listing.StatusActive)
	}
	if len(q.Kinds) > 0 {
		phs := make([]string, len(q.Kinds))
		for i, k := range q.Kinds {
			phs[i] = s.ph(n)
			args = append(args, k)
			n++
			_ = i
		}
		where = append(where, "l.kind IN ("+strings.Join(phs, ",")+")")
	}
	if len(q.Authors) > 0 {
		phs := make([]string, len(q.Authors))
		for i, a := range q.Authors {
			phs[i] = s.ph(n)
			args = append(args, a)
			n++
			_ = i
		}
		where = append(where, "l.pubkey IN ("+strings.Join(phs, ",")+")")
	}
	if q.Since != nil {
		add("l.created_at >= "+s.ph(n), *q.Since)
	}
	if q.Until != nil {
		add("l.created_at <= "+s.ph(n), *q.Until)
	}
	if needGeoJoin {
		var ors []string
		for _, pfx := range q.GeoPrefixes {
			match := listing.GeoMatchPrefix(pfx, minPrefix)
			if len(match) < minPrefix {
				continue
			}
			ors = append(ors, "g.geohash LIKE "+s.ph(n))
			args = append(args, match+"%")
			n++
		}
		if len(ors) > 0 {
			where = append(where, "("+strings.Join(ors, " OR ")+")")
		} else {
			return nil, nil
		}
	}

	join := "FROM listings l"
	if needGeoJoin {
		join += " JOIN listing_geo g ON g.coord = l.coord"
	}
	cols := "l.coord, l.event_id, l.created_at"
	if sortByProximity {
		cols += ", g.lat, g.lon"
	}
	sqlStr := fmt.Sprintf(`SELECT %s %s WHERE %s ORDER BY l.created_at DESC, l.event_id DESC LIMIT %s`,
		cols, join, strings.Join(where, " AND "), s.ph(n))
	args = append(args, capN)

	rows, err := s.db.QueryContext(ctx, sqlStr, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	type rowT struct {
		coord, eventID string
		created        int64
		lat, lon       float64
		hasLL          bool
	}
	var rowsOut []rowT
	coordSet := map[string]annItem{}
	for rows.Next() {
		var r rowT
		if sortByProximity {
			var lat, lon sql.NullFloat64
			if err := rows.Scan(&r.coord, &r.eventID, &r.created, &lat, &lon); err != nil {
				return nil, err
			}
			if lat.Valid && lon.Valid {
				r.lat, r.lon, r.hasLL = lat.Float64, lon.Float64, true
			}
		} else if err := rows.Scan(&r.coord, &r.eventID, &r.created); err != nil {
			return nil, err
		}
		rowsOut = append(rowsOut, r)
		coordSet[r.coord] = annItem{Coord: r.coord, EventID: r.eventID, CreatedAt: r.created}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	if !wantSearch {
		if sortByProximity {
			if oLat, oLon, ok := listing.GeoOrigin(q.GeoPrefixes); ok {
				dist := func(r rowT) float64 {
					if !r.hasLL {
						return math.Inf(1)
					}
					return listing.HaversineKm(oLat, oLon, r.lat, r.lon)
				}
				sort.SliceStable(rowsOut, func(i, j int) bool {
					di, dj := dist(rowsOut[i]), dist(rowsOut[j])
					if di != dj {
						return di < dj
					}
					if rowsOut[i].created != rowsOut[j].created {
						return rowsOut[i].created > rowsOut[j].created
					}
					return rowsOut[i].eventID > rowsOut[j].eventID
				})
			}
		}
		ids := make([]string, 0, limit)
		for _, r := range rowsOut {
			ids = append(ids, r.eventID)
			if len(ids) >= limit {
				break
			}
		}
		return ids, nil
	}

	qvec, err := s.embedQuery(ctx, q.Search)
	if err != nil {
		return nil, err
	}
	items := s.ann.Snapshot(coordSet)
	// The SQL row is authoritative while an index update is between commit and
	// refreshing the in-memory vector. Never return an old event ID from ANN.
	current := items[:0]
	for _, it := range items {
		if row, ok := coordSet[it.Coord]; ok && row.EventID == it.EventID {
			current = append(current, it)
		}
	}
	items = current
	if len(items) == 0 {
		// no vectors yet; newest-first fallback
		ids := make([]string, 0, limit)
		for _, r := range rowsOut {
			ids = append(ids, r.eventID)
			if len(ids) >= limit {
				break
			}
		}
		return ids, nil
	}
	rankByCosine(qvec, items)
	ids := make([]string, 0, limit)
	seen := map[string]struct{}{}
	for _, it := range items {
		if _, ok := seen[it.EventID]; ok {
			continue
		}
		seen[it.EventID] = struct{}{}
		ids = append(ids, it.EventID)
		if len(ids) >= limit {
			break
		}
	}
	return ids, nil
}

func (s *sqlStore) embedQuery(ctx context.Context, text string) ([]float32, error) {
	if s.embedder == nil {
		return nil, fmt.Errorf("embedder unavailable")
	}
	key := s.embedder.ModelID() + "\n" + text
	if v, ok := s.qcache.get(key); ok {
		return v, nil
	}
	v, err := s.embedder.Embed(ctx, text)
	if err != nil {
		return nil, err
	}
	s.qcache.put(key, v)
	return v, nil
}
