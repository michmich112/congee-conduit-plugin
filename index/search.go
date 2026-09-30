package index

import (
	"container/heap"
	"context"
	"database/sql"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/michmich112/conduit-plugin/embed"
	"github.com/michmich112/conduit-plugin/listing"
)

func searchSQL(ctx context.Context, s *sqlStore, q Query) ([]string, error) {
	if strings.TrimSpace(q.Search) != "" {
		q.Search = normalizedSearch(q.Search)
		if q.Search == "" {
			return nil, nil
		}
	}
	limit := clampLimit(q.Limit, 0)
	capN := q.SearchCandidateCap
	if capN <= 0 {
		capN = 1000
	}
	minPrefix := q.GeoMinPrefixLen
	if minPrefix <= 0 {
		minPrefix = 2
	}
	wantSearch := strings.TrimSpace(q.Search) != ""
	needGeoJoin := q.GeoEnabled && len(q.GeoPrefixes) > 0
	sortByProximity := needGeoJoin && !wantSearch
	if wantSearch && !needGeoJoin {
		return s.searchCached(ctx, q, limit, capN)
	}

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
	if wantSearch {
		// Fetch eligibility across the complete index; candidate limits are applied
		// after query-relevant semantic and lexical retrieval.
		where = append(where, "l.status = 'active'")
		cols += ", l.pubkey, COALESCE(l.title,''), COALESCE(l.body,'')"
	}
	sqlStr := fmt.Sprintf(`SELECT %s %s WHERE %s`,
		cols, join, strings.Join(where, " AND "))
	if !wantSearch {
		sqlStr += " ORDER BY l.created_at DESC, l.event_id DESC LIMIT " + s.ph(n)
		args = append(args, capN)
	}

	rows, err := s.db.QueryContext(ctx, sqlStr, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	type rowT struct {
		coord, eventID      string
		created             int64
		lat, lon            float64
		hasLL               bool
		pubkey, title, body string
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
		} else if wantSearch {
			if err := rows.Scan(&r.coord, &r.eventID, &r.created, &r.pubkey, &r.title, &r.body); err != nil {
				return nil, err
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
	if err := rows.Close(); err != nil {
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

	// The vector and lexical branches independently retrieve from the complete
	// eligible set. A vector cache entry is usable only for its current event ID.
	eligible := make(map[string]searchCandidate, len(rowsOut))
	for _, r := range rowsOut {
		eligible[r.coord] = searchCandidate{
			Coord: r.coord, EventID: r.eventID, PubKey: r.pubkey,
			Title: r.title, Body: r.body, CreatedAt: r.created,
		}
	}
	return s.searchRanked(ctx, q, limit, capN, eligible, coordSet, true)

}

// NIP-50 key:value extensions that this index does not implement should not
// become product words or query-embedding input. Preserve ordinary free text.
func normalizedSearch(search string) string {
	words := strings.Fields(search)
	kept := words[:0]
	for _, word := range words {
		key, value, hasColon := strings.Cut(word, ":")
		if hasColon && value != "" && !strings.HasPrefix(value, "//") && extensionKey(key) {
			continue
		}
		kept = append(kept, word)
	}
	return strings.Join(kept, " ")
}

func extensionKey(key string) bool {
	if key == "" || !unicode.IsLetter(rune(key[0])) {
		return false
	}
	for _, r := range key {
		if !unicode.IsLetter(r) && !unicode.IsNumber(r) && r != '_' && r != '-' {
			return false
		}
	}
	return true
}

type searchCandidate struct {
	Coord, EventID, PubKey, Title, Body string
	CreatedAt                           int64
	Semantic, Lexical                   float64
	HasVector, HasLexical               bool
	Vec                                 []float32
	Score                               float64
}

func (s *sqlStore) searchCached(ctx context.Context, q Query, limit, capN int) ([]string, error) {
	eligible := make(map[string]searchCandidate)
	if q.VectorEnabled && s.embedder != nil {
		for _, item := range s.ann.Snapshot(nil) {
			kind, author, ok := coordinateIdentity(item.Coord)
			if !ok || !allowsKind(q.Kinds, kind) || !allowsAuthor(q.Authors, author) ||
				(q.Since != nil && item.CreatedAt < *q.Since) ||
				(q.Until != nil && item.CreatedAt > *q.Until) {
				continue
			}
			eligible[item.Coord] = searchCandidate{
				Coord: item.Coord, EventID: item.EventID,
				PubKey: author, CreatedAt: item.CreatedAt,
			}
		}
	}
	return s.searchRanked(ctx, q, limit, capN, eligible, nil, false)
}

func coordinateIdentity(coord string) (int, string, bool) {
	kindText, rest, ok := strings.Cut(coord, ":")
	if !ok {
		return 0, "", false
	}
	author, _, ok := strings.Cut(rest, ":")
	if !ok {
		return 0, "", false
	}
	kind, err := strconv.Atoi(kindText)
	return kind, author, err == nil
}

func allowsKind(allowed []int, actual int) bool {
	if len(allowed) == 0 {
		return true
	}
	for _, value := range allowed {
		if value == actual {
			return true
		}
	}
	return false
}

func allowsAuthor(allowed []string, actual string) bool {
	if len(allowed) == 0 {
		return true
	}
	for _, value := range allowed {
		if value == actual {
			return true
		}
	}
	return false
}

func (s *sqlStore) searchRanked(ctx context.Context, q Query, limit, capN int, eligible map[string]searchCandidate, coords map[string]annItem, strictEligibility bool) ([]string, error) {
	pool := make(map[string]searchCandidate)
	var qvec []float32
	if q.VectorEnabled && s.embedder != nil {
		// Leave part of the intercept deadline for lexical retrieval if embedding
		// is slow or unavailable.
		embedCtx, cancel := context.WithTimeout(ctx, 120*time.Millisecond)
		v, err := s.embedQuery(embedCtx, q.Search)
		cancel()
		if err == nil {
			qvec = v
		}
	}
	if len(qvec) > 0 && len(eligible) > 0 {
		global := &candidateMinHeap{}
		heap.Init(global)
		perAuthor := make(map[string]searchCandidate)
		for _, item := range s.ann.Snapshot(coords) {
			base, ok := eligible[item.Coord]
			if !ok || base.EventID != item.EventID || len(item.Vec) == 0 {
				continue
			}
			base.Semantic = math.Max(0, float64(embed.Cosine(qvec, item.Vec)))
			// A low positive cosine is not enough evidence to call a product a
			// match. Lexical retrieval can still admit it independently.
			if base.Semantic < 0.25 {
				continue
			}
			base.HasVector = true
			base.Vec = item.Vec
			offerCandidate(global, base, capN)
			best, exists := perAuthor[base.PubKey]
			if !exists || betterSemantic(base, best) {
				perAuthor[base.PubKey] = base
			}
		}
		// Select the strongest author representatives independently of the
		// global top set, which may be dominated by one publisher.
		authors := &candidateMinHeap{}
		heap.Init(authors)
		authorBudget := capN / 3
		if authorBudget < 1 {
			authorBudget = 1
		}
		for _, best := range perAuthor {
			offerCandidate(authors, best, authorBudget)
		}
		for _, candidate := range *authors {
			pool[candidate.Coord] = candidate
		}
		globalRanked := []searchCandidate(*global)
		sort.Slice(globalRanked, func(i, j int) bool { return betterSemantic(globalRanked[i], globalRanked[j]) })
		for _, candidate := range globalRanked {
			if len(pool) >= capN {
				break
			}
			pool[candidate.Coord] = candidate
		}
	}

	lexical, err := s.lexicalCandidates(ctx, q, capN)
	if err != nil {
		if len(pool) == 0 {
			return nil, err
		}
		lexical = nil
	}
	maxLexical := 0.0
	for _, hit := range lexical {
		if hit.Score > maxLexical {
			maxLexical = hit.Score
		}
	}
	for _, hit := range lexical {
		base, ok := eligible[hit.Coord]
		if strictEligibility && (!ok || base.EventID != hit.EventID) {
			continue
		}
		if !ok || base.EventID != hit.EventID {
			base = searchCandidate{Coord: hit.Coord, EventID: hit.EventID, PubKey: hit.PubKey, CreatedAt: hit.CreatedAt}
		}
		if existing, ok := pool[hit.Coord]; ok && existing.EventID == hit.EventID {
			base = existing
		}
		base.Title, base.Body = hit.Title, hit.Body
		base.HasLexical = true
		if maxLexical > 0 {
			base.Lexical = math.Max(0, hit.Score/maxLexical)
		} else {
			base.Lexical = 1 / (1 + 0.04*float64(hit.Rank))
		}
		pool[hit.Coord] = base
	}
	if len(pool) == 0 {
		s.searchLexicalFallback.Add(1)
		return nil, nil
	}
	if !strictEligibility {
		if err := s.hydrateCurrentCandidates(ctx, pool); err != nil {
			return nil, err
		}
	}
	items := make([]searchCandidate, 0, len(pool))
	hasCurrentVector := false
	now := nowUnix()
	var ranks map[string]int
	if q.NIP85Provider != "" {
		targets := make([]string, 0, len(pool))
		seen := make(map[string]bool, len(pool))
		for _, item := range pool {
			if item.PubKey != "" && !seen[item.PubKey] {
				seen[item.PubKey] = true
				targets = append(targets, item.PubKey)
			}
		}
		maxAge := q.NIP85MaxAgeDays
		if maxAge <= 0 {
			maxAge = 14
		}
		var err error
		ranks, err = s.userRankScores(ctx, q.NIP85Provider, targets, now-int64(maxAge)*24*60*60)
		if err != nil {
			s.nip85ReadErrors.Add(1)
		}
	}
	for _, item := range pool {
		hasCurrentVector = hasCurrentVector || item.HasVector
		// Age can reorder comparable matches, but it cannot make an unrelated
		// listing a match. This is a 75-day half-life, capped at five percent.
		age := math.Max(0, float64(now-item.CreatedAt))
		freshness := 0.05 * math.Exp2(-age/(75*24*60*60))
		switch {
		case len(qvec) == 0:
			item.Score = item.Lexical + freshness
		case item.HasVector:
			item.Score = 0.80*item.Semantic + 0.15*item.Lexical + freshness
		default:
			// Missing embeddings must not hide real lexical matches.
			item.Score = 0.65*item.Lexical + freshness
		}
		if rank, ok := ranks[item.PubKey]; ok {
			// A missing assertion is neutral. Published ranks move the score
			// by at most 0.025 in either direction, after relevance retrieval.
			item.Score += 0.025 * (float64(rank) - 50) / 50
		}
		items = append(items, item)
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].Score != items[j].Score {
			return items[i].Score > items[j].Score
		}
		return candidateTie(items[i], items[j])
	})
	if hasCurrentVector {
		s.searchSemantic.Add(1)
	} else {
		s.searchLexicalFallback.Add(1)
	}
	return diversifiedIDs(items, limit), nil
}

// Validate cached candidates against the current active row before returning
// IDs. This also supplies title/body for duplicate handling without scanning
// every listing in SQL on the common non-geo path.
func (s *sqlStore) hydrateCurrentCandidates(ctx context.Context, pool map[string]searchCandidate) error {
	coords := make([]string, 0, len(pool))
	seen := make(map[string]struct{}, len(pool))
	for coord := range pool {
		coords = append(coords, coord)
	}
	for start := 0; start < len(coords); start += 400 {
		end := start + 400
		if end > len(coords) {
			end = len(coords)
		}
		batch := coords[start:end]
		args := make([]any, len(batch))
		phs := make([]string, len(batch))
		for i, coord := range batch {
			args[i], phs[i] = coord, s.ph(i+1)
		}
		rows, err := s.db.QueryContext(ctx, `SELECT coord, event_id, pubkey, created_at, COALESCE(title,''), COALESCE(body,'')
FROM listings WHERE status = 'active' AND coord IN (`+strings.Join(phs, ",")+`)`, args...)
		if err != nil {
			return err
		}
		for rows.Next() {
			var coord, eventID, pubkey, title, body string
			var created int64
			if err := rows.Scan(&coord, &eventID, &pubkey, &created, &title, &body); err != nil {
				_ = rows.Close()
				return err
			}
			candidate := pool[coord]
			if candidate.EventID != eventID {
				continue
			}
			seen[coord] = struct{}{}
			candidate.PubKey, candidate.Title, candidate.Body, candidate.CreatedAt = pubkey, title, body, created
			pool[coord] = candidate
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return err
		}
		if err := rows.Close(); err != nil {
			return err
		}
	}
	// A deleted, inactive, or superseded coordinate has no matching row.
	for coord := range pool {
		if _, ok := seen[coord]; !ok {
			delete(pool, coord)
		}
	}
	return nil
}

type candidateMinHeap []searchCandidate

func (h candidateMinHeap) Len() int           { return len(h) }
func (h candidateMinHeap) Less(i, j int) bool { return betterSemantic(h[j], h[i]) }
func (h candidateMinHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *candidateMinHeap) Push(x any)        { *h = append(*h, x.(searchCandidate)) }
func (h *candidateMinHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

func betterSemantic(a, b searchCandidate) bool {
	if a.Semantic != b.Semantic {
		return a.Semantic > b.Semantic
	}
	return candidateTie(a, b)
}

func offerCandidate(h *candidateMinHeap, item searchCandidate, limit int) {
	if limit <= 0 {
		return
	}
	if h.Len() < limit {
		heap.Push(h, item)
	} else if betterSemantic(item, (*h)[0]) {
		(*h)[0] = item
		heap.Fix(h, 0)
	}
}

func candidateTie(a, b searchCandidate) bool {
	if a.CreatedAt != b.CreatedAt {
		return a.CreatedAt > b.CreatedAt
	}
	if a.EventID != b.EventID {
		return a.EventID > b.EventID
	}
	return a.Coord > b.Coord
}

func diversifiedIDs(items []searchCandidate, limit int) []string {
	ids := make([]string, 0, limit)
	used := make([]bool, len(items))
	authorCount := make(map[string]int)
	authorSelections := make(map[string][]int)
	content := make([]string, len(items))
	wordSets := make([]map[string]struct{}, len(items))
	for i := range items {
		content[i] = normalizedContent(items[i].Title, items[i].Body)
	}
	wordsAt := func(i int) map[string]struct{} {
		if wordSets[i] == nil {
			wordSets[i] = make(map[string]struct{})
			for _, word := range strings.Fields(content[i]) {
				wordSets[i][word] = struct{}{}
			}
		}
		return wordSets[i]
	}
	isDuplicate := func(i int) bool {
		if content[i] == "" {
			return false
		}
		for _, prior := range authorSelections[items[i].PubKey] {
			if content[i] == content[prior] {
				return true
			}
			// Require substantial shared wording; short listings and products
			// with materially different descriptions remain distinct.
			a, b := wordsAt(i), wordsAt(prior)
			if len(a) < 8 || len(b) < 8 {
				continue
			}
			common := 0
			for word := range a {
				if _, ok := b[word]; ok {
					common++
				}
			}
			if float64(common)/float64(len(a)+len(b)-common) >= 0.92 {
				return true
			}
		}
		return false
	}
	for len(ids) < limit {
		best := -1
		bestAdjusted := math.Inf(-1)
		for i, item := range items {
			if used[i] || isDuplicate(i) {
				continue
			}
			// A bounded exposure penalty promotes another merchant only when its
			// relevance is reasonably close; a sole merchant can fill the page.
			penalty := math.Min(0.15, float64(authorCount[item.PubKey])*0.05)
			adjusted := item.Score - penalty
			if adjusted > bestAdjusted || (adjusted == bestAdjusted && (best < 0 || candidateTie(item, items[best]))) {
				best, bestAdjusted = i, adjusted
			}
		}
		if best < 0 {
			break
		}
		item := items[best]
		used[best] = true
		ids = append(ids, item.EventID)
		authorCount[item.PubKey]++
		authorSelections[item.PubKey] = append(authorSelections[item.PubKey], best)
	}
	return ids
}

func normalizedContent(title, body string) string {
	return strings.Join(strings.FieldsFunc(strings.ToLower(title+" "+body), func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsNumber(r)
	}), " ")
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
