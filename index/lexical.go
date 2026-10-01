package index

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"sort"
	"strings"
	"unicode"

	"github.com/michmich112/conduit-plugin/listing"
)

// LexicalCandidate is a text match from the active listing index. Score is
// normalized to [0, 1); Rank is stable within one retrieval result.
type LexicalCandidate struct {
	Coord     string
	EventID   string
	PubKey    string
	Title     string
	Body      string
	CreatedAt int64
	Score     float64
	Rank      int
}

// The SQLite FTS table uses listings as external content. Triggers keep it in
// step with replacements and deletes; the one-time rebuild indexes rows that
// predate this schema version.
func initSQLiteFTS(ctx context.Context, db *sql.DB) error {
	triggers := []string{
		`CREATE TRIGGER IF NOT EXISTS listings_fts_insert AFTER INSERT ON listings BEGIN
  INSERT INTO listings_fts(rowid, title, body) VALUES (new.rowid, new.title, new.body);
END`,
		`CREATE TRIGGER IF NOT EXISTS listings_fts_delete AFTER DELETE ON listings BEGIN
  INSERT INTO listings_fts(listings_fts, rowid, title, body) VALUES ('delete', old.rowid, old.title, old.body);
END`,
		`CREATE TRIGGER IF NOT EXISTS listings_fts_update AFTER UPDATE ON listings BEGIN
  INSERT INTO listings_fts(listings_fts, rowid, title, body) VALUES ('delete', old.rowid, old.title, old.body);
  INSERT INTO listings_fts(rowid, title, body) VALUES (new.rowid, new.title, new.body);
END`,
	}
	for _, trigger := range triggers {
		if _, err := db.ExecContext(ctx, trigger); err != nil {
			return fmt.Errorf("create listing FTS trigger: %w", err)
		}
	}
	var version string
	err := db.QueryRowContext(ctx, `SELECT value FROM index_meta WHERE key = 'listings_fts_version'`).Scan(&version)
	if err != nil && err != sql.ErrNoRows {
		return err
	}
	if version == "1" {
		return nil
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `INSERT INTO listings_fts(listings_fts) VALUES ('rebuild')`); err != nil {
		return fmt.Errorf("rebuild listing FTS: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO index_meta(key, value) VALUES ('listings_fts_version', '1')
ON CONFLICT(key) DO UPDATE SET value = excluded.value`); err != nil {
		return err
	}
	return tx.Commit()
}

// lexicalTerms only passes literal Unicode words to either backend's query
// parser. This keeps user query syntax from becoming FTS operators.
func lexicalTerms(text string) []string {
	var terms []string
	var word []rune
	flush := func() {
		if len(word) == 0 || len(terms) >= 16 {
			word = word[:0]
			return
		}
		terms = append(terms, string(word))
		word = word[:0]
	}
	for _, r := range strings.ToLower(text) {
		if unicode.IsLetter(r) || unicode.IsNumber(r) {
			if len(word) < 64 {
				word = append(word, r)
			}
		} else {
			flush()
		}
	}
	flush()
	return terms
}

// lexicalCandidates retrieves query-matching active listings independently
// of vector availability. It keeps the strongest global hits and probes for
// other authors only when that pool has low author diversity. The probe bound
// limits repeated FTS work under the intercept deadline; global hits still
// fill the pool when few authors match.
func (s *sqlStore) lexicalCandidates(ctx context.Context, q Query, limit int) ([]LexicalCandidate, error) {
	terms := lexicalTerms(q.Search)
	if len(terms) == 0 {
		return nil, nil
	}
	if limit <= 0 {
		limit = 100
	}
	if limit > 5000 {
		limit = 5000
	}
	minPrefix := q.GeoMinPrefixLen
	if minPrefix <= 0 {
		minPrefix = 2
	}

	where := []string{"l.status = " + s.ph(2)}
	if s.backend != "postgres" {
		// SQLite binds anonymous placeholders by their textual order.
		where = append([]string{"listings_fts MATCH " + s.ph(1)}, where...)
	}
	args := []any{}
	n := 3
	if s.backend == "postgres" {
		quoted := make([]string, len(terms))
		for i, term := range terms {
			quoted[i] = "'" + term + "'"
		}
		args = append(args, strings.Join(quoted, " | "), listing.StatusActive)
	} else {
		quoted := make([]string, len(terms))
		for i, term := range terms {
			quoted[i] = `"` + term + `"`
		}
		args = append(args, strings.Join(quoted, " OR "), listing.StatusActive)
	}
	if len(q.Kinds) > 0 {
		phs := make([]string, len(q.Kinds))
		for i, kind := range q.Kinds {
			phs[i] = s.ph(n)
			args = append(args, kind)
			n++
		}
		where = append(where, "l.kind IN ("+strings.Join(phs, ",")+")")
	}
	if len(q.Authors) > 0 {
		phs := make([]string, len(q.Authors))
		for i, author := range q.Authors {
			phs[i] = s.ph(n)
			args = append(args, author)
			n++
		}
		where = append(where, "l.pubkey IN ("+strings.Join(phs, ",")+")")
	}
	if q.Since != nil {
		where = append(where, "l.created_at >= "+s.ph(n))
		args = append(args, *q.Since)
		n++
	}
	if q.Until != nil {
		where = append(where, "l.created_at <= "+s.ph(n))
		args = append(args, *q.Until)
		n++
	}
	join := ""
	if q.GeoEnabled && len(q.GeoPrefixes) > 0 {
		join = " JOIN listing_geo g ON g.coord = l.coord"
		ors := make([]string, 0, len(q.GeoPrefixes))
		for _, prefix := range q.GeoPrefixes {
			match := listing.GeoMatchPrefix(prefix, minPrefix)
			if len(match) < minPrefix {
				continue
			}
			ors = append(ors, "g.geohash LIKE "+s.ph(n))
			args = append(args, match+"%")
			n++
		}
		if len(ors) == 0 {
			return nil, nil
		}
		where = append(where, "("+strings.Join(ors, " OR ")+")")
	}

	var baseStatement, orderBy string
	if s.backend == "postgres" {
		const document = `(setweight(to_tsvector('simple', COALESCE(l.title, '')), 'A') || setweight(to_tsvector('simple', COALESCE(l.body, '')), 'B'))`
		query := "to_tsquery('simple', " + s.ph(1) + ")"
		where = append(where, document+" @@ "+query)
		baseStatement = fmt.Sprintf(`SELECT l.coord, l.event_id, l.pubkey, l.created_at,
ts_rank_cd(%s, %s) AS lexical_score FROM listings l%s WHERE %s`,
			document, query, join, strings.Join(where, " AND "))
		orderBy = ` ORDER BY lexical_score DESC, l.created_at DESC, l.event_id DESC`
	} else {
		baseStatement = fmt.Sprintf(`SELECT l.coord, l.event_id, l.pubkey, l.created_at,
bm25(listings_fts, 4.0, 1.0) AS lexical_score FROM listings_fts
JOIN listings l ON l.rowid = listings_fts.rowid%s WHERE %s`,
			join, strings.Join(where, " AND "))
		orderBy = ` ORDER BY lexical_score ASC, l.created_at DESC, l.event_id DESC`
	}
	queryRows := func(extra string, extraArgs []any, queryLimit, rankStart int) ([]LexicalCandidate, error) {
		statement := baseStatement + extra + orderBy + " LIMIT " + s.ph(n+len(extraArgs))
		queryArgs := make([]any, 0, len(args)+len(extraArgs)+1)
		queryArgs = append(queryArgs, args...)
		queryArgs = append(queryArgs, extraArgs...)
		queryArgs = append(queryArgs, queryLimit)
		rows, err := s.db.QueryContext(ctx, statement, queryArgs...)
		if err != nil {
			return nil, err
		}
		var result []LexicalCandidate
		for rows.Next() {
			var candidate LexicalCandidate
			var raw float64
			if err := rows.Scan(&candidate.Coord, &candidate.EventID, &candidate.PubKey,
				&candidate.CreatedAt, &raw); err != nil {
				_ = rows.Close()
				return nil, err
			}
			if s.backend == "postgres" {
				candidate.Score = raw / (1 + raw)
			} else {
				candidate.Score = math.Abs(raw) / (1 + math.Abs(raw))
			}
			candidate.Rank = rankStart + len(result) + 1
			result = append(result, candidate)
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return nil, err
		}
		return result, rows.Close()
	}

	out, err := queryRows("", nil, limit, 0)
	if err != nil || len(out) == 0 {
		return out, err
	}
	const maxAuthorRepresentatives = 8
	repLimit := limit
	if repLimit > maxAuthorRepresentatives {
		repLimit = maxAuthorRepresentatives
	}
	seenAuthors := make(map[string]struct{}, len(out))
	for _, candidate := range out {
		seenAuthors[candidate.PubKey] = struct{}{}
	}
	for len(seenAuthors) < repLimit {
		authors := make([]string, 0, len(seenAuthors))
		for author := range seenAuthors {
			authors = append(authors, author)
		}
		sort.Strings(authors)
		phs := make([]string, len(authors))
		extraArgs := make([]any, len(authors))
		for i, author := range authors {
			phs[i], extraArgs[i] = s.ph(n+i), author
		}
		probe, err := queryRows(" AND l.pubkey NOT IN ("+strings.Join(phs, ",")+")", extraArgs, 1, len(out))
		if err != nil {
			return nil, err
		}
		if len(probe) == 0 {
			break
		}
		seenAuthors[probe[0].PubKey] = struct{}{}
		out = append(out, probe[0])
	}
	if err := s.fillLexicalText(ctx, out); err != nil {
		return nil, err
	}
	return out, nil
}

// Fetch text for the selected pool only. A common query can match thousands
// of rows, while reranking needs the bodies of at most 2*limit candidates.
func (s *sqlStore) fillLexicalText(ctx context.Context, candidates []LexicalCandidate) error {
	for start := 0; start < len(candidates); start += 400 {
		end := start + 400
		if end > len(candidates) {
			end = len(candidates)
		}
		placeholders := make([]string, end-start)
		args := make([]any, end-start)
		positions := make(map[string]int, end-start)
		for i := start; i < end; i++ {
			placeholders[i-start] = s.ph(i - start + 1)
			args[i-start] = candidates[i].Coord
			positions[candidates[i].Coord] = i
		}
		statement := `SELECT coord, COALESCE(title,''), COALESCE(body,'') FROM listings WHERE coord IN (` + strings.Join(placeholders, ",") + `)`
		rows, err := s.db.QueryContext(ctx, statement, args...)
		if err != nil {
			return err
		}
		for rows.Next() {
			var coord, title, body string
			if err := rows.Scan(&coord, &title, &body); err != nil {
				_ = rows.Close()
				return err
			}
			if i, ok := positions[coord]; ok {
				candidates[i].Title, candidates[i].Body = title, body
			}
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return err
		}
		if err := rows.Close(); err != nil {
			return err
		}
	}
	return nil
}
