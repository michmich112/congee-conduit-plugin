package index

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/michmich112/conduit-plugin/listing"
	"github.com/michmich112/conduit-plugin/nip85"
)

// UpsertUserRank keeps the latest signed assertion for one provider and
// subject. Congee stores only the current addressable event, but plugin
// notifications and backfill may arrive out of order.
func (s *sqlStore) UpsertUserRank(ctx context.Context, rank nip85.UserRank) error {
	return s.runWrite(func() error {
		_, err := s.db.ExecContext(ctx, `INSERT INTO nip85_user_ranks
(provider, target, event_id, created_at, rank) VALUES (`+placeholders(s, 5)+`)
ON CONFLICT(provider, target) DO UPDATE SET
 event_id=excluded.event_id, created_at=excluded.created_at, rank=excluded.rank
WHERE excluded.created_at > nip85_user_ranks.created_at
 OR (excluded.created_at = nip85_user_ranks.created_at AND excluded.event_id > nip85_user_ranks.event_id)`,
			rank.Provider, rank.Target, rank.EventID, rank.CreatedAt, rank.Rank)
		return err
	})
}

func (s *sqlStore) MerchantPubkeys(ctx context.Context) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT DISTINCT pubkey FROM listings WHERE status = `+s.ph(1)+` ORDER BY pubkey`, listing.StatusActive)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var keys []string
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			return nil, err
		}
		keys = append(keys, key)
	}
	return keys, rows.Err()
}

func (s *sqlStore) HasActiveMerchant(ctx context.Context, pubkey string) (bool, error) {
	var found int
	err := s.db.QueryRowContext(ctx, `SELECT 1 FROM listings WHERE pubkey = `+s.ph(1)+` AND status = `+s.ph(2)+` LIMIT 1`, pubkey, listing.StatusActive).Scan(&found)
	if err == nil {
		return true, nil
	}
	if err == sql.ErrNoRows {
		return false, nil
	}
	return false, err
}

func (s *sqlStore) userRankScores(ctx context.Context, provider string, targets []string, cutoff int64) (map[string]int, error) {
	scores := make(map[string]int)
	if provider == "" || len(targets) == 0 {
		return scores, nil
	}
	for start := 0; start < len(targets); start += 300 {
		end := start + 300
		if end > len(targets) {
			end = len(targets)
		}
		phs := make([]string, end-start)
		args := make([]any, 0, end-start+2)
		args = append(args, provider, cutoff)
		for i, target := range targets[start:end] {
			phs[i] = s.ph(i + 3)
			args = append(args, target)
		}
		query := fmt.Sprintf(`SELECT target, rank FROM nip85_user_ranks
WHERE provider = %s AND created_at >= %s AND target IN (%s)`, s.ph(1), s.ph(2), strings.Join(phs, ","))
		rows, err := s.db.QueryContext(ctx, query, args...)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var target string
			var rank int
			if err := rows.Scan(&target, &rank); err != nil {
				_ = rows.Close()
				return nil, err
			}
			scores[target] = rank
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return nil, err
		}
		if err := rows.Close(); err != nil {
			return nil, err
		}
	}
	return scores, nil
}
