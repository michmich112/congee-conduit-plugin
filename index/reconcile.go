package index

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/michmich112/conduit-plugin/listing"
	"time"
)

var ErrMoreWork = errors.New("reconciliation has more bounded work")

var ErrSuperseded = errors.New("reconciliation work superseded")

type ReconcileJob struct {
	Key, Coord, Author, Cursor, Phase, LastError string
	Kind, Attempts                               int
	Version, NextAttempt, PendingSince           int64
}

// ReconcileStore keeps retries and incomplete merchant scans in the index database.
type ReconcileStore interface {
	Invalidate(context.Context, ReconcileJob, bool) error
	Jobs(context.Context, int64, int) ([]ReconcileJob, error)
	TargetsAfter(context.Context, string, int) ([]string, error)
	Stage(context.Context, ReconcileJob, []listing.Listing, string, string) error
	Staged(context.Context, ReconcileJob, int) ([]listing.Listing, error)
	ApplyJob(context.Context, ReconcileJob, *listing.Listing) error
	FinishJob(context.Context, ReconcileJob) error
	RetryJob(context.Context, ReconcileJob, error) error
	JobStats(context.Context) (map[string]any, error)
}

// TargetsAfter groups legacy merchant coordinates before scheduling work, so
// one merchant is not rescanned once for each persisted product.
func (s *sqlStore) TargetsAfter(ctx context.Context, after string, limit int) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT DISTINCT CASE WHEN kind IN (30017,30018) THEN CAST(kind AS TEXT) || ':' || pubkey || ':*' ELSE coord END AS target FROM listings WHERE (CASE WHEN kind IN (30017,30018) THEN CAST(kind AS TEXT) || ':' || pubkey || ':*' ELSE coord END) > `+s.ph(1)+` ORDER BY target LIMIT `+s.ph(2), after, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var c string
		if err := rows.Scan(&c); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

func (s *sqlStore) Invalidate(ctx context.Context, j ReconcileJob, force bool) error {
	return s.runWrite(func() error {
		now := time.Now().UnixMilli()
		suffix := "DO NOTHING"
		if force {
			suffix = "DO UPDATE SET version=reconcile_jobs.version+1, attempts=0, next_attempt=excluded.next_attempt, last_error='', cursor='', phase='scan'"
		}
		_, err := s.db.ExecContext(ctx, `INSERT INTO reconcile_jobs(job_key,coord,kind,author,version,attempts,next_attempt,pending_since,last_error,cursor,phase) VALUES (`+placeholders(s, 11)+`) ON CONFLICT(job_key) `+suffix, j.Key, j.Coord, j.Kind, j.Author, 1, 0, now, now, "", "", "scan")
		return err
	})
}

func (s *sqlStore) Jobs(ctx context.Context, now int64, limit int) ([]ReconcileJob, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT job_key,coord,kind,author,version,attempts,next_attempt,pending_since,last_error,cursor,phase FROM reconcile_jobs WHERE next_attempt <= `+s.ph(1)+` ORDER BY next_attempt,pending_since,job_key LIMIT `+s.ph(2), now, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ReconcileJob
	for rows.Next() {
		var j ReconcileJob
		if err := rows.Scan(&j.Key, &j.Coord, &j.Kind, &j.Author, &j.Version, &j.Attempts, &j.NextAttempt, &j.PendingSince, &j.LastError, &j.Cursor, &j.Phase); err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return out, rows.Err()
}

func (s *sqlStore) checkJob(ctx context.Context, tx *sql.Tx, j ReconcileJob) error {
	q := `SELECT version FROM reconcile_jobs WHERE job_key = ` + s.ph(1)
	if s.backend == "postgres" {
		q += " FOR UPDATE"
	}
	var v int64
	if err := tx.QueryRowContext(ctx, q, j.Key).Scan(&v); err != nil {
		if err == sql.ErrNoRows {
			return ErrSuperseded
		}
		return err
	}
	if v != j.Version {
		return ErrSuperseded
	}
	return nil
}

func (s *sqlStore) Stage(ctx context.Context, j ReconcileJob, ls []listing.Listing, cursor, phase string) error {
	return s.runWrite(func() error {
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		if err := s.checkJob(ctx, tx, j); err != nil {
			return err
		}
		if _, err := s.deleteStagedBatch(ctx, tx, j.Key, &j.Version); err != nil {
			return err
		}
		for _, l := range ls {
			var b string
			var old listing.Listing
			err := tx.QueryRowContext(ctx, `SELECT listing FROM reconcile_stage WHERE job_key = `+s.ph(1)+` AND version = `+s.ph(2)+` AND coord = `+s.ph(3), j.Key, j.Version, l.Coord).Scan(&b)
			if err != nil && err != sql.ErrNoRows {
				return err
			}
			if err == nil {
				if err := json.Unmarshal([]byte(b), &old); err != nil {
					return err
				}
				if old.CreatedAt > l.CreatedAt || old.CreatedAt == l.CreatedAt && old.EventID < l.EventID {
					continue
				}
			}
			bts, err := json.Marshal(l)
			if err != nil {
				return err
			}
			if _, err := tx.ExecContext(ctx, `INSERT INTO reconcile_stage(job_key,version,coord,listing) VALUES (`+placeholders(s, 4)+`) ON CONFLICT(job_key,version,coord) DO UPDATE SET listing=excluded.listing`, j.Key, j.Version, l.Coord, string(bts)); err != nil {
				return err
			}
		}
		_, err = tx.ExecContext(ctx, `UPDATE reconcile_jobs SET attempts=0,last_error='',cursor = `+s.ph(1)+`, phase = `+s.ph(2)+`, next_attempt = `+s.ph(3)+` WHERE job_key = `+s.ph(4), cursor, phase, time.Now().UnixMilli(), j.Key)
		if err != nil {
			return err
		}
		return tx.Commit()
	})
}

func (s *sqlStore) Staged(ctx context.Context, j ReconcileJob, limit int) ([]listing.Listing, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT listing FROM reconcile_stage WHERE job_key = `+s.ph(1)+` AND version = `+s.ph(2)+` AND coord > `+s.ph(3)+` ORDER BY coord LIMIT `+s.ph(4), j.Key, j.Version, j.Cursor, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []listing.Listing
	for rows.Next() {
		var b string
		var l listing.Listing
		if err := rows.Scan(&b); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(b), &l); err != nil {
			return nil, err
		}
		out = append(out, l)
	}
	return out, rows.Err()
}

func (s *sqlStore) ApplyJob(ctx context.Context, j ReconcileJob, l *listing.Listing) error {
	if l != nil {
		return s.upsertJob(ctx, *l, false, &j)
	}
	lock := s.coordLock(j.Coord)
	lock.Lock()
	defer lock.Unlock()
	return s.runWrite(func() error {
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		if err := s.checkJob(ctx, tx, j); err != nil {
			return err
		}
		for _, table := range []string{"listing_embeddings", "listing_geo", "listings"} {
			if _, err := tx.ExecContext(ctx, `DELETE FROM `+table+` WHERE coord = `+s.ph(1), j.Coord); err != nil {
				return err
			}
		}
		if err := tx.Commit(); err != nil {
			return err
		}
		s.bumpEpoch(j.Coord)
		s.ann.Delete(j.Coord)
		return nil
	})
}

func (s *sqlStore) FinishJob(ctx context.Context, j ReconcileJob) error {
	return s.runWrite(func() error {
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		if err := s.checkJob(ctx, tx, j); err != nil {
			return err
		}
		// A complete legacy scan can remove missing coordinates. Partial scans never reach this path.
		var removed []string
		if j.Coord == "" && j.Phase != "cleanup" {
			rows, err := tx.QueryContext(ctx, `SELECT coord FROM listings WHERE kind = `+s.ph(1)+` AND pubkey = `+s.ph(2)+` AND coord NOT IN (SELECT coord FROM reconcile_stage WHERE job_key = `+s.ph(3)+` AND version = `+s.ph(4)+`) ORDER BY coord LIMIT 100`, j.Kind, j.Author, j.Key, j.Version)
			if err != nil {
				return err
			}
			for rows.Next() {
				var c string
				if err := rows.Scan(&c); err != nil {
					rows.Close()
					return err
				}
				removed = append(removed, c)
			}
			err = rows.Err()
			rows.Close()
			if err != nil {
				return err
			}
			for _, coord := range removed {
				for _, table := range []string{"listing_embeddings", "listing_geo", "listings"} {
					if _, err := tx.ExecContext(ctx, `DELETE FROM `+table+` WHERE coord = `+s.ph(1), coord); err != nil {
						return err
					}
				}
			}
		}
		if len(removed) == 100 {
			if err := tx.Commit(); err != nil {
				return err
			}
			for _, c := range removed {
				s.bumpEpoch(c)
				s.ann.Delete(c)
			}
			return ErrMoreWork
		}
		if j.Coord == "" {
			n, err := s.deleteStagedBatch(ctx, tx, j.Key, nil)
			if err != nil {
				return err
			}
			if n == 200 {
				if _, err := tx.ExecContext(ctx, `UPDATE reconcile_jobs SET phase='cleanup',next_attempt=`+s.ph(1)+` WHERE job_key=`+s.ph(2), time.Now().UnixMilli(), j.Key); err != nil {
					return err
				}
				if err := tx.Commit(); err != nil {
					return err
				}
				for _, c := range removed {
					s.bumpEpoch(c)
					s.ann.Delete(c)
				}
				return ErrMoreWork
			}
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM reconcile_jobs WHERE job_key = `+s.ph(1), j.Key); err != nil {
			return err
		}

		if err := tx.Commit(); err != nil {
			return err
		}
		for _, coord := range removed {
			s.bumpEpoch(coord)
			s.ann.Delete(coord)
		}
		return nil
	})
}

// Cleanup also has a fixed transaction bound; large merchant snapshots must
// not turn their final commit or a new invalidation into a long writer lock.
func (s *sqlStore) deleteStagedBatch(ctx context.Context, tx *sql.Tx, key string, keep *int64) (int64, error) {
	query := `DELETE FROM reconcile_stage WHERE job_key = ` + s.ph(1) + ` AND (version,coord) IN (SELECT version,coord FROM reconcile_stage WHERE job_key = ` + s.ph(2)
	args := []any{key, key}
	if keep != nil {
		query += ` AND version != ` + s.ph(3)
		args = append(args, *keep)
	}
	query += ` ORDER BY version,coord LIMIT 200)`
	r, err := tx.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, err
	}
	return r.RowsAffected()
}

func (s *sqlStore) RetryJob(ctx context.Context, j ReconcileJob, cause error) error {
	delay := time.Second << min(j.Attempts, 8)
	delay += time.Duration(time.Now().UnixNano() % int64(delay/4+1))
	next := time.Now().Add(delay).UnixMilli()
	return s.runWrite(func() error {
		_, err := s.db.ExecContext(ctx, `UPDATE reconcile_jobs SET attempts=attempts+1,last_error = `+s.ph(1)+`, next_attempt = `+s.ph(2)+` WHERE job_key = `+s.ph(3)+` AND version = `+s.ph(4), cause.Error(), next, j.Key, j.Version)
		return err
	})
}

func (s *sqlStore) JobStats(ctx context.Context) (map[string]any, error) {
	var count, oldest int64
	var failures int
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*),COALESCE(MIN(pending_since),0),COALESCE(SUM(CASE WHEN last_error != '' THEN 1 ELSE 0 END),0) FROM reconcile_jobs`).Scan(&count, &oldest, &failures)
	if err != nil {
		return nil, fmt.Errorf("job stats: %w", err)
	}
	age := int64(0)
	if oldest > 0 {
		age = time.Now().UnixMilli() - oldest
	}
	return map[string]any{"pending": count, "oldest_pending_ms": age, "failed": failures}, nil
}
