package main

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrations/*.sql
var migrations embed.FS

type Store struct{ DB *pgxpool.Pool }

func openStore(ctx context.Context, url string) (*Store, error) {
	db, err := pgxpool.New(ctx, url)
	if err != nil {
		return nil, err
	}
	if err = db.Ping(ctx); err != nil {
		db.Close()
		return nil, err
	}
	return &Store{DB: db}, nil
}

func (s *Store) migrate(ctx context.Context) error {
	b, err := migrations.ReadFile("migrations/001_init.sql")
	if err != nil {
		return err
	}
	_, err = s.DB.Exec(ctx, string(b))
	return err
}

const jobColumns = `id::text,type,payload,status,priority,max_attempts,attempts,run_at,leased_until,result,error,created_at,started_at,finished_at,updated_at,idempotency_key,request_hash,lease_token::text`

type scanner interface{ Scan(...any) error }

func scanJob(row scanner) (Job, error) {
	var j Job
	err := row.Scan(&j.ID, &j.Type, &j.Payload, &j.Status, &j.Priority, &j.MaxAttempts, &j.Attempts, &j.RunAt, &j.LeasedUntil, &j.Result, &j.Error, &j.CreatedAt, &j.StartedAt, &j.FinishedAt, &j.UpdatedAt, &j.IdempotencyKey, &j.RequestHash, &j.LeaseToken)
	return j, err
}

var errConflict = errors.New("idempotency key already used for another request")

func (s *Store) submit(ctx context.Context, r SubmitRequest) (Job, bool, error) {
	if err := r.normalize(); err != nil {
		return Job{}, false, err
	}
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return Job{}, false, err
	}
	defer tx.Rollback(ctx)
	id := newUUID()
	runAt := time.Now().UTC()
	if r.RunAt != nil {
		runAt = r.RunAt.UTC()
	}
	var key *string
	if r.IdempotencyKey != "" {
		key = &r.IdempotencyKey
	}
	hash := r.hash()
	row := tx.QueryRow(ctx, `INSERT INTO jobs (id,type,payload,status,priority,max_attempts,run_at,idempotency_key,request_hash)
		VALUES ($1,$2,$3,'queued',$4,$5,$6,$7,$8)
		ON CONFLICT (idempotency_key) DO NOTHING RETURNING `+jobColumns,
		id, r.Type, r.Payload, r.Priority, *r.MaxAttempts, runAt, key, hash)
	j, err := scanJob(row)
	if errors.Is(err, pgx.ErrNoRows) {
		j, err = scanJob(tx.QueryRow(ctx, `SELECT `+jobColumns+` FROM jobs WHERE idempotency_key=$1`, r.IdempotencyKey))
		if err != nil {
			return Job{}, false, err
		}
		if j.RequestHash != hash {
			return Job{}, false, errConflict
		}
		if err = tx.Commit(ctx); err != nil {
			return Job{}, false, err
		}
		return j, true, nil
	}
	if err != nil {
		return Job{}, false, err
	}
	_, err = tx.Exec(ctx, `INSERT INTO outbox (job_id,available_at) VALUES ($1,$2)`, j.ID, runAt)
	if err != nil {
		return Job{}, false, err
	}
	if err = tx.Commit(ctx); err != nil {
		return Job{}, false, err
	}
	return j, false, nil
}

func (s *Store) getJob(ctx context.Context, id string) (Job, error) {
	return scanJob(s.DB.QueryRow(ctx, `SELECT `+jobColumns+` FROM jobs WHERE id=$1`, id))
}

func (s *Store) claim(ctx context.Context, id string, lease time.Duration) (Job, error) {
	token := newUUID()
	return scanJob(s.DB.QueryRow(ctx, `UPDATE jobs SET status='running',attempts=attempts+1,
		lease_token=$2,leased_until=now()+($3::bigint * interval '1 microsecond'),
		started_at=COALESCE(started_at,now()),updated_at=now(),error=NULL
		WHERE id=$1 AND status='queued' AND run_at<=now() AND attempts<max_attempts
		RETURNING `+jobColumns, id, token, lease.Microseconds()))
}

func (s *Store) heartbeat(ctx context.Context, id, token string, lease time.Duration) (bool, error) {
	tag, err := s.DB.Exec(ctx, `UPDATE jobs SET leased_until=now()+($3::bigint * interval '1 microsecond'),updated_at=now()
		WHERE id=$1 AND lease_token=$2 AND status='running' AND leased_until>now()`, id, token, lease.Microseconds())
	return tag.RowsAffected() == 1, err
}

func (s *Store) complete(ctx context.Context, j Job, result []byte) (bool, error) {
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)
	// Lock first so the event and terminal transition use the same fenced transaction.
	var token string
	err = tx.QueryRow(ctx, `SELECT lease_token::text FROM jobs WHERE id=$1 AND status='running' AND leased_until>now() FOR UPDATE`, j.ID).Scan(&token)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if j.LeaseToken == nil || token != *j.LeaseToken {
		return false, nil
	}
	if j.Type == "record_event" {
		var p struct {
			Name string `json:"name"`
		}
		if err = json.Unmarshal(j.Payload, &p); err != nil {
			return false, err
		}
		_, err = tx.Exec(ctx, `INSERT INTO recorded_events (job_id,name) VALUES ($1,$2) ON CONFLICT (job_id) DO NOTHING`, j.ID, p.Name)
		if err != nil {
			return false, err
		}
	}
	_, err = tx.Exec(ctx, `UPDATE jobs SET status='succeeded',result=$3,lease_token=NULL,leased_until=NULL,
		finished_at=now(),updated_at=now() WHERE id=$1 AND lease_token=$2`, j.ID, token, result)
	if err != nil {
		return false, err
	}
	return true, tx.Commit(ctx)
}

func (s *Store) fail(ctx context.Context, j Job, reason string, delay time.Duration) (bool, error) {
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)
	var attempts, maxAttempts int
	err = tx.QueryRow(ctx, `SELECT attempts,max_attempts FROM jobs WHERE id=$1 AND lease_token=$2 AND status='running' AND leased_until>now() FOR UPDATE`, j.ID, j.LeaseToken).Scan(&attempts, &maxAttempts)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if len(reason) > 2000 {
		reason = reason[:2000]
	}
	if attempts >= maxAttempts {
		_, err = tx.Exec(ctx, `UPDATE jobs SET status='failed',error=$2,finished_at=now(),lease_token=NULL,leased_until=NULL,updated_at=now() WHERE id=$1`, j.ID, reason)
	} else {
		_, err = tx.Exec(ctx, `UPDATE jobs SET status='queued',error=$2,run_at=now()+($3::bigint * interval '1 microsecond'),lease_token=NULL,leased_until=NULL,enqueued_at=NULL,updated_at=now() WHERE id=$1`, j.ID, reason, delay.Microseconds())
		if err == nil {
			_, err = tx.Exec(ctx, `INSERT INTO outbox (job_id,available_at) SELECT id,run_at FROM jobs WHERE id=$1`, j.ID)
		}
	}
	if err != nil {
		return false, err
	}
	return true, tx.Commit(ctx)
}

func (s *Store) expireLeases(ctx context.Context, batch int) (int, error) {
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)
	rows, err := tx.Query(ctx, `SELECT id::text,attempts,max_attempts FROM jobs WHERE status='running' AND leased_until<=now() ORDER BY leased_until LIMIT $1 FOR UPDATE SKIP LOCKED`, batch)
	if err != nil {
		return 0, err
	}
	type expired struct {
		id            string
		attempts, max int
	}
	var items []expired
	for rows.Next() {
		var x expired
		if err := rows.Scan(&x.id, &x.attempts, &x.max); err != nil {
			rows.Close()
			return 0, err
		}
		items = append(items, x)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return 0, err
	}
	for _, x := range items {
		if x.attempts >= x.max {
			_, err = tx.Exec(ctx, `UPDATE jobs SET status='failed',error='lease expired',finished_at=now(),lease_token=NULL,leased_until=NULL,updated_at=now() WHERE id=$1`, x.id)
		} else {
			_, err = tx.Exec(ctx, `UPDATE jobs SET status='queued',error='lease expired',run_at=now()+($2::bigint * interval '1 microsecond'),lease_token=NULL,leased_until=NULL,enqueued_at=NULL,updated_at=now() WHERE id=$1`, x.id, retryDelay(x.attempts).Microseconds())
			if err == nil {
				_, err = tx.Exec(ctx, `INSERT INTO outbox (job_id,available_at) SELECT id,run_at FROM jobs WHERE id=$1`, x.id)
			}
		}
		if err != nil {
			return 0, err
		}
	}
	return len(items), tx.Commit(ctx)
}

type readyJob struct {
	ID, Priority string
	RunAt        time.Time
}

func (s *Store) publishOutbox(ctx context.Context, q *Queue, batch int) (int, error) {
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)
	rows, err := tx.Query(ctx, `SELECT o.id,j.id::text,j.priority,j.run_at FROM outbox o JOIN jobs j ON j.id=o.job_id
		WHERE o.delivered_at IS NULL AND o.available_at<=now() ORDER BY o.id LIMIT $1 FOR UPDATE OF o SKIP LOCKED`, batch)
	if err != nil {
		return 0, err
	}
	type item struct {
		outboxID int64
		readyJob
	}
	var items []item
	for rows.Next() {
		var x item
		if err := rows.Scan(&x.outboxID, &x.ID, &x.Priority, &x.RunAt); err != nil {
			rows.Close()
			return 0, err
		}
		items = append(items, x)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return 0, err
	}
	for _, x := range items {
		var status string
		if err = tx.QueryRow(ctx, `SELECT status FROM jobs WHERE id=$1`, x.ID).Scan(&status); err != nil {
			return 0, err
		}
		if status == "queued" {
			if err = q.publish(ctx, x.readyJob); err != nil {
				return 0, err
			}
			if _, err = tx.Exec(ctx, `UPDATE jobs SET enqueued_at=now() WHERE id=$1 AND status='queued'`, x.ID); err != nil {
				return 0, err
			}
		}
		if _, err = tx.Exec(ctx, `UPDATE outbox SET delivered_at=now() WHERE id=$1`, x.outboxID); err != nil {
			return 0, err
		}
	}
	return len(items), tx.Commit(ctx)
}

func (s *Store) reconcile(ctx context.Context, q *Queue, batch int) (int, error) {
	rows, err := s.DB.Query(ctx, `SELECT id::text,priority,run_at FROM jobs WHERE status='queued' AND run_at<=now()
		AND (enqueued_at IS NULL OR enqueued_at<now()-interval '30 seconds') ORDER BY run_at LIMIT $1`, batch)
	if err != nil {
		return 0, err
	}
	var items []readyJob
	for rows.Next() {
		var x readyJob
		if err := rows.Scan(&x.ID, &x.Priority, &x.RunAt); err != nil {
			rows.Close()
			return 0, err
		}
		items = append(items, x)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return 0, err
	}
	for _, x := range items {
		if err = q.publish(ctx, x); err != nil {
			return 0, err
		}
		if _, err = s.DB.Exec(ctx, `UPDATE jobs SET enqueued_at=now() WHERE id=$1 AND status='queued'`, x.ID); err != nil {
			return 0, err
		}
	}
	return len(items), nil
}

func (s *Store) check(ctx context.Context) error {
	if err := s.DB.Ping(ctx); err != nil {
		return fmt.Errorf("postgres: %w", err)
	}
	return nil
}
