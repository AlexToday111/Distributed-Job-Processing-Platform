package postgres

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"jobplatform/internal/domain"
)

type Repository struct {
	Pool                                         *pgxpool.Pool
	Lease, HeartbeatTimeout, RetryBase, RetryMax time.Duration
	Batch                                        int
}

func interval(d time.Duration) pgtype.Interval {
	return pgtype.Interval{Microseconds: d.Microseconds(), Valid: true}
}

func Open(ctx context.Context, url string, maxConns int) (*pgxpool.Pool, error) {
	c, e := pgxpool.ParseConfig(url)
	if e != nil {
		return nil, e
	}
	c.MaxConns = int32(maxConns)
	c.MinConns = 1
	c.MaxConnLifetime = time.Hour
	c.MaxConnIdleTime = 5 * time.Minute
	return pgxpool.NewWithConfig(ctx, c)
}

const cols = `id::text,queue,type,payload,status,priority,scheduled_at,max_attempts,attempts,timeout_seconds,coalesce(idempotency_key,''),created_at,updated_at,started_at,completed_at,result,last_error,worker_id,coalesce(lease_token::text,''),lease_expires_at,request_id,trace_parent,trace_state`

type scanner interface{ Scan(...any) error }

func scan(s scanner) (domain.Job, error) {
	var j domain.Job
	e := s.Scan(&j.ID, &j.Queue, &j.Type, &j.Payload, &j.Status, &j.Priority, &j.ScheduledAt, &j.MaxAttempts, &j.Attempts, &j.TimeoutSeconds, &j.IdempotencyKey, &j.CreatedAt, &j.UpdatedAt, &j.StartedAt, &j.CompletedAt, &j.Result, &j.LastError, &j.WorkerID, &j.LeaseToken, &j.LeaseExpiresAt, &j.RequestID, &j.TraceParent, &j.TraceState)
	if errors.Is(e, pgx.ErrNoRows) {
		e = domain.ErrNotFound
	}
	return j, e
}
func (r *Repository) Create(ctx context.Context, c domain.CreateJob, requestID, parent, state string) (domain.Job, bool, error) {
	run := time.Now().UTC().Add(time.Duration(c.DelaySeconds) * time.Second)
	if c.RunAt != nil {
		run = *c.RunAt
	}
	status := domain.Ready
	if run.After(time.Now()) {
		status = domain.Scheduled
	}
	j, e := scan(r.Pool.QueryRow(ctx, `INSERT INTO jobs(id,queue,type,payload,status,priority,scheduled_at,max_attempts,timeout_seconds,idempotency_key,request_id,trace_parent,trace_state) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,NULLIF($10,''),$11,$12,$13) ON CONFLICT(queue,idempotency_key) DO NOTHING RETURNING `+cols, uuid.NewString(), c.Queue, c.Type, c.Payload, status, c.Priority, run, c.MaxAttempts, c.TimeoutSeconds, c.IdempotencyKey, requestID, parent, state))
	if errors.Is(e, domain.ErrNotFound) && c.IdempotencyKey != "" {
		j, e = scan(r.Pool.QueryRow(ctx, `SELECT `+cols+` FROM jobs WHERE queue=$1 AND idempotency_key=$2`, c.Queue, c.IdempotencyKey))
		return j, false, e
	}
	var dbErr *pgconn.PgError
	if errors.As(e, &dbErr) {
		if dbErr.ConstraintName == "jobs_payload_size" {
			e = domain.ErrPayloadTooLarge
		}
		if dbErr.Code == "22003" {
			e = domain.ErrInvalidPayload
		}
	}
	return j, e == nil, e
}
func (r *Repository) Get(ctx context.Context, id string) (domain.Job, error) {
	return scan(r.Pool.QueryRow(ctx, `SELECT `+cols+` FROM jobs WHERE id=$1`, id))
}

type cursor struct {
	At time.Time `json:"at"`
	ID string    `json:"id"`
}

func (r *Repository) List(ctx context.Context, f domain.Filter) (domain.Page, error) {
	var p domain.Page
	p.Jobs = []domain.Job{}
	var cur *cursor
	if f.Cursor != "" {
		b, e := base64.RawURLEncoding.DecodeString(f.Cursor)
		if e != nil {
			return p, fmt.Errorf("invalid cursor")
		}
		cur = &cursor{}
		if e = json.Unmarshal(b, cur); e != nil || cur.At.IsZero() {
			return p, fmt.Errorf("invalid cursor")
		}
		if _, e = uuid.Parse(cur.ID); e != nil {
			return p, fmt.Errorf("invalid cursor")
		}
	}
	var at any
	var id any
	if cur != nil {
		at = cur.At
		id = cur.ID
	}
	if f.Limit < 1 || f.Limit > 100 {
		f.Limit = 50
	}
	rows, e := r.Pool.Query(ctx, `SELECT `+cols+` FROM jobs WHERE ($1='' OR status=$1) AND ($2='' OR queue=$2) AND ($3='' OR type=$3) AND ($4='' OR worker_id=$4) AND ($5::timestamptz IS NULL OR created_at >= $5) AND ($6::timestamptz IS NULL OR created_at <= $6) AND ($7::timestamptz IS NULL OR (created_at,id)<($7,$8::uuid)) ORDER BY created_at DESC,id DESC LIMIT $9`, f.Status, f.Queue, f.Type, f.Worker, f.CreatedFrom, f.CreatedTo, at, id, f.Limit+1)
	if e != nil {
		return p, e
	}
	defer rows.Close()
	for rows.Next() {
		j, e := scan(rows)
		if e != nil {
			return p, e
		}
		p.Jobs = append(p.Jobs, j)
	}
	if e = rows.Err(); e != nil {
		return p, e
	}
	if len(p.Jobs) > f.Limit {
		p.Jobs = p.Jobs[:f.Limit]
		last := p.Jobs[len(p.Jobs)-1]
		b, _ := json.Marshal(cursor{last.CreatedAt, last.ID})
		p.NextCursor = base64.RawURLEncoding.EncodeToString(b)
	}
	return p, nil
}
func (r *Repository) Attempts(ctx context.Context, id string) ([]domain.Attempt, error) {
	if _, e := r.Get(ctx, id); e != nil {
		return nil, e
	}
	rows, e := r.Pool.Query(ctx, `SELECT id,job_id::text,attempt_number,worker_id,started_at,finished_at,status,error,duration_ms FROM job_attempts WHERE job_id=$1 ORDER BY id`, id)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []domain.Attempt{}
	for rows.Next() {
		var a domain.Attempt
		if e = rows.Scan(&a.ID, &a.JobID, &a.Number, &a.WorkerID, &a.StartedAt, &a.FinishedAt, &a.Status, &a.Error, &a.DurationMS); e != nil {
			return nil, e
		}
		out = append(out, a)
	}
	return out, rows.Err()
}
func (r *Repository) Mutate(ctx context.Context, id string, to domain.Status) (domain.Job, error) {
	tx, e := r.Pool.Begin(ctx)
	if e != nil {
		return domain.Job{}, e
	}
	defer rollback(tx)
	j, e := scan(tx.QueryRow(ctx, `SELECT `+cols+` FROM jobs WHERE id=$1 FOR UPDATE`, id))
	if e != nil {
		return j, e
	}
	if j.Status == to && to == domain.Cancelled {
		return j, tx.Commit(ctx)
	}
	if !domain.CanTransition(j.Status, to) {
		return j, domain.ErrConflict
	}
	if j.Status == domain.Running {
		_, e = tx.Exec(ctx, `UPDATE job_attempts SET finished_at=now(),status='CANCELLED',duration_ms=(extract(epoch FROM now()-started_at)*1000)::bigint WHERE lease_token=$1 AND finished_at IS NULL`, j.LeaseToken)
		if e != nil {
			return j, e
		}
	}
	j, e = scan(tx.QueryRow(ctx, `UPDATE jobs SET status=$2,updated_at=now(),lease_token=NULL,lease_expires_at=NULL,attempts=CASE WHEN $2='READY' THEN 0 ELSE attempts END,scheduled_at=CASE WHEN $2='READY' THEN now() ELSE scheduled_at END,completed_at=CASE WHEN $2='CANCELLED' THEN now() ELSE NULL END,result=NULL,last_error='',started_at=CASE WHEN $2='READY' THEN NULL ELSE started_at END WHERE id=$1 RETURNING `+cols, id, to))
	if e != nil {
		return j, e
	}
	return j, tx.Commit(ctx)
}
func rollback(tx pgx.Tx) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = tx.Rollback(ctx)
}
func (r *Repository) Register(ctx context.Context, id, host string, queues []string, capacity int, version string) error {
	_, e := r.Pool.Exec(ctx, `INSERT INTO workers(id,hostname,queues,concurrency,version) VALUES($1,$2,$3,$4,$5) ON CONFLICT(id) DO UPDATE SET hostname=excluded.hostname,queues=excluded.queues,concurrency=excluded.concurrency,version=excluded.version,last_heartbeat=now()`, id, host, queues, capacity, version)
	return e
}

type Lease struct{ ID, Token string }

func (r *Repository) Heartbeat(ctx context.Context, worker string, active []Lease) ([]Lease, error) {
	tag, e := r.Pool.Exec(ctx, `UPDATE workers SET last_heartbeat=now() WHERE id=$1`, worker)
	if e != nil {
		return nil, e
	}
	if tag.RowsAffected() == 0 {
		return nil, domain.ErrNotFound
	}
	cancel := []Lease{}
	for _, a := range active {
		tag, e = r.Pool.Exec(ctx, `UPDATE jobs SET lease_expires_at=clock_timestamp()+$4::interval WHERE id=$1 AND lease_token=$2 AND worker_id=$3 AND status='RUNNING' AND lease_expires_at>clock_timestamp()`, a.ID, a.Token, worker, interval(r.Lease))
		if e != nil {
			return nil, e
		}
		if tag.RowsAffected() == 0 {
			cancel = append(cancel, a)
		}
	}
	return cancel, nil
}
func (r *Repository) Claim(ctx context.Context, worker string, slots int) ([]domain.Job, error) {
	out := []domain.Job{}
	tx, e := r.Pool.Begin(ctx)
	if e != nil {
		return nil, e
	}
	defer rollback(tx)
	// Serializing claims per worker makes capacity authoritative even across schedulers.
	var queues []string
	var capacity int
	e = tx.QueryRow(ctx, `SELECT queues,concurrency FROM workers WHERE id=$1 AND last_heartbeat>now()-$2::interval FOR UPDATE`, worker, interval(r.HeartbeatTimeout)).Scan(&queues, &capacity)
	if errors.Is(e, pgx.ErrNoRows) {
		return nil, domain.ErrNotFound
	}
	if e != nil {
		return nil, e
	}
	var running int
	e = tx.QueryRow(ctx, `SELECT count(*) FROM jobs WHERE status='RUNNING' AND worker_id=$1`, worker).Scan(&running)
	if e != nil {
		return nil, e
	}
	slots = min(slots, capacity-running, r.Batch)
	if slots <= 0 {
		return out, tx.Commit(ctx)
	}
	rows, e := tx.Query(ctx, `SELECT `+cols+` FROM jobs WHERE status='READY' AND queue=ANY($1) ORDER BY priority DESC,created_at,id LIMIT $2 FOR UPDATE SKIP LOCKED`, queues, slots)
	if e != nil {
		return nil, e
	}
	for rows.Next() {
		j, e := scan(rows)
		if e != nil {
			rows.Close()
			return nil, e
		}
		out = append(out, j)
	}
	rows.Close()
	if e = rows.Err(); e != nil {
		return nil, e
	}
	for i := range out {
		j := &out[i]
		j.LeaseToken = uuid.NewString()
		j.Attempts++
		j.WorkerID = worker
		j.Status = domain.Running
		_, e = tx.Exec(ctx, `UPDATE jobs SET status='RUNNING',attempts=attempts+1,worker_id=$2,lease_token=$3,lease_expires_at=clock_timestamp()+$4::interval,started_at=now(),updated_at=now() WHERE id=$1`, j.ID, worker, j.LeaseToken, interval(r.Lease))
		if e != nil {
			return nil, e
		}
		_, e = tx.Exec(ctx, `INSERT INTO job_attempts(job_id,attempt_number,worker_id,lease_token,status) VALUES($1,$2,$3,$4,'RUNNING')`, j.ID, j.Attempts, worker, j.LeaseToken)
		if e != nil {
			return nil, e
		}
	}
	return out, tx.Commit(ctx)
}
func (r *Repository) Complete(ctx context.Context, worker, id, token string, success bool, result json.RawMessage, errText string) (domain.Status, error) {
	tx, e := r.Pool.Begin(ctx)
	if e != nil {
		return "", e
	}
	defer rollback(tx)
	j, e := scan(tx.QueryRow(ctx, `SELECT `+cols+` FROM jobs WHERE id=$1 FOR UPDATE`, id))
	if e != nil {
		return "", e
	}
	// Check the database clock after acquiring ownership's row lock. A scheduler
	// clock skew or time spent waiting for the lock must not revive an expired lease.
	var liveLease bool
	if j.Status == domain.Running && j.LeaseToken == token && j.WorkerID == worker {
		if e := tx.QueryRow(ctx, `SELECT lease_expires_at>clock_timestamp() FROM jobs WHERE id=$1`, id).Scan(&liveLease); e != nil {
			return "", e
		}
	}
	if !liveLease {
		var done bool
		attemptStatus := "FAILED"
		if success {
			attemptStatus = "SUCCEEDED"
		}
		e = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM job_attempts WHERE job_id=$1 AND worker_id=$2 AND lease_token=$3 AND status=$4 AND finished_at IS NOT NULL)`, id, worker, token, attemptStatus).Scan(&done)
		if e != nil {
			return "", e
		}
		if done {
			return "", tx.Commit(ctx)
		}
		return "", domain.ErrConflict
	}
	status, e := r.finish(ctx, tx, j, success, result, errText)
	if e != nil {
		return "", e
	}
	return status, tx.Commit(ctx)
}
func (r *Repository) finish(ctx context.Context, tx pgx.Tx, j domain.Job, success bool, result json.RawMessage, errText string) (domain.Status, error) {
	status := domain.Succeeded
	attemptStatus := "SUCCEEDED"
	delay := time.Duration(0)
	if !success {
		status = domain.RetryWait
		attemptStatus = "FAILED"
		delay = domain.Backoff(j.Attempts, r.RetryBase, r.RetryMax, rand.Float64())
		if j.Attempts >= j.MaxAttempts {
			status = domain.DeadLettered
		}
		result = nil
	}
	if !domain.CanTransition(j.Status, status) {
		return "", domain.ErrConflict
	}
	if len(errText) > 4096 {
		errText = errText[:4096]
	}
	_, e := tx.Exec(ctx, `UPDATE jobs SET status=$2,result=$3,last_error=$4,scheduled_at=CASE WHEN $2='RETRY_WAIT' THEN now()+$5::interval ELSE scheduled_at END,lease_token=NULL,lease_expires_at=NULL,completed_at=CASE WHEN $2 IN ('SUCCEEDED','DEAD_LETTERED') THEN now() ELSE NULL END,updated_at=now() WHERE id=$1`, j.ID, status, result, errText, interval(delay))
	if e != nil {
		return "", e
	}
	_, e = tx.Exec(ctx, `UPDATE job_attempts SET status=$2,error=$3,finished_at=now(),duration_ms=(extract(epoch FROM now()-started_at)*1000)::bigint WHERE lease_token=$1 AND finished_at IS NULL`, j.LeaseToken, attemptStatus, errText)
	return status, e
}
func (r *Repository) Tick(ctx context.Context) (map[domain.Status]int, error) {
	counts := map[domain.Status]int{}
	tx, e := r.Pool.Begin(ctx)
	if e != nil {
		return counts, e
	}
	defer rollback(tx)
	_, e = tx.Exec(ctx, `WITH due AS (SELECT id FROM jobs WHERE status IN ('SCHEDULED','RETRY_WAIT') AND scheduled_at<=now() ORDER BY scheduled_at LIMIT $1 FOR UPDATE SKIP LOCKED) UPDATE jobs SET status='READY',updated_at=now() FROM due WHERE jobs.id=due.id`, r.Batch)
	if e != nil {
		return counts, e
	}
	rows, e := tx.Query(ctx, `SELECT `+cols+` FROM jobs WHERE status='RUNNING' AND lease_expires_at<=now() ORDER BY lease_expires_at LIMIT $1 FOR UPDATE SKIP LOCKED`, r.Batch)
	if e != nil {
		return counts, e
	}
	jobs := []domain.Job{}
	for rows.Next() {
		j, e := scan(rows)
		if e != nil {
			rows.Close()
			return counts, e
		}
		jobs = append(jobs, j)
	}
	rows.Close()
	if e = rows.Err(); e != nil {
		return counts, e
	}
	for _, j := range jobs {
		s, e := r.finish(ctx, tx, j, false, nil, "lease expired")
		if e != nil {
			return counts, e
		}
		counts[s]++
		if _, e = tx.Exec(ctx, `UPDATE job_attempts SET status='EXPIRED' WHERE lease_token=$1`, j.LeaseToken); e != nil {
			return counts, e
		}
	}
	return counts, tx.Commit(ctx)
}
