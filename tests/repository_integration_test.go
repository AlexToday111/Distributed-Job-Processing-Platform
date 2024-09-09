//go:build integration

package tests

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"jobplatform/internal/domain"
	"jobplatform/internal/repository/postgres"
)

func repository(t *testing.T) *postgres.Repository {
	t.Helper()
	ctx := context.Background()
	container, e := tcpostgres.Run(ctx, "postgres:17-alpine", tcpostgres.WithDatabase("jobs"), tcpostgres.WithUsername("jobs"), tcpostgres.WithPassword("jobs"), tcpostgres.BasicWaitStrategies())
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		if e := container.Terminate(context.Background()); e != nil {
			t.Log(e)
		}
	})
	url, e := container.ConnectionString(ctx, "sslmode=disable")
	if e != nil {
		t.Fatal(e)
	}
	pool, e := postgres.Open(ctx, url, 30)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(pool.Close)
	migrations, e := filepath.Glob("../migrations/*.up.sql")
	if e != nil {
		t.Fatal(e)
	}
	if len(migrations) == 0 {
		t.Fatal("no migrations")
	}
	for _, path := range migrations {
		migration, e := os.ReadFile(path)
		if e != nil {
			t.Fatal(e)
		}
		if _, e = pool.Exec(ctx, string(migration)); e != nil {
			t.Fatal(e)
		}
	}
	return &postgres.Repository{Pool: pool, Lease: time.Minute, HeartbeatTimeout: time.Minute, RetryBase: time.Millisecond, RetryMax: time.Millisecond, Batch: 100}
}
func create(t *testing.T, r *postgres.Repository, key string) domain.Job {
	t.Helper()
	c := domain.CreateJob{Type: "sleep", Payload: json.RawMessage(`{"durationMs":1}`), IdempotencyKey: key}
	if e := c.Validate(); e != nil {
		t.Fatal(e)
	}
	j, _, e := r.Create(context.Background(), c, "test", "", "")
	if e != nil {
		t.Fatal(e)
	}
	return j
}
func register(t *testing.T, r *postgres.Repository, id string, capacity int) {
	t.Helper()
	if e := r.Register(context.Background(), id, "test", []string{"default"}, capacity, "test"); e != nil {
		t.Fatal(e)
	}
}
func claimOne(t *testing.T, r *postgres.Repository, id string) domain.Job {
	t.Helper()
	js, e := r.Claim(context.Background(), id, 1)
	if e != nil || len(js) != 1 {
		t.Fatalf("claim: %v %d", e, len(js))
	}
	return js[0]
}
func TestRepositoryReliability(t *testing.T) {
	r := repository(t)
	ctx := context.Background()
	t.Run("idempotency 100 concurrent clients", func(t *testing.T) {
		var wg sync.WaitGroup
		ids := make(chan string, 100)
		errs := make(chan error, 100)
		for i := 0; i < 100; i++ {
			wg.Go(func() {
				c := domain.CreateJob{Queue: "default", Type: "sleep", Payload: json.RawMessage(`{}`), MaxAttempts: 3, TimeoutSeconds: 60, IdempotencyKey: "same"}
				j, _, e := r.Create(ctx, c, "test", "", "")
				if e != nil {
					errs <- e
					return
				}
				ids <- j.ID
			})
		}
		wg.Wait()
		close(ids)
		close(errs)
		for e := range errs {
			t.Error(e)
		}
		first := ""
		for id := range ids {
			if first == "" {
				first = id
			}
			if id != first {
				t.Fatal("duplicate idempotent job")
			}
		}
		var n int
		if e := r.Pool.QueryRow(ctx, `SELECT count(*) FROM jobs WHERE idempotency_key='same'`).Scan(&n); e != nil || n != 1 {
			t.Fatalf("count: %d %v", n, e)
		}
	})
	// Each scenario uses a fresh durable state to make claims deterministic.
	reset := func() {
		t.Helper()
		if _, e := r.Pool.Exec(ctx, `TRUNCATE job_attempts,jobs,workers RESTART IDENTITY`); e != nil {
			t.Fatal(e)
		}
	}
	t.Run("multiple schedulers concurrent claims", func(t *testing.T) {
		reset()
		for i := 0; i < 80; i++ {
			create(t, r, "")
		}
		var wg sync.WaitGroup
		seen := make(chan string, 80)
		errs := make(chan error, 8)
		for i := 0; i < 8; i++ {
			id := fmt.Sprintf("worker-%d", i)
			register(t, r, id, 10)
			wg.Go(func() {
				jobs, e := r.Claim(ctx, id, 10)
				if e != nil {
					errs <- e
					return
				}
				for _, j := range jobs {
					seen <- j.ID
				}
			})
		}
		wg.Wait()
		close(seen)
		close(errs)
		for e := range errs {
			t.Error(e)
		}
		unique := map[string]bool{}
		for id := range seen {
			if unique[id] {
				t.Fatal("duplicate concurrent claim")
			}
			unique[id] = true
		}
		if len(unique) != 80 {
			t.Fatalf("claimed %d/80", len(unique))
		}
		var attempts int
		_ = r.Pool.QueryRow(ctx, `SELECT count(*) FROM job_attempts`).Scan(&attempts)
		if attempts != 80 {
			t.Fatalf("attempts %d", attempts)
		}
	})
	t.Run("same worker capacity cannot be exceeded", func(t *testing.T) {
		reset()
		for i := 0; i < 30; i++ {
			create(t, r, "")
		}
		register(t, r, "worker", 4)
		var wg sync.WaitGroup
		var mu sync.Mutex
		total := 0
		for i := 0; i < 8; i++ {
			wg.Go(func() {
				jobs, e := r.Claim(ctx, "worker", 4)
				if e != nil {
					t.Error(e)
				}
				mu.Lock()
				total += len(jobs)
				mu.Unlock()
			})
		}
		wg.Wait()
		if total != 4 {
			t.Fatalf("capacity exceeded: %d", total)
		}
	})
	t.Run("retry DLQ completion idempotency and requeue", func(t *testing.T) {
		reset()
		j := create(t, r, "")
		register(t, r, "worker", 1)
		for attempt := 1; attempt <= 3; attempt++ {
			a := claimOne(t, r, "worker")
			st, e := r.Complete(ctx, "worker", j.ID, a.LeaseToken, false, nil, "failed")
			if e != nil {
				t.Fatal(e)
			}
			expected := domain.RetryWait
			if attempt == 3 {
				expected = domain.DeadLettered
			}
			if st != expected {
				t.Fatalf("status %s", st)
			}
			if _, e = r.Complete(ctx, "worker", j.ID, a.LeaseToken, false, nil, "failed"); e != nil {
				t.Fatalf("lost ack replay: %v", e)
			}
			_, e = r.Pool.Exec(ctx, `UPDATE jobs SET scheduled_at=now()-interval '1 second' WHERE id=$1`, j.ID)
			if e != nil {
				t.Fatal(e)
			}
			if _, e = r.Tick(ctx); e != nil {
				t.Fatal(e)
			}
		}
		a, e := r.Attempts(ctx, j.ID)
		if e != nil || len(a) != 3 {
			t.Fatalf("history: %d %v", len(a), e)
		}
		j, e = r.Mutate(ctx, j.ID, domain.Ready)
		if e != nil || j.Attempts != 0 {
			t.Fatalf("requeue: %+v %v", j, e)
		}
		if a, e = r.Attempts(ctx, j.ID); e != nil || len(a) != 3 {
			t.Fatal("requeue erased history")
		}
	})
	t.Run("expired lease recovery and fencing", func(t *testing.T) {
		reset()
		j := create(t, r, "")
		register(t, r, "old", 1)
		register(t, r, "new", 1)
		old := claimOne(t, r, "old")
		if _, e := r.Pool.Exec(ctx, `UPDATE jobs SET lease_expires_at=now()-interval '1 second' WHERE id=$1`, j.ID); e != nil {
			t.Fatal(e)
		}
		if rejected, e := r.Heartbeat(ctx, "old", []postgres.Lease{{ID: j.ID, Token: old.LeaseToken}}); e != nil || len(rejected) != 1 {
			t.Fatalf("expired lease revived: %v %v", rejected, e)
		}
		if _, e := r.Complete(ctx, "old", j.ID, old.LeaseToken, true, json.RawMessage(`{}`), ""); !errors.Is(e, domain.ErrConflict) {
			t.Fatalf("expired unreaped completion accepted: %v", e)
		}
		if _, e := r.Tick(ctx); e != nil {
			t.Fatal(e)
		}
		_, _ = r.Pool.Exec(ctx, `UPDATE jobs SET scheduled_at=now()-interval '1 second' WHERE id=$1`, j.ID)
		_, _ = r.Tick(ctx)
		current := claimOne(t, r, "new")
		if _, e := r.Complete(ctx, "old", j.ID, old.LeaseToken, true, json.RawMessage(`{}`), ""); !errors.Is(e, domain.ErrConflict) {
			t.Fatalf("stale completion accepted: %v", e)
		}
		if _, e := r.Complete(ctx, "new", j.ID, current.LeaseToken, true, json.RawMessage(`{"ok":true}`), ""); e != nil {
			t.Fatal(e)
		}
		out, e := r.Get(ctx, j.ID)
		if e != nil || out.Status != domain.Succeeded {
			t.Fatal("recovery failed")
		}
		if !out.ScheduledAt.Equal(current.ScheduledAt) {
			t.Fatal("success changed scheduling eligibility timestamp")
		}
	})
	t.Run("cancel fences completion and rejects heartbeat", func(t *testing.T) {
		reset()
		j := create(t, r, "")
		register(t, r, "worker", 1)
		a := claimOne(t, r, "worker")
		if _, e := r.Mutate(ctx, j.ID, domain.Cancelled); e != nil {
			t.Fatal(e)
		}
		rejected, e := r.Heartbeat(ctx, "worker", []postgres.Lease{{ID: j.ID, Token: a.LeaseToken}})
		if e != nil || len(rejected) != 1 {
			t.Fatal("missing cancellation")
		}
		if _, e := r.Complete(ctx, "worker", j.ID, a.LeaseToken, true, json.RawMessage(`{}`), ""); !errors.Is(e, domain.ErrConflict) {
			t.Fatal("cancel overwritten")
		}
		history, e := r.Attempts(ctx, j.ID)
		if e != nil || len(history) != 1 || history[0].Status != "CANCELLED" {
			t.Fatal("inconsistent attempt")
		}
	})
	t.Run("delay priority and cursor", func(t *testing.T) {
		reset()
		c := domain.CreateJob{Queue: "default", Type: "sleep", Payload: json.RawMessage(`{}`), MaxAttempts: 3, TimeoutSeconds: 60, DelaySeconds: 30}
		delayed, _, e := r.Create(ctx, c, "test", "", "")
		if e != nil || delayed.Status != domain.Scheduled {
			t.Fatal("delay ignored")
		}
		low := create(t, r, "")
		high := create(t, r, "")
		_, e = r.Pool.Exec(ctx, `UPDATE jobs SET priority=10 WHERE id=$1`, high.ID)
		if e != nil {
			t.Fatal(e)
		}
		register(t, r, "worker", 10)
		js, e := r.Claim(ctx, "worker", 10)
		if e != nil || len(js) != 2 || js[0].ID != high.ID || js[1].ID != low.ID {
			t.Fatalf("priority/delay: %+v %v", js, e)
		}
		page, e := r.List(ctx, domain.Filter{Limit: 2})
		if e != nil || len(page.Jobs) != 2 || page.NextCursor == "" {
			t.Fatal("first page")
		}
		next, e := r.List(ctx, domain.Filter{Limit: 2, Cursor: page.NextCursor})
		if e != nil || len(next.Jobs) != 1 || next.Jobs[0].ID == page.Jobs[0].ID || next.Jobs[0].ID == page.Jobs[1].ID {
			t.Fatal("cursor duplicate or missing")
		}
		if _, e = r.List(ctx, domain.Filter{Cursor: uuid.NewString()}); e == nil {
			t.Fatal("bad cursor accepted")
		}
	})
}
