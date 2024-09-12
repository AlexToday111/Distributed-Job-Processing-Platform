package api

import (
	"context"
	"encoding/json"
	"github.com/google/uuid"
	"io"
	"jobplatform/internal/domain"
	"jobplatform/internal/observability"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type fakeStore struct {
	created domain.CreateJob
	count   int
}

func (s *fakeStore) Create(ctx context.Context, c domain.CreateJob, id, p, st string) (domain.Job, bool, error) {
	s.created = c
	s.count++
	return domain.Job{ID: uuid.NewString(), Status: domain.Ready}, true, nil
}
func (*fakeStore) Get(context.Context, string) (domain.Job, error) {
	return domain.Job{}, domain.ErrNotFound
}
func (*fakeStore) List(context.Context, domain.Filter) (domain.Page, error) {
	return domain.Page{Jobs: []domain.Job{}}, nil
}
func (*fakeStore) Mutate(context.Context, string, domain.Status) (domain.Job, error) {
	return domain.Job{}, domain.ErrConflict
}
func (*fakeStore) Attempts(context.Context, string) ([]domain.Attempt, error) {
	return nil, domain.ErrNotFound
}
func TestAPIValidationAndErrors(t *testing.T) {
	store := &fakeStore{}
	h := &Handler{Store: store, Metrics: observability.NewMetrics(), Log: slog.New(slog.NewTextHandler(io.Discard, nil)), Ping: func(context.Context) error { return nil }}
	routes := h.Routes()
	for _, c := range []struct {
		method, path, body string
		status             int
	}{{"POST", "/api/v1/jobs", `{"type":"sleep","payload":{}}`, 201}, {"POST", "/api/v1/jobs", `{"type":"sleep","payload":{},"unknown":1}`, 400}, {"POST", "/api/v1/jobs", `{"type":"sleep","payload":{}} {}`, 400}, {"POST", "/api/v1/jobs", `{"type":"sleep","payload":{},"maxAttempts":-1}`, 400}, {"POST", "/api/v1/jobs", `{"type":"sleep","payload":"` + strings.Repeat("x", 1<<20) + `"}`, 413}, {"GET", "/api/v1/jobs/not-a-uuid", "", 400}, {"GET", "/api/v1/jobs/" + uuid.NewString(), "", 404}, {"GET", "/api/v1/jobs?limit=1000", "", 400}, {"GET", "/api/v1/jobs?cursor=bad", "", 400}, {"GET", "/api/v1/jobs?status=BAD", "", 400}, {"GET", "/health/ready", "", 200}} {
		req := httptest.NewRequest(c.method, c.path, strings.NewReader(c.body))
		res := httptest.NewRecorder()
		routes.ServeHTTP(res, req)
		if res.Code != c.status {
			t.Errorf("%s %s: %d != %d: %s", c.method, c.path, res.Code, c.status, res.Body.String())
		}
		if _, e := uuid.Parse(res.Header().Get("X-Request-Id")); e != nil {
			t.Fatal("missing correlation ID")
		}
		if c.status >= 400 {
			var v map[string]string
			if e := json.Unmarshal(res.Body.Bytes(), &v); e != nil || v["code"] == "" || v["requestId"] == "" {
				t.Fatal("inconsistent errors")
			}
		}
	}
	if store.count != 1 {
		t.Fatal("invalid jobs persisted")
	}
}
func TestAuthAndIdempotencyHeader(t *testing.T) {
	store := &fakeStore{}
	h := &Handler{Store: store, Metrics: observability.NewMetrics(), Log: slog.New(slog.NewTextHandler(io.Discard, nil)), Key: "secret", Ping: func(context.Context) error { return nil }}
	routes := h.Routes()
	for _, auth := range []bool{false, true} {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs", strings.NewReader(`{"type":"sleep","payload":{}}`))
		req.Header.Set("Idempotency-Key", "unique")
		if auth {
			req.Header.Set("Authorization", "Bearer secret")
		}
		res := httptest.NewRecorder()
		routes.ServeHTTP(res, req)
		expected := 401
		if auth {
			expected = 201
		}
		if res.Code != expected {
			t.Fatal(res.Code)
		}
	}
	if store.count != 1 || store.created.IdempotencyKey != "unique" {
		t.Fatal("header ignored")
	}
}
