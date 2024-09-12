package api

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"jobplatform/internal/domain"
	"jobplatform/internal/observability"
)

type Store interface {
	Create(context.Context, domain.CreateJob, string, string, string) (domain.Job, bool, error)
	Get(context.Context, string) (domain.Job, error)
	List(context.Context, domain.Filter) (domain.Page, error)
	Mutate(context.Context, string, domain.Status) (domain.Job, error)
	Attempts(context.Context, string) ([]domain.Attempt, error)
}
type Handler struct {
	Store   Store
	Metrics *observability.Metrics
	Log     *slog.Logger
	Key     string
	Ping    func(context.Context) error
}

func (h *Handler) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health/live", func(w http.ResponseWriter, r *http.Request) { write(w, 200, map[string]string{"status": "live"}) })
	mux.HandleFunc("GET /health/ready", func(w http.ResponseWriter, r *http.Request) {
		ctx, c := context.WithTimeout(r.Context(), 2*time.Second)
		defer c()
		if e := h.Ping(ctx); e != nil {
			write(w, 503, map[string]string{"status": "not ready"})
			return
		}
		write(w, 200, map[string]string{"status": "ready"})
	})
	mux.Handle("GET /metrics", h.Metrics.Handler())
	mux.HandleFunc("POST /api/v1/jobs", h.create)
	mux.HandleFunc("GET /api/v1/jobs", h.list)
	mux.HandleFunc("GET /api/v1/dead-letter", h.list)
	mux.HandleFunc("GET /api/v1/jobs/{id}", h.get)
	mux.HandleFunc("GET /api/v1/jobs/{id}/attempts", h.attempts)
	mux.HandleFunc("POST /api/v1/jobs/{id}/cancel", h.mutate)
	mux.HandleFunc("POST /api/v1/jobs/{id}/retry", h.mutate)
	mux.HandleFunc("POST /api/v1/jobs/{id}/requeue", h.mutate)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		h.problem(w, r, 404, "ROUTE_NOT_FOUND", "route does not exist")
	})
	return otelhttp.NewHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestID := r.Header.Get("X-Request-Id")
		requestCtx, cancelRequest := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancelRequest()
		r = r.WithContext(requestCtx)
		if _, e := uuid.Parse(requestID); e != nil {
			requestID = uuid.NewString()
		}
		r.Header.Set("X-Request-Id", requestID)
		w.Header().Set("X-Request-Id", requestID)
		rw := &statusWriter{ResponseWriter: w, status: 200}
		start := time.Now()
		defer func() {
			if v := recover(); v != nil {
				h.Log.Error("HTTP panic", "request_id", requestID, "panic", fmt.Sprint(v))
				h.fail(rw, r, errors.New("panic"))
			}
			route := r.Pattern
			if route == "" {
				route = "unmatched"
			}
			h.Metrics.HTTP.WithLabelValues(route, strconv.Itoa(rw.status)).Inc()
			h.Metrics.HTTPDuration.WithLabelValues(route).Observe(time.Since(start).Seconds())
			h.Log.Info("HTTP request", "request_id", requestID, "method", r.Method, "route", route, "status", rw.status, "duration", time.Since(start))
		}()
		if strings.HasPrefix(r.URL.Path, "/api/") && h.Key != "" && subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte("Bearer "+h.Key)) != 1 {
			h.problem(rw, r, 401, "UNAUTHORIZED", "valid bearer token required")
			return
		}
		mux.ServeHTTP(rw, r)
	}), "http")
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(code int) { w.status = code; w.ResponseWriter.WriteHeader(code) }
func write(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
func (h *Handler) problem(w http.ResponseWriter, r *http.Request, status int, code, msg string) {
	write(w, status, map[string]string{"code": code, "message": msg, "requestId": r.Header.Get("X-Request-Id")})
}
func (h *Handler) fail(w http.ResponseWriter, r *http.Request, e error) {
	switch {
	case errors.Is(e, domain.ErrPayloadTooLarge):
		h.problem(w, r, 413, "PAYLOAD_TOO_LARGE", "normalized payload limit is 1 MiB")
	case errors.Is(e, domain.ErrInvalidPayload):
		h.problem(w, r, 400, "VALIDATION_ERROR", e.Error())
	case errors.Is(e, domain.ErrNotFound):
		h.problem(w, r, 404, "JOB_NOT_FOUND", "job does not exist")
	case errors.Is(e, domain.ErrConflict):
		h.problem(w, r, 409, "STATE_CONFLICT", "operation not allowed in current state")
	default:
		h.Log.Error("request failed", "request_id", r.Header.Get("X-Request-Id"), "error", e)
		h.problem(w, r, 500, "INTERNAL_ERROR", "request failed")
	}
}
func (h *Handler) id(w http.ResponseWriter, r *http.Request) (string, bool) {
	id := r.PathValue("id")
	if _, e := uuid.Parse(id); e != nil {
		h.problem(w, r, 400, "VALIDATION_ERROR", "invalid job UUID")
		return "", false
	}
	return id, true
}
func (h *Handler) create(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	var c domain.CreateJob
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	e := dec.Decode(&c)
	if e == nil {
		var extra any
		if dec.Decode(&extra) != io.EOF {
			e = errors.New("expected a single JSON object")
		}
	}
	if e != nil {
		var large *http.MaxBytesError
		if errors.As(e, &large) {
			h.problem(w, r, 413, "PAYLOAD_TOO_LARGE", "body limit is 1 MiB")
		} else {
			h.problem(w, r, 400, "VALIDATION_ERROR", "invalid JSON body")
		}
		return
	}
	if key := r.Header.Get("Idempotency-Key"); key != "" {
		if c.IdempotencyKey != "" && key != c.IdempotencyKey {
			h.problem(w, r, 400, "VALIDATION_ERROR", "idempotency header and body disagree")
			return
		}
		c.IdempotencyKey = key
	}
	if e = c.Validate(); e != nil {
		h.problem(w, r, 400, "VALIDATION_ERROR", e.Error())
		return
	}
	ctx, span := otel.Tracer("api").Start(r.Context(), "job.persist")
	defer span.End()
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	j, created, e := h.Store.Create(ctx, c, r.Header.Get("X-Request-Id"), carrier.Get("traceparent"), carrier.Get("tracestate"))
	if e != nil {
		h.fail(w, r, e)
		return
	}
	code := 200
	if created {
		code = 201
		h.Metrics.Event("created", 1)
	}
	w.Header().Set("Location", "/api/v1/jobs/"+j.ID)
	write(w, code, j)
}
func (h *Handler) get(w http.ResponseWriter, r *http.Request) {
	id, ok := h.id(w, r)
	if !ok {
		return
	}
	j, e := h.Store.Get(r.Context(), id)
	if e != nil {
		h.fail(w, r, e)
		return
	}
	write(w, 200, j)
}
func (h *Handler) attempts(w http.ResponseWriter, r *http.Request) {
	id, ok := h.id(w, r)
	if !ok {
		return
	}
	a, e := h.Store.Attempts(r.Context(), id)
	if e != nil {
		h.fail(w, r, e)
		return
	}
	write(w, 200, a)
}
func (h *Handler) mutate(w http.ResponseWriter, r *http.Request) {
	id, ok := h.id(w, r)
	if !ok {
		return
	}
	to := domain.Ready
	event := "requeued"
	if strings.HasSuffix(r.URL.Path, "/cancel") {
		to = domain.Cancelled
		event = "cancelled"
	}
	j, e := h.Store.Mutate(r.Context(), id, to)
	if e != nil {
		h.fail(w, r, e)
		return
	}
	h.Metrics.Event(event, 1)
	h.Log.Info("job state changed", "job_id", id, "status", to, "request_id", r.Header.Get("X-Request-Id"))
	write(w, 200, j)
}
func (h *Handler) list(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	f := domain.Filter{Status: domain.Status(q.Get("status")), Queue: q.Get("queue"), Type: q.Get("type"), Worker: q.Get("worker"), Cursor: q.Get("cursor"), Limit: 50}
	if r.URL.Path == "/api/v1/dead-letter" {
		f.Status = domain.DeadLettered
	}
	if f.Status != "" && !domain.ValidStatus(f.Status) {
		h.problem(w, r, 400, "VALIDATION_ERROR", "invalid status")
		return
	}
	if q.Has("limit") {
		n, e := strconv.Atoi(q.Get("limit"))
		if e != nil || n < 1 || n > 100 {
			h.problem(w, r, 400, "VALIDATION_ERROR", "limit must be 1..100")
			return
		}
		f.Limit = n
	}
	for _, v := range []struct {
		k   string
		dst **time.Time
	}{{"createdFrom", &f.CreatedFrom}, {"createdTo", &f.CreatedTo}} {
		if s := q.Get(v.k); s != "" {
			t, e := time.Parse(time.RFC3339, s)
			if e != nil {
				h.problem(w, r, 400, "VALIDATION_ERROR", "invalid "+v.k)
				return
			}
			*v.dst = &t
		}
	}
	if f.CreatedFrom != nil && f.CreatedTo != nil && f.CreatedFrom.After(*f.CreatedTo) {
		h.problem(w, r, 400, "VALIDATION_ERROR", "createdFrom must precede createdTo")
		return
	}
	// Validate opaque cursors before reaching the repository so malformed input is a 400.
	if e := validateCursor(f.Cursor); e != nil {
		h.problem(w, r, 400, "VALIDATION_ERROR", "invalid cursor")
		return
	}
	p, e := h.Store.List(r.Context(), f)
	if e != nil {
		h.fail(w, r, e)
		return
	}
	write(w, 200, p)
}
