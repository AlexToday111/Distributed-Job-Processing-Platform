package scheduler

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"jobplatform/internal/domain"
	"jobplatform/internal/observability"
	"jobplatform/internal/repository/postgres"
	"jobplatform/internal/transport/pb"
)

type Service struct {
	pb.UnimplementedWorkerServiceServer
	Repo    *postgres.Repository
	Metrics *observability.Metrics
	Log     *slog.Logger
	Token   string
}

func (s *Service) Interceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (out any, e error) {
	defer func() {
		if v := recover(); v != nil {
			s.Log.Error("RPC panic", "method", info.FullMethod, "panic", v)
			e = status.Error(codes.Internal, "internal error")
		}
		s.Metrics.GRPC.WithLabelValues(info.FullMethod, status.Code(e).String()).Inc()
	}()
	if s.Token != "" {
		md, _ := metadata.FromIncomingContext(ctx)
		v := md.Get("authorization")
		if len(v) != 1 || subtle.ConstantTimeCompare([]byte(v[0]), []byte("Bearer "+s.Token)) != 1 {
			return nil, status.Error(codes.Unauthenticated, "worker token required")
		}
	}
	return handler(ctx, req)
}
func rpcError(e error) error {
	if e == nil {
		return nil
	}
	switch {
	case errors.Is(e, domain.ErrNotFound):
		return status.Error(codes.NotFound, "worker/job not found")
	case errors.Is(e, domain.ErrConflict):
		return status.Error(codes.FailedPrecondition, "lease no longer valid")
	case errors.Is(e, context.Canceled):
		return status.Error(codes.Canceled, "cancelled")
	case errors.Is(e, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, "deadline exceeded")
	default:
		return status.Error(codes.Unavailable, "storage unavailable")
	}
}
func validID(id string) bool { _, e := uuid.Parse(id); return e == nil }
func (s *Service) RegisterWorker(ctx context.Context, in *pb.RegisterRequest) (*pb.Empty, error) {
	if in.WorkerId == "" || len(in.WorkerId) > 128 || len(in.Hostname) > 255 || len(in.Version) > 64 || in.Concurrency < 1 || in.Concurrency > 2048 || len(in.Queues) < 1 || len(in.Queues) > 32 {
		return nil, status.Error(codes.InvalidArgument, "invalid worker metadata")
	}
	for _, q := range in.Queues {
		if len(q) == 0 || len(q) > 64 {
			return nil, status.Error(codes.InvalidArgument, "invalid queue")
		}
	}
	e := s.Repo.Register(ctx, in.WorkerId, in.Hostname, in.Queues, int(in.Concurrency), in.Version)
	if e != nil {
		s.Log.Error("register failed", "worker_id", in.WorkerId, "error", e)
	}
	return &pb.Empty{}, rpcError(e)
}
func (s *Service) Heartbeat(ctx context.Context, in *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	if in.WorkerId == "" || len(in.Active) > 2048 {
		return nil, status.Error(codes.InvalidArgument, "invalid heartbeat")
	}
	leases := make([]postgres.Lease, 0, len(in.Active))
	for _, a := range in.Active {
		if !validID(a.JobId) || !validID(a.LeaseToken) {
			return nil, status.Error(codes.InvalidArgument, "invalid lease")
		}
		leases = append(leases, postgres.Lease{ID: a.JobId, Token: a.LeaseToken})
	}
	rejected, e := s.Repo.Heartbeat(ctx, in.WorkerId, leases)
	event := "heartbeat"
	if e != nil {
		event = "heartbeat_failure"
	}
	s.Metrics.Event(event, 1)
	out := &pb.HeartbeatResponse{}
	for _, a := range rejected {
		out.RejectedLeases = append(out.RejectedLeases, &pb.ActiveLease{JobId: a.ID, LeaseToken: a.Token})
	}
	return out, rpcError(e)
}
func (s *Service) Claim(ctx context.Context, in *pb.ClaimRequest) (*pb.ClaimResponse, error) {
	if in.WorkerId == "" || in.AvailableSlots < 0 || in.AvailableSlots > 2048 {
		return nil, status.Error(codes.InvalidArgument, "invalid capacity")
	}
	start := time.Now()
	// Eight normalized 1 MiB payloads fit below the client's 16 MiB RPC limit.
	// Cap before claiming so an oversized response cannot consume unseen leases.
	jobs, e := s.Repo.Claim(ctx, in.WorkerId, min(int(in.AvailableSlots), 8))
	s.Metrics.Claim.Observe(time.Since(start).Seconds())
	if e != nil {
		return nil, rpcError(e)
	}
	out := &pb.ClaimResponse{}
	for _, j := range jobs {
		parent := otel.GetTextMapPropagator().Extract(ctx, propagation.MapCarrier{"traceparent": j.TraceParent, "tracestate": j.TraceState})
		parent, span := otel.Tracer("scheduler").Start(parent, "job.dispatch")
		carrier := propagation.MapCarrier{}
		otel.GetTextMapPropagator().Inject(parent, carrier)
		span.End()
		out.Jobs = append(out.Jobs, &pb.Assignment{Id: j.ID, Type: j.Type, Payload: j.Payload, Queue: j.Queue, Attempt: int32(j.Attempts), TimeoutSeconds: int32(j.TimeoutSeconds), LeaseToken: j.LeaseToken, RequestId: j.RequestID, TraceParent: carrier.Get("traceparent"), TraceState: carrier.Get("tracestate")})
		s.Metrics.Event("started", 1)
		s.Metrics.Event("claimed", 1)
		s.Metrics.Wait.Observe(max(0, time.Since(j.ScheduledAt).Seconds()))
		s.Log.Info("job dispatched", "job_id", j.ID, "worker_id", in.WorkerId, "attempt", j.Attempts, "queue", j.Queue, "job_type", j.Type, "request_id", j.RequestID)
	}
	return out, nil
}
func (s *Service) Complete(ctx context.Context, in *pb.CompleteRequest) (*pb.Empty, error) {
	if !validID(in.JobId) || !validID(in.LeaseToken) || in.WorkerId == "" || len(in.Result) > 1<<20 || len(in.Error) > 4096 {
		return nil, status.Error(codes.InvalidArgument, "invalid completion")
	}
	if in.Succeeded && !json.Valid(in.Result) {
		return nil, status.Error(codes.InvalidArgument, "result must be JSON")
	}
	state, e := s.Repo.Complete(ctx, in.WorkerId, in.JobId, in.LeaseToken, in.Succeeded, in.Result, in.Error)
	if e != nil {
		return nil, rpcError(e)
	}
	if state != "" {
		if in.Succeeded {
			s.Metrics.Event("succeeded", 1)
		} else {
			s.Metrics.Event("failed", 1)
		}
		if state == domain.RetryWait {
			s.Metrics.Event("retried", 1)
		}
		if state == domain.DeadLettered {
			s.Metrics.Event("dead_lettered", 1)
		}
	}
	s.Log.Info("completion acknowledged", "job_id", in.JobId, "worker_id", in.WorkerId, "status", state)
	return &pb.Empty{}, nil
}
func (s *Service) Run(ctx context.Context, interval, timeout time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			op, c := context.WithTimeout(ctx, timeout)
			counts, e := s.Repo.Tick(op)
			if e != nil {
				s.Log.Error("scheduler tick failed", "error", e)
				counts = nil
			}
			for state, n := range counts {
				s.Log.Warn("expired leases recovered", "status", state, "count", n)
				s.Metrics.Events.WithLabelValues("lease_expired").Add(float64(n))
				s.Metrics.Event("failed", float64(n))
				if state == domain.RetryWait {
					s.Metrics.Event("retried", float64(n))
				}
				if state == domain.DeadLettered {
					s.Metrics.Event("dead_lettered", float64(n))
				}
			}
			if e := s.refresh(op); e != nil {
				s.Log.Error("metrics snapshot failed", "error", e)
			}
			c()
		}
	}
}
func (s *Service) refresh(ctx context.Context) error {
	rows, e := s.Repo.Pool.Query(ctx, `SELECT status,count(*) FROM jobs GROUP BY status`)
	if e != nil {
		return fmt.Errorf("job counts: %w", e)
	}
	counts := map[string]float64{}
	for rows.Next() {
		var st string
		var n float64
		if e := rows.Scan(&st, &n); e != nil {
			rows.Close()
			return fmt.Errorf("job count row: %w", e)
		}
		counts[st] = n
	}
	rows.Close()
	if e := rows.Err(); e != nil {
		return fmt.Errorf("job count rows: %w", e)
	}
	var registered, healthy float64
	if e := s.Repo.Pool.QueryRow(ctx, `SELECT count(*),count(*) FILTER(WHERE last_heartbeat>now()-$1::interval) FROM workers`, pgtype.Interval{Microseconds: s.Repo.HeartbeatTimeout.Microseconds(), Valid: true}).Scan(&registered, &healthy); e != nil {
		return fmt.Errorf("worker counts: %w", e)
	}
	for _, st := range []domain.Status{domain.Ready, domain.Running, domain.Scheduled, domain.RetryWait, domain.Succeeded, domain.DeadLettered, domain.Cancelled} {
		s.Metrics.SetState(string(st), counts[string(st)])
	}
	{
		s.Metrics.Workers.WithLabelValues("registered").Set(registered)
		s.Metrics.Workers.WithLabelValues("healthy").Set(healthy)
		s.Metrics.Gauges["workers_registered"].Set(registered)
		s.Metrics.Gauges["workers_healthy"].Set(healthy)
		s.Metrics.Gauges["workers_active"].Set(healthy)
	}
	return nil
}
