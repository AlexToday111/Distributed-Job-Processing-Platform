package worker

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"jobplatform/internal/config"
	"jobplatform/internal/observability"
	"jobplatform/internal/transport/pb"
)

type active struct {
	job    *pb.Assignment
	cancel context.CancelFunc
	ctx    context.Context
}
type Worker struct {
	Client   pb.WorkerServiceClient
	Config   config.Config
	Registry *Registry
	Metrics  *observability.Metrics
	Log      *slog.Logger
	mu       sync.Mutex
	active   map[string]*active
	ready    atomic.Bool
	draining atomic.Bool
}

func (w *Worker) Ready() bool { return w.ready.Load() && !w.draining.Load() }
func (w *Worker) rpcContext(ctx context.Context) (context.Context, context.CancelFunc) {
	ctx, c := context.WithTimeout(ctx, w.Config.RPCTimeout)
	if w.Config.WorkerToken != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+w.Config.WorkerToken)
	}
	return ctx, c
}
func (w *Worker) register(ctx context.Context) error {
	host, _ := os.Hostname()
	op, c := w.rpcContext(ctx)
	defer c()
	_, e := w.Client.RegisterWorker(op, &pb.RegisterRequest{WorkerId: w.Config.WorkerID, Hostname: host, Queues: w.Config.Queues, Concurrency: int32(w.Config.Concurrency + w.Config.QueueSize), Version: "dev"})
	w.ready.Store(e == nil)
	return e
}
func (w *Worker) heartbeat(ctx context.Context) {
	w.mu.Lock()
	leases := make([]*pb.ActiveLease, 0, len(w.active))
	for _, a := range w.active {
		leases = append(leases, &pb.ActiveLease{JobId: a.job.Id, LeaseToken: a.job.LeaseToken})
	}
	w.mu.Unlock()
	op, c := w.rpcContext(ctx)
	res, e := w.Client.Heartbeat(op, &pb.HeartbeatRequest{WorkerId: w.Config.WorkerID, Active: leases})
	c()
	w.ready.Store(e == nil)
	if e != nil {
		w.Metrics.Event("heartbeat_failure", 1)
		w.Log.Warn("heartbeat failed", "worker_id", w.Config.WorkerID, "error", e)
		if status.Code(e) == codes.NotFound {
			_ = w.register(ctx)
		}
		return
	}
	w.Metrics.Event("heartbeat", 1)
	w.mu.Lock()
	for _, lease := range res.RejectedLeases {
		if a := w.active[lease.LeaseToken]; a != nil {
			a.cancel()
		}
	}
	w.mu.Unlock()
}
func (w *Worker) Run(stop context.Context) error {
	workCtx, cancelAll := context.WithCancel(context.Background())
	defer cancelAll()
	w.active = map[string]*active{}
	queue := make(chan *active, w.Config.QueueSize)
	var pool sync.WaitGroup
	for i := 0; i < w.Config.Concurrency; i++ {
		pool.Go(func() {
			for a := range queue {
				w.execute(workCtx, a)
			}
		})
	}
	hbCtx, stopHB := context.WithCancel(workCtx)
	var hb sync.WaitGroup
	hb.Go(func() {
		t := time.NewTicker(w.Config.HeartbeatInterval)
		defer t.Stop()
		for {
			select {
			case <-hbCtx.Done():
				return
			case <-t.C:
				w.heartbeat(hbCtx)
			}
		}
	})
	t := time.NewTicker(w.Config.PollInterval)
	defer t.Stop()
	for stop.Err() == nil {
		if !w.ready.Load() {
			if e := w.register(stop); e != nil {
				w.Log.Warn("registration failed", "error", e)
				select {
				case <-stop.Done():
				case <-t.C:
				}
				continue
			}
		}
		w.mu.Lock()
		slots := w.Config.Concurrency + w.Config.QueueSize - len(w.active)
		w.mu.Unlock()
		if slots > 0 {
			op, c := w.rpcContext(stop)
			res, e := w.Client.Claim(op, &pb.ClaimRequest{WorkerId: w.Config.WorkerID, AvailableSlots: int32(slots)})
			c()
			if e != nil {
				w.ready.Store(false)
				w.Log.Warn("claim failed", "error", e)
			} else {
				for _, j := range res.Jobs {
					ctx, c := context.WithCancel(workCtx)
					a := &active{job: j, cancel: c, ctx: ctx}
					w.mu.Lock()
					w.active[j.LeaseToken] = a
					w.Metrics.Active.Set(float64(len(w.active)))
					w.mu.Unlock()
					select {
					case queue <- a:
					case <-stop.Done():
						queue <- a
					}
				}
			}
		}
		select {
		case <-stop.Done():
		case <-t.C:
		}
	}
	w.draining.Store(true)
	w.ready.Store(false)
	close(queue)
	done := make(chan struct{})
	go func() { pool.Wait(); close(done) }()
	timer := time.NewTimer(w.Config.ShutdownTimeout)
	defer timer.Stop()
	select {
	case <-done:
	case <-timer.C:
		cancelAll()
		<-done
	}
	stopHB()
	hb.Wait()
	return nil
}
func (w *Worker) execute(workCtx context.Context, a *active) {
	defer func() {
		a.cancel()
		w.mu.Lock()
		delete(w.active, a.job.LeaseToken)
		w.Metrics.Active.Set(float64(len(w.active)))
		w.mu.Unlock()
	}()
	parent := otel.GetTextMapPropagator().Extract(a.ctx, propagation.MapCarrier{"traceparent": a.job.TraceParent, "tracestate": a.job.TraceState})
	ctx, span := otel.Tracer("worker").Start(parent, "job.execute")
	defer span.End()
	ctx, c := context.WithTimeout(ctx, time.Duration(a.job.TimeoutSeconds)*time.Second)
	start := time.Now()
	result, e := w.Registry.Execute(ctx, a.job.Type, a.job.Payload)
	c()
	w.Metrics.Execution.Observe(time.Since(start).Seconds())
	if e == nil && !json.Valid(result) {
		e = status.Error(codes.Internal, "handler returned invalid JSON")
	}
	in := &pb.CompleteRequest{WorkerId: w.Config.WorkerID, JobId: a.job.Id, LeaseToken: a.job.LeaseToken, Succeeded: e == nil, Result: result}
	if e != nil {
		in.Error = e.Error()
		if len(in.Error) > 4096 {
			in.Error = in.Error[:4096]
		}
		in.Result = nil
		span.RecordError(e)
	}
	w.Log.Info("handler finished", "worker_id", w.Config.WorkerID, "job_id", a.job.Id, "attempt", a.job.Attempt, "queue", a.job.Queue, "job_type", a.job.Type, "request_id", a.job.RequestId, "duration", time.Since(start), "error", e)
	// Keep completion in memory and keep renewing its lease until durable acknowledgement.
	for {
		op, c := w.rpcContext(workCtx)
		_, err := w.Client.Complete(op, in)
		c()
		if err == nil {
			return
		}
		code := status.Code(err)
		if code == codes.FailedPrecondition || code == codes.NotFound || code == codes.InvalidArgument {
			return
		}
		w.Log.Warn("completion retry", "job_id", a.job.Id, "error", err)
		timer := time.NewTimer(w.Config.PollInterval)
		select {
		case <-workCtx.Done():
			timer.Stop()
			return
		case <-a.ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}
