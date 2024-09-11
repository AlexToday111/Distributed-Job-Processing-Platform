package worker

import (
	"context"
	"encoding/json"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"io"
	"jobplatform/internal/config"
	"jobplatform/internal/observability"
	"jobplatform/internal/transport/pb"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fakeClient struct {
	mu           sync.Mutex
	jobs         []*pb.Assignment
	completed    chan *pb.CompleteRequest
	reject       atomic.Bool
	failComplete atomic.Int32
}

func (f *fakeClient) RegisterWorker(context.Context, *pb.RegisterRequest, ...grpc.CallOption) (*pb.Empty, error) {
	return &pb.Empty{}, nil
}
func (f *fakeClient) Heartbeat(ctx context.Context, in *pb.HeartbeatRequest, opts ...grpc.CallOption) (*pb.HeartbeatResponse, error) {
	res := &pb.HeartbeatResponse{}
	if f.reject.Load() {
		res.RejectedLeases = in.Active
	}
	return res, nil
}
func (f *fakeClient) Claim(ctx context.Context, in *pb.ClaimRequest, opts ...grpc.CallOption) (*pb.ClaimResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := min(int(in.AvailableSlots), len(f.jobs))
	res := &pb.ClaimResponse{Jobs: f.jobs[:n]}
	f.jobs = f.jobs[n:]
	return res, nil
}
func (f *fakeClient) Complete(ctx context.Context, in *pb.CompleteRequest, opts ...grpc.CallOption) (*pb.Empty, error) {
	if f.failComplete.Add(-1) >= 0 {
		return nil, status.Error(codes.Unavailable, "offline")
	}
	select {
	case f.completed <- in:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return &pb.Empty{}, nil
}
func assignment() *pb.Assignment {
	return &pb.Assignment{Id: uuid.NewString(), LeaseToken: uuid.NewString(), Type: "test", Payload: []byte(`{}`), TimeoutSeconds: 1}
}
func testWorker(f *fakeClient, h Handler) *Worker {
	c := config.Config{WorkerID: "test", Queues: []string{"default"}, Concurrency: 2, QueueSize: 2, RPCTimeout: time.Second, PollInterval: time.Millisecond, HeartbeatInterval: 5 * time.Millisecond, ShutdownTimeout: time.Second}
	r := NewRegistry()
	_ = r.Register("test", h)
	return &Worker{Client: f, Config: c, Registry: r, Metrics: observability.NewMetrics(), Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
}
func waitDone(t *testing.T, ch <-chan error) {
	t.Helper()
	select {
	case e := <-ch:
		if e != nil {
			t.Fatal(e)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("worker leaked or did not shutdown")
	}
}
func TestBoundedPoolAndGracefulShutdown(t *testing.T) {
	f := &fakeClient{completed: make(chan *pb.CompleteRequest, 20)}
	for i := 0; i < 20; i++ {
		f.jobs = append(f.jobs, assignment())
	}
	var running, peak atomic.Int32
	started := make(chan struct{}, 20)
	release := make(chan struct{})
	h := HandlerFunc(func(ctx context.Context, p json.RawMessage) (json.RawMessage, error) {
		n := running.Add(1)
		defer running.Add(-1)
		for old := peak.Load(); n > old && !peak.CompareAndSwap(old, n); old = peak.Load() {
		}
		started <- struct{}{}
		select {
		case <-release:
			return json.RawMessage(`{}`), nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	})
	w := testWorker(f, h)
	ctx, stop := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	for i := 0; i < 2; i++ {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("did not start")
		}
	}
	stop()
	close(release)
	waitDone(t, done)
	if peak.Load() > 2 {
		t.Fatal("unbounded execution")
	}
	if n := len(f.completed); n > 4 || n < 2 {
		t.Fatalf("accepted capacity exceeded or lost jobs: %d", n)
	}
	for len(f.completed) > 0 {
		if !(<-f.completed).Succeeded {
			t.Fatal("graceful shutdown cancelled running work")
		}
	}
}
func TestRemoteCancellation(t *testing.T) {
	f := &fakeClient{completed: make(chan *pb.CompleteRequest, 1), jobs: []*pb.Assignment{assignment()}}
	started := make(chan struct{})
	w := testWorker(f, HandlerFunc(func(ctx context.Context, p json.RawMessage) (json.RawMessage, error) {
		close(started)
		<-ctx.Done()
		return nil, ctx.Err()
	}))
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("not started")
	}
	f.reject.Store(true)
	select {
	case res := <-f.completed:
		if res.Succeeded {
			t.Fatal("cancelled job succeeded")
		}
	case <-time.After(time.Second):
		t.Fatal("cancellation not propagated")
	}
	stop()
	waitDone(t, done)
}
func TestCompletionRetryDoesNotRerunHandler(t *testing.T) {
	f := &fakeClient{completed: make(chan *pb.CompleteRequest, 1), jobs: []*pb.Assignment{assignment()}}
	f.failComplete.Store(3)
	var calls atomic.Int32
	w := testWorker(f, HandlerFunc(func(context.Context, json.RawMessage) (json.RawMessage, error) {
		calls.Add(1)
		return json.RawMessage(`{}`), nil
	}))
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	select {
	case <-f.completed:
	case <-time.After(time.Second):
		t.Fatal("completion not retried")
	}
	stop()
	waitDone(t, done)
	if calls.Load() != 1 {
		t.Fatal("handler rerun on ack failure")
	}
}
func TestExecutionTimeout(t *testing.T) {
	f := &fakeClient{completed: make(chan *pb.CompleteRequest, 1), jobs: []*pb.Assignment{assignment()}}
	w := testWorker(f, HandlerFunc(func(ctx context.Context, p json.RawMessage) (json.RawMessage, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}))
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	select {
	case res := <-f.completed:
		if res.Succeeded {
			t.Fatal("timeout succeeded")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("missing timeout")
	}
	stop()
	waitDone(t, done)
}
func BenchmarkWorkerPool(b *testing.B) {
	jobs := make(chan int, 16)
	var wg sync.WaitGroup
	var result atomic.Int64
	for i := 0; i < 4; i++ {
		wg.Go(func() {
			for range jobs {
				result.Add(1)
			}
		})
	}
	for b.Loop() {
		jobs <- 1
	}
	close(jobs)
	wg.Wait()
}
func BenchmarkJobDispatch(b *testing.B) {
	j := assignment()
	for b.Loop() {
		if _, e := proto.Marshal(j); e != nil {
			b.Fatal(e)
		}
	}
}
