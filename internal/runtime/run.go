package runtime

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"jobplatform/internal/api"
	"jobplatform/internal/config"
	"jobplatform/internal/observability"
	"jobplatform/internal/repository/postgres"
	"jobplatform/internal/scheduler"
	"jobplatform/internal/transport/pb"
	"jobplatform/internal/worker"
)

func Main(role string) {
	if e := Run(role); e != nil {
		slog.Error("process stopped", "role", role, "error", e)
		os.Exit(1)
	}
}
func server(addr string, h http.Handler) *http.Server {
	return &http.Server{Addr: addr, Handler: h, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 15 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 1 << 16}
}
func Run(role string) error {
	c, e := config.Load(role)
	if e != nil {
		return e
	}
	log := observability.Logger(c.LogLevel)
	slog.SetDefault(log)
	m := observability.NewMetrics()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	traceCtx, cancel := context.WithTimeout(ctx, c.RPCTimeout)
	shutdownTrace, e := observability.Tracing(traceCtx, role, c.OTLPEndpoint)
	cancel()
	if e != nil {
		return e
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), c.RPCTimeout)
		defer cancel()
		if e := shutdownTrace(ctx); e != nil {
			log.Warn("tracing shutdown", "error", e)
		}
	}()
	errorsCh := make(chan error, 4)
	var servers []*http.Server
	var wg sync.WaitGroup
	startHTTP := func(s *http.Server) {
		servers = append(servers, s)
		wg.Go(func() {
			log.Info("HTTP listening", "role", role, "addr", s.Addr)
			if e := s.ListenAndServe(); e != nil && !errors.Is(e, http.ErrServerClosed) {
				errorsCh <- e
			}
		})
	}
	if c.PprofAddr != "" {
		host, _, e := net.SplitHostPort(c.PprofAddr)
		if e != nil || !net.ParseIP(host).IsLoopback() {
			return fmt.Errorf("PPROF_ADDR must use an explicit loopback IP")
		}
		mux := http.NewServeMux()
		mux.HandleFunc("/debug/pprof/", pprof.Index)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
		mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		startHTTP(server(c.PprofAddr, mux))
	}
	var grpcServer *grpc.Server
	var runDone chan error
	switch role {
	case "api", "scheduler":
		pool, e := postgres.Open(ctx, c.DatabaseURL, c.DBMaxConns)
		if e != nil {
			return e
		}
		defer pool.Close()
		op, cancel := context.WithTimeout(ctx, c.RPCTimeout)
		e = pool.Ping(op)
		cancel()
		if e != nil {
			return fmt.Errorf("database ping: %w", e)
		}
		repo := &postgres.Repository{Pool: pool, Lease: c.LeaseDuration, HeartbeatTimeout: c.HeartbeatTimeout, RetryBase: c.RetryBase, RetryMax: c.RetryMax, Batch: c.BatchSize}
		if role == "api" {
			h := &api.Handler{Store: repo, Metrics: m, Log: log, Key: c.APIKey, Ping: pool.Ping}
			startHTTP(server(c.HTTPAddr, h.Routes()))
		} else {
			s := &scheduler.Service{Repo: repo, Metrics: m, Log: log, Token: c.WorkerToken}
			grpcServer = grpc.NewServer(grpc.StatsHandler(otelgrpc.NewServerHandler()), grpc.UnaryInterceptor(s.Interceptor), grpc.MaxRecvMsgSize(2<<20))
			pb.RegisterWorkerServiceServer(grpcServer, s)
			healthpb.RegisterHealthServer(grpcServer, health.NewServer())
			listener, e := net.Listen("tcp", c.GRPCAddr)
			if e != nil {
				return e
			}
			wg.Go(func() {
				log.Info("gRPC listening", "addr", c.GRPCAddr)
				if e := grpcServer.Serve(listener); e != nil {
					errorsCh <- e
				}
			})
			wg.Go(func() { s.Run(ctx, c.PollInterval, c.RPCTimeout) })
			startHTTP(server(c.HTTPAddr, healthRoutes(m, pool.Ping)))
		}
	case "worker":
		conn, e := grpc.NewClient(c.SchedulerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithStatsHandler(otelgrpc.NewClientHandler()), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(16<<20)))
		if e != nil {
			return e
		}
		defer conn.Close()
		w := &worker.Worker{Client: pb.NewWorkerServiceClient(conn), Config: c, Registry: worker.DemoRegistry(c.HTTPAllowlist), Metrics: m, Log: log}
		runDone = make(chan error, 1)
		go func() { runDone <- w.Run(ctx) }()
		startHTTP(server(c.HTTPAddr, healthRoutes(m, func(context.Context) error {
			if !w.Ready() {
				return errors.New("scheduler unavailable")
			}
			return nil
		})))
	default:
		return fmt.Errorf("unknown role %s", role)
	}
	select {
	case <-ctx.Done():
	case e = <-errorsCh:
		stop()
	case e = <-runDone:
		stop()
		runDone = nil
	}
	stop()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), c.ShutdownTimeout)
	defer cancel()
	for _, s := range servers {
		if err := s.Shutdown(shutdownCtx); err != nil {
			_ = s.Close()
		}
	}
	if grpcServer != nil {
		done := make(chan struct{})
		go func() { grpcServer.GracefulStop(); close(done) }()
		select {
		case <-done:
		case <-shutdownCtx.Done():
			grpcServer.Stop()
		}
	}
	if runDone != nil {
		select {
		case err := <-runDone:
			if e == nil {
				e = err
			}
		case <-shutdownCtx.Done():
			log.Warn("worker shutdown deadline exceeded")
		}
	}
	wg.Wait()
	return e
}
func healthRoutes(m *observability.Metrics, ping func(context.Context) error) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", m.Handler())
	mux.HandleFunc("GET /health/live", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) })
	mux.HandleFunc("GET /health/ready", func(w http.ResponseWriter, r *http.Request) {
		ctx, c := context.WithTimeout(r.Context(), 2*time.Second)
		defer c()
		if ping(ctx) != nil {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(200)
	})
	return mux
}
