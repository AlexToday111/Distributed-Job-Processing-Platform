package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	DatabaseURL, HTTPAddr, GRPCAddr, SchedulerAddr, WorkerID, LogLevel, OTLPEndpoint, APIKey, WorkerToken, PprofAddr   string
	Queues, HTTPAllowlist                                                                                              []string
	Concurrency, QueueSize, BatchSize, DBMaxConns                                                                      int
	PollInterval, LeaseDuration, HeartbeatInterval, HeartbeatTimeout, ShutdownTimeout, RPCTimeout, RetryBase, RetryMax time.Duration
}

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
func Load(role string) (Config, error) {
	c := Config{DatabaseURL: env("DATABASE_URL", "postgres://jobs:jobs@localhost:5432/jobs?sslmode=disable"), HTTPAddr: env("HTTP_ADDR", ":8080"), GRPCAddr: env("GRPC_ADDR", ":9090"), SchedulerAddr: env("SCHEDULER_ADDR", "localhost:9090"), WorkerID: env("WORKER_ID", ""), LogLevel: env("LOG_LEVEL", "info"), OTLPEndpoint: os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"), APIKey: env("API_KEY", "local-demo-api-key"), WorkerToken: env("WORKER_TOKEN", "local-demo-worker-token"), PprofAddr: os.Getenv("PPROF_ADDR"), Queues: strings.Split(env("WORKER_QUEUES", "default"), ","), HTTPAllowlist: strings.FieldsFunc(env("HTTP_ALLOWLIST", "example.com"), func(r rune) bool { return r == ',' })}
	for _, v := range []struct {
		k             string
		dst           *int
		def, min, max int
	}{{"WORKER_CONCURRENCY", &c.Concurrency, 4, 1, 1024}, {"WORKER_QUEUE_SIZE", &c.QueueSize, 4, 0, 1024}, {"SCHEDULER_BATCH_SIZE", &c.BatchSize, 100, 1, 1000}, {"DB_MAX_CONNS", &c.DBMaxConns, 20, 1, 1000}} {
		n, e := strconv.Atoi(env(v.k, strconv.Itoa(v.def)))
		if e != nil || n < v.min || n > v.max {
			return c, fmt.Errorf("invalid %s", v.k)
		}
		*v.dst = n
	}
	for _, v := range []struct {
		k   string
		dst *time.Duration
		def string
	}{{"SCHEDULER_POLL_INTERVAL", &c.PollInterval, "500ms"}, {"LEASE_DURATION", &c.LeaseDuration, "30s"}, {"WORKER_HEARTBEAT_INTERVAL", &c.HeartbeatInterval, "5s"}, {"WORKER_HEARTBEAT_TIMEOUT", &c.HeartbeatTimeout, "20s"}, {"SHUTDOWN_TIMEOUT", &c.ShutdownTimeout, "30s"}, {"RPC_TIMEOUT", &c.RPCTimeout, "5s"}, {"RETRY_BASE", &c.RetryBase, "1s"}, {"RETRY_MAX", &c.RetryMax, "60s"}} {
		d, e := time.ParseDuration(env(v.k, v.def))
		if e != nil || d <= 0 {
			return c, fmt.Errorf("invalid %s", v.k)
		}
		*v.dst = d
	}
	if c.HeartbeatInterval*2 >= c.LeaseDuration || c.HeartbeatInterval*2 >= c.HeartbeatTimeout || c.RetryBase > c.RetryMax {
		return c, fmt.Errorf("heartbeat must be less than half lease/heartbeat timeout; retry base <= max")
	}
	if role == "worker" && c.WorkerID == "" {
		return c, fmt.Errorf("WORKER_ID is required")
	}
	return c, nil
}
