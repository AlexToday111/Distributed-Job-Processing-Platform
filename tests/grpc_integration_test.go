//go:build integration

package tests

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
	"jobplatform/internal/domain"
	"jobplatform/internal/observability"
	"jobplatform/internal/scheduler"
	"jobplatform/internal/transport/pb"
)

func TestGRPCPayloadBudgetAndAuthentication(t *testing.T) {
	repo := repository(t)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	service := &scheduler.Service{Repo: repo, Metrics: observability.NewMetrics(), Log: log, Token: "test-token"}
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer(grpc.UnaryInterceptor(service.Interceptor))
	pb.RegisterWorkerServiceServer(server, service)
	done := make(chan struct{})
	go func() {
		defer close(done)
		if e := server.Serve(listener); e != nil {
			t.Log(e)
		}
	}()
	t.Cleanup(func() { server.Stop(); _ = listener.Close(); <-done })
	conn, e := grpc.NewClient("passthrough:///test", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }), grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(16<<20)))
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { _ = conn.Close() })
	client := pb.NewWorkerServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	registration := &pb.RegisterRequest{WorkerId: "large-worker", Hostname: "test", Queues: []string{"default"}, Concurrency: 64, Version: "test"}
	if _, e = client.RegisterWorker(ctx, registration); status.Code(e) != codes.Unauthenticated {
		t.Fatalf("unauthenticated registration: %v", e)
	}
	ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer test-token")
	if _, e = client.RegisterWorker(ctx, registration); e != nil {
		t.Fatal(e)
	}
	payload := json.RawMessage(`"` + strings.Repeat("x", (1<<20)-100) + `"`)
	for i := 0; i < 16; i++ {
		c := domain.CreateJob{Type: "sleep", Payload: payload}
		if e = c.Validate(); e != nil {
			t.Fatal(e)
		}
		if _, _, e = repo.Create(ctx, c, "test", "", ""); e != nil {
			t.Fatal(e)
		}
	}
	seen := map[string]bool{}
	for i := 0; i < 2; i++ {
		response, e := client.Claim(ctx, &pb.ClaimRequest{WorkerId: registration.WorkerId, AvailableSlots: 64})
		if e != nil {
			t.Fatal(e)
		}
		if len(response.Jobs) != 8 || proto.Size(response) > 16<<20 {
			t.Fatalf("unbounded RPC: jobs=%d bytes=%d", len(response.Jobs), proto.Size(response))
		}
		for _, job := range response.Jobs {
			if seen[job.Id] {
				t.Fatal("duplicate wire assignment")
			}
			seen[job.Id] = true
		}
	}
	// The HTTP-sized raw array expands past 1 MiB when JSONB inserts separators.
	expanded := domain.CreateJob{Type: "sleep", Payload: json.RawMessage("[" + strings.TrimSuffix(strings.Repeat("0,", 400000), ",") + "]")}
	if e = expanded.Validate(); e != nil {
		t.Fatal(e)
	}
	if _, _, e = repo.Create(ctx, expanded, "test", "", ""); !errors.Is(e, domain.ErrPayloadTooLarge) {
		t.Fatalf("normalized payload overflow: %v", e)
	}
}
