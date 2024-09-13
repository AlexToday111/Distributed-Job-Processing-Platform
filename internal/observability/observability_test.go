package observability

import (
	"context"
	"go.opentelemetry.io/otel"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestOTLPExporterUsesTraceEndpoint(t *testing.T) {
	paths := make(chan string, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths <- r.URL.Path
		if r.URL.Path != "/v1/traces" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/x-protobuf")
		w.WriteHeader(200)
	}))
	defer server.Close()
	oldProvider, oldPropagator := otel.GetTracerProvider(), otel.GetTextMapPropagator()
	defer func() { otel.SetTracerProvider(oldProvider); otel.SetTextMapPropagator(oldPropagator) }()
	ctx, c := context.WithTimeout(context.Background(), 5*time.Second)
	defer c()
	shutdown, e := Tracing(ctx, "test", server.URL)
	if e != nil {
		t.Fatal(e)
	}
	_, span := otel.Tracer("test").Start(ctx, "execution")
	span.End()
	if e := shutdown(ctx); e != nil {
		t.Fatal(e)
	}
	select {
	case p := <-paths:
		if p != "/v1/traces" {
			t.Fatalf("invalid OTLP path %s", p)
		}
	default:
		t.Fatal("no exported spans")
	}
}
