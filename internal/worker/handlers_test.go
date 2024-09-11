package worker

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"testing"
	"time"
)

func TestRegistry(t *testing.T) {
	r := NewRegistry()
	if e := r.Register("panic", HandlerFunc(func(context.Context, json.RawMessage) (json.RawMessage, error) { panic("demo") })); e != nil {
		t.Fatal(e)
	}
	if e := r.Register("panic", HandlerFunc(Sleep)); e == nil {
		t.Fatal("duplicate accepted")
	}
	if _, e := r.Execute(context.Background(), "missing", nil); e == nil {
		t.Fatal("missing accepted")
	}
	if _, e := r.Execute(context.Background(), "panic", nil); e == nil {
		t.Fatal("panic not recovered")
	}
}
func TestSleepCancellation(t *testing.T) {
	ctx, c := context.WithCancel(context.Background())
	c()
	start := time.Now()
	if _, e := Sleep(ctx, json.RawMessage(`{"durationMs":10000}`)); !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
	if time.Since(start) > time.Second {
		t.Fatal("cancellation slow")
	}
}
func TestUnstableDeterministic(t *testing.T) {
	for _, v := range []struct {
		random float64
		fail   bool
	}{{0.1, true}, {0.9, false}} {
		h := Unstable(func() float64 { return v.random })
		_, e := h.Execute(context.Background(), json.RawMessage(`{"failureProbability":0.5}`))
		if (e != nil) != v.fail {
			t.Fatal("random injection ignored")
		}
	}
}
func TestSafeHTTP(t *testing.T) {
	h := SafeHTTP([]string{"example.com", "127.0.0.1"})
	for _, p := range []string{`{"url":"http://127.0.0.1"}`, `{"url":"http://localhost"}`, `{"url":"file:///etc/passwd"}`, `{"url":"https://user:password@example.com"}`, `{"url":"https://example.com:22"}`, `{"url":"https://example.com","method":"POST"}`} {
		if _, e := h.Execute(context.Background(), json.RawMessage(p)); e == nil {
			t.Errorf("accepted %s", p)
		}
	}
	for _, ip := range []string{"127.0.0.1", "10.0.0.1", "169.254.169.254", "100.64.0.1", "::1", "fc00::1", "::ffff:192.168.1.1"} {
		if publicIP(net.ParseIP(ip)) {
			t.Errorf("accepted private %s", ip)
		}
	}
}
func BenchmarkHandlerRegistry(b *testing.B) {
	r := DemoRegistry(nil)
	p := json.RawMessage(`{"failureProbability":0}`)
	for b.Loop() {
		_, _ = r.Execute(context.Background(), "unstable", p)
	}
}
