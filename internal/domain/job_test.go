package domain

import (
	"encoding/json"
	"testing"
	"time"
)

func TestStateMachine(t *testing.T) {
	cases := []struct {
		from, to Status
		ok       bool
	}{{Ready, Running, true}, {Running, Succeeded, true}, {Running, RetryWait, true}, {Running, DeadLettered, true}, {Running, Cancelled, true}, {DeadLettered, Ready, true}, {Succeeded, Ready, false}, {Cancelled, Ready, false}, {Ready, Succeeded, false}, {Scheduled, Running, false}}
	for _, c := range cases {
		if got := CanTransition(c.from, c.to); got != c.ok {
			t.Errorf("%s -> %s = %v", c.from, c.to, got)
		}
	}
}
func TestValidation(t *testing.T) {
	good := CreateJob{Type: "sleep", Payload: json.RawMessage(`{}`)}
	if e := good.Validate(); e != nil {
		t.Fatal(e)
	}
	if good.MaxAttempts != 3 || good.TimeoutSeconds != 60 || good.Queue != "default" {
		t.Fatal("defaults")
	}
	for _, c := range []CreateJob{{Type: "sleep", Payload: json.RawMessage(`null`)}, {Type: "sleep", Payload: json.RawMessage(`{}`), MaxAttempts: -1}, {Payload: json.RawMessage(`{}`)}, {Type: "sleep", Payload: json.RawMessage(`{}`), TimeoutSeconds: 999999}, {Type: "sleep", Payload: json.RawMessage(`{}`), DelaySeconds: 1, RunAt: new(time.Time)}} {
		if c.Validate() == nil {
			t.Fatalf("accepted invalid input: %+v", c)
		}
	}
}
func TestBackoff(t *testing.T) {
	for attempt := 1; attempt < 1000; attempt++ {
		low := Backoff(attempt, time.Second, time.Minute, 0)
		high := Backoff(attempt, time.Second, time.Minute, 1)
		if low <= 0 || high > time.Minute || low > high || low*2 != high {
			t.Fatalf("invalid jitter bounds: %v %v", low, high)
		}
	}
	if Backoff(1, time.Second, time.Minute, 1) != time.Second || Backoff(2, time.Second, time.Minute, 1) != 2*time.Second {
		t.Fatal("not exponential")
	}
}
func BenchmarkRetryCalculation(b *testing.B) {
	for b.Loop() {
		_ = Backoff(8, time.Second, time.Minute, 0.7)
	}
}
