package config

import "testing"

func TestLoadValidation(t *testing.T) {
	t.Setenv("WORKER_ID", "test")
	if _, e := Load("worker"); e != nil {
		t.Fatal(e)
	}
	for _, c := range []struct{ k, v string }{{"WORKER_CONCURRENCY", "0"}, {"LEASE_DURATION", "0s"}, {"SCHEDULER_POLL_INTERVAL", "bad"}, {"WORKER_HEARTBEAT_INTERVAL", "25s"}} {
		t.Run(c.k, func(t *testing.T) {
			t.Setenv(c.k, c.v)
			if _, e := Load("worker"); e == nil {
				t.Fatal("invalid config accepted")
			}
		})
	}
}
