package domain

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"
)

type Status string

const (
	Scheduled    Status = "SCHEDULED"
	Ready        Status = "READY"
	Running      Status = "RUNNING"
	Succeeded    Status = "SUCCEEDED"
	RetryWait    Status = "RETRY_WAIT"
	DeadLettered Status = "DEAD_LETTERED"
	Cancelled    Status = "CANCELLED"
)

var ErrNotFound = errors.New("job not found")
var ErrConflict = errors.New("invalid state or expired lease")
var ErrPayloadTooLarge = errors.New("normalized payload exceeds 1 MiB")
var ErrInvalidPayload = errors.New("payload contains numbers outside PostgreSQL JSON limits")
var transitions = map[Status]map[Status]bool{
	Scheduled: {Ready: true, Cancelled: true}, Ready: {Running: true, Cancelled: true},
	Running:   {Succeeded: true, RetryWait: true, DeadLettered: true, Cancelled: true},
	RetryWait: {Ready: true, Cancelled: true}, DeadLettered: {Ready: true},
}

func CanTransition(from, to Status) bool { return transitions[from][to] }
func ValidStatus(s Status) bool {
	switch s {
	case Scheduled, Ready, Running, Succeeded, RetryWait, DeadLettered, Cancelled:
		return true
	}
	return false
}

type Job struct {
	ID             string          `json:"id"`
	Queue          string          `json:"queue"`
	Type           string          `json:"type"`
	Payload        json.RawMessage `json:"payload"`
	Status         Status          `json:"status"`
	Priority       int             `json:"priority"`
	ScheduledAt    time.Time       `json:"scheduledAt"`
	MaxAttempts    int             `json:"maxAttempts"`
	Attempts       int             `json:"attempts"`
	TimeoutSeconds int             `json:"timeoutSeconds"`
	IdempotencyKey string          `json:"idempotencyKey,omitempty"`
	CreatedAt      time.Time       `json:"createdAt"`
	UpdatedAt      time.Time       `json:"updatedAt"`
	StartedAt      *time.Time      `json:"startedAt,omitempty"`
	CompletedAt    *time.Time      `json:"completedAt,omitempty"`
	Result         json.RawMessage `json:"result,omitempty"`
	LastError      string          `json:"lastError,omitempty"`
	WorkerID       string          `json:"workerId,omitempty"`
	LeaseToken     string          `json:"-"`
	LeaseExpiresAt *time.Time      `json:"leaseExpiresAt,omitempty"`
	RequestID      string          `json:"requestId"`
	TraceParent    string          `json:"-"`
	TraceState     string          `json:"-"`
}
type CreateJob struct {
	Queue          string          `json:"queue"`
	Type           string          `json:"type"`
	Payload        json.RawMessage `json:"payload"`
	Priority       int             `json:"priority"`
	DelaySeconds   int             `json:"delaySeconds"`
	RunAt          *time.Time      `json:"runAt"`
	MaxAttempts    int             `json:"maxAttempts"`
	TimeoutSeconds int             `json:"timeoutSeconds"`
	IdempotencyKey string          `json:"idempotencyKey"`
}

func (c *CreateJob) Validate() error {
	if c.Queue == "" {
		c.Queue = "default"
	}
	if c.MaxAttempts == 0 {
		c.MaxAttempts = 3
	}
	if c.TimeoutSeconds == 0 {
		c.TimeoutSeconds = 60
	}
	if len(c.Queue) > 64 || len(c.Type) == 0 || len(c.Type) > 64 || len(c.IdempotencyKey) > 200 {
		return fmt.Errorf("queue/type/key length is invalid")
	}
	if !json.Valid(c.Payload) || string(c.Payload) == "null" {
		return fmt.Errorf("payload must be valid non-null JSON")
	}
	if c.Priority < 0 || c.Priority > 100 || c.MaxAttempts < 1 || c.MaxAttempts > 100 || c.TimeoutSeconds < 1 || c.TimeoutSeconds > 86400 || c.DelaySeconds < 0 || c.DelaySeconds > 31536000 {
		return fmt.Errorf("priority, attempts, timeout or delay is out of range")
	}
	if c.RunAt != nil && c.DelaySeconds != 0 {
		return fmt.Errorf("runAt and delaySeconds are mutually exclusive")
	}
	return nil
}

type Attempt struct {
	ID         int64      `json:"id"`
	JobID      string     `json:"jobId"`
	Number     int        `json:"attemptNumber"`
	WorkerID   string     `json:"workerId"`
	StartedAt  time.Time  `json:"startedAt"`
	FinishedAt *time.Time `json:"finishedAt,omitempty"`
	Status     string     `json:"status"`
	Error      string     `json:"error,omitempty"`
	DurationMS *int64     `json:"durationMs,omitempty"`
}
type Filter struct {
	Status                      Status
	Queue, Type, Worker, Cursor string
	CreatedFrom, CreatedTo      *time.Time
	Limit                       int
}
type Page struct {
	Jobs       []Job  `json:"jobs"`
	NextCursor string `json:"nextCursor,omitempty"`
}

// Equal jitter keeps retries bounded while avoiding a synchronized retry wave.
func Backoff(attempt int, base, cap time.Duration, random float64) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	d := float64(base) * math.Pow(2, float64(min(attempt-1, 30)))
	d = math.Min(d, float64(cap))
	random = math.Max(0, math.Min(random, 1))
	return time.Duration(d * (0.5 + random*0.5))
}
