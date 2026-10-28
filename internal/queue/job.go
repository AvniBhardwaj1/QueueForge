package queue

import (
	"encoding/json"
	"errors"
	"time"
)

var (
	ErrNotFound       = errors.New("job not found")
	ErrNotCancellable = errors.New("job is not queued")
	ErrNotRetryable   = errors.New("job is not failed or dead")
)

type Status string

const (
	StatusQueued    Status = "queued"
	StatusRunning   Status = "running"
	StatusSucceeded Status = "succeeded"
	StatusFailed    Status = "failed"
	StatusDead      Status = "dead"
	StatusCancelled Status = "cancelled"
)

type Job struct {
	ID          string          `json:"id"`
	Type        string          `json:"type"`
	Payload     json.RawMessage `json:"payload"`
	Priority    int             `json:"priority"`
	Status      Status          `json:"status"`
	Attempts    int             `json:"attempts"`
	MaxAttempts int             `json:"max_attempts"`
	RunAt       time.Time       `json:"run_at"`
	LockedAt    *time.Time      `json:"locked_at,omitempty"`
	LastError   string          `json:"last_error,omitempty"`
	Result      json.RawMessage `json:"result,omitempty"`
	CreatedAt   time.Time       `json:"created_at"`
	UpdatedAt   time.Time       `json:"updated_at"`
}

type Filter struct {
	Status string
	Type   string
	Limit  int
	Offset int
}

type Stats struct {
	Counts            map[Status]int `json:"counts"`
	JobsPerMinute     int            `json:"jobs_per_minute"`
	AverageDurationMS float64        `json:"average_duration_ms"`
}
