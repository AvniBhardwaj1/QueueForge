package queue

import (
	"context"
	"time"
)

type Store interface {
	Insert(context.Context, *Job) error
	Get(context.Context, string) (*Job, error)
	List(context.Context, Filter) ([]Job, error)
	Claim(context.Context) (*Job, error)
	Heartbeat(context.Context, string) error
	Complete(context.Context, string, []byte) error
	Fail(context.Context, string, string, time.Time, bool) error
	Retry(context.Context, string) (*Job, error)
	Cancel(context.Context, string) error
	Reap(context.Context, time.Duration) (int64, error)
	RequeueRunning(context.Context) error
	Stats(context.Context) (*Stats, error)
}
