package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand"
	"runtime/debug"
	"sync"
	"time"

	"queueforge/internal/backoff"
	"queueforge/internal/queue"
)

type Pool struct {
	store        queue.Store
	handlers     map[string]HandlerFunc
	workers      int
	pollInterval time.Duration
	jobTimeout   time.Duration
	logger       *slog.Logger
	baseBackoff  time.Duration
	maxBackoff   time.Duration
	jobs         chan *queue.Job
	stopClaims   chan struct{}
	workCtx      context.Context
	cancelWork   context.CancelFunc
	stopOnce     sync.Once
	wg           sync.WaitGroup
}

func NewPool(store queue.Store, handlers map[string]HandlerFunc, workers int, pollInterval, jobTimeout time.Duration, logger *slog.Logger) *Pool {
	if workers < 1 {
		workers = 4
	}
	if pollInterval <= 0 {
		pollInterval = 200 * time.Millisecond
	}
	if jobTimeout <= 0 {
		jobTimeout = 30 * time.Second
	}
	if logger == nil {
		logger = slog.Default()
	}
	workCtx, cancel := context.WithCancel(context.Background())
	return &Pool{
		store: store, handlers: handlers, workers: workers, pollInterval: pollInterval,
		jobTimeout: jobTimeout, logger: logger, baseBackoff: 2 * time.Second,
		maxBackoff: 5 * time.Minute, jobs: make(chan *queue.Job),
		stopClaims: make(chan struct{}), workCtx: workCtx, cancelWork: cancel,
	}
}

func (p *Pool) Start() {
	for range p.workers {
		p.wg.Add(1)
		go p.runWorker()
	}
	p.wg.Add(1)
	go p.dispatch()
}

func (p *Pool) dispatch() {
	defer p.wg.Done()
	defer close(p.jobs)
	ticker := time.NewTicker(p.pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-p.stopClaims:
			return
		default:
		}
		job, err := p.store.Claim(p.workCtx)
		if err != nil {
			p.logger.Error("claim job", "error", err)
		} else if job != nil {
			select {
			case p.jobs <- job:
			case <-p.stopClaims:
				return
			}
			continue
		}
		select {
		case <-p.stopClaims:
			return
		case <-ticker.C:
		}
	}
}

func (p *Pool) runWorker() {
	defer p.wg.Done()
	for job := range p.jobs {
		p.process(job)
	}
}

func (p *Pool) process(job *queue.Job) {
	started := time.Now()
	handler, ok := p.handlers[job.Type]
	if !ok {
		p.fail(job, fmt.Errorf("no handler registered for %q", job.Type), started)
		return
	}
	ctx, cancel := context.WithTimeout(p.workCtx, p.jobTimeout)
	defer cancel()
	heartbeatStop := make(chan struct{})
	heartbeatDone := make(chan struct{})
	go func() {
		defer close(heartbeatDone)
		p.heartbeat(job.ID, heartbeatStop)
	}()
	var result any
	var handlerErr error
	func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				handlerErr = fmt.Errorf("handler panic: %v\n%s", recovered, debug.Stack())
			}
		}()
		result, handlerErr = handler(ctx, job.Payload)
	}()
	close(heartbeatStop)
	<-heartbeatDone
	if handlerErr != nil {
		p.fail(job, handlerErr, started)
		return
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		p.fail(job, fmt.Errorf("encode handler result: %w", err), started)
		return
	}
	if err := p.store.Complete(context.Background(), job.ID, encoded); err != nil {
		p.logger.Error("complete job", "job_id", job.ID, "type", job.Type, "attempt", job.Attempts, "error", err)
		return
	}
	p.logger.Info("job completed", "job_id", job.ID, "type", job.Type, "attempt", job.Attempts, "duration", time.Since(started))
}

func (p *Pool) heartbeat(jobID string, done <-chan struct{}) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-done:
			return
		case <-p.workCtx.Done():
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			if err := p.store.Heartbeat(ctx, jobID); err != nil {
				p.logger.Error("heartbeat job", "job_id", jobID, "error", err)
			}
			cancel()
		}
	}
}

func (p *Pool) fail(job *queue.Job, cause error, started time.Time) {
	dead := job.Attempts >= job.MaxAttempts
	runAt := time.Now().UTC().Add(backoff.Delay(job.Attempts, p.baseBackoff, p.maxBackoff, rand.Float64()))
	if err := p.store.Fail(context.Background(), job.ID, cause.Error(), runAt, dead); err != nil {
		p.logger.Error("record job failure", "job_id", job.ID, "type", job.Type, "attempt", job.Attempts, "error", err, "handler_error", cause)
		return
	}
	p.logger.Warn("job failed", "job_id", job.ID, "type", job.Type, "attempt", job.Attempts, "duration", time.Since(started), "error", cause, "dead", dead)
}

// StopClaims stops new claims while allowing already claimed work to drain.
func (p *Pool) StopClaims() {
	p.stopOnce.Do(func() { close(p.stopClaims) })
}

func (p *Pool) Wait() {
	p.wg.Wait()
}

func (p *Pool) CancelWork() {
	p.cancelWork()
}

func (p *Pool) Close() {
	p.StopClaims()
	p.cancelWork()
}
