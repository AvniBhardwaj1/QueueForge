package sqlite

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"queueforge/internal/queue"
)

func testStore(t *testing.T, path string) *Store {
	t.Helper()
	store, err := Open(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	return store
}

func TestRetryAndCancelTransitions(t *testing.T) {
	store := testStore(t, filepath.Join(t.TempDir(), "jobs.db"))
	ctx := context.Background()
	job := &queue.Job{Type: "echo", Payload: json.RawMessage(`{"hello":"world"}`), Priority: 1, Status: queue.StatusDead, Attempts: 5, MaxAttempts: 5}
	if err := store.Insert(ctx, job); err != nil {
		t.Fatal(err)
	}
	retried, err := store.Retry(ctx, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if retried.Status != queue.StatusQueued || retried.Attempts != 0 {
		t.Fatalf("unexpected retried job: %#v", retried)
	}
	if err := store.Cancel(ctx, job.ID); err != nil {
		t.Fatal(err)
	}
	cancelled, err := store.Get(ctx, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if cancelled.Status != queue.StatusCancelled {
		t.Fatalf("status = %s, want cancelled", cancelled.Status)
	}
	if err := store.Cancel(ctx, "missing"); err != queue.ErrNotFound {
		t.Fatalf("cancel missing error = %v, want ErrNotFound", err)
	}
}

func TestClaimPriorityAndReap(t *testing.T) {
	store := testStore(t, filepath.Join(t.TempDir(), "priority.db"))
	ctx := context.Background()
	low := &queue.Job{Type: "echo", Payload: json.RawMessage(`{}`), Priority: 2, MaxAttempts: 3}
	high := &queue.Job{Type: "echo", Payload: json.RawMessage(`{}`), Priority: 0, MaxAttempts: 3}
	if err := store.Insert(ctx, low); err != nil {
		t.Fatal(err)
	}
	if err := store.Insert(ctx, high); err != nil {
		t.Fatal(err)
	}
	claimed, err := store.Claim(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if claimed.ID != high.ID {
		t.Fatalf("claimed %s, want highest-priority job %s", claimed.ID, high.ID)
	}
	if _, err := store.Reap(ctx, 0); err != nil {
		t.Fatal(err)
	}
	recovered, err := store.Get(ctx, high.ID)
	if err != nil {
		t.Fatal(err)
	}
	if recovered.Status != queue.StatusQueued {
		t.Fatalf("reaped job status = %s, want queued", recovered.Status)
	}
	if err := store.Cancel(ctx, low.ID); err != nil {
		t.Fatalf("cancel queued job: %v", err)
	}
}

func TestJobPersistsAfterReopen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "persist.db")
	first, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	job := &queue.Job{Type: "echo", Payload: json.RawMessage(`{"persistent":true}`), Priority: 1}
	if err := first.Insert(ctx, job); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	persisted, err := reopened.Get(ctx, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if persisted.ID != job.ID || persisted.Status != queue.StatusQueued || string(persisted.Payload) != `{"persistent":true}` {
		t.Fatalf("unexpected persisted job: %#v", persisted)
	}
}

func TestConcurrentClaimsAreUnique(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "concurrent.db")
	const jobsCount = 300
	seed := testStore(t, path)
	for i := 0; i < jobsCount; i++ {
		job := &queue.Job{Type: "echo", Payload: json.RawMessage(`{}`), Priority: i % 3, MaxAttempts: 5}
		if err := seed.Insert(ctx, job); err != nil {
			t.Fatal(err)
		}
	}

	const claimers = 8
	stores := make([]*Store, claimers)
	for i := range stores {
		store, err := Open(ctx, path)
		if err != nil {
			t.Fatal(err)
		}
		stores[i] = store
		t.Cleanup(func() {
			if err := store.Close(); err != nil {
				t.Error(err)
			}
		})
	}
	var claimed atomic.Int64
	var mu sync.Mutex
	seen := make(map[string]bool, jobsCount)
	errs := make(chan error, claimers)
	var wg sync.WaitGroup
	for _, store := range stores {
		wg.Add(1)
		go func(store *Store) {
			defer wg.Done()
			for claimed.Load() < jobsCount {
				job, err := store.Claim(ctx)
				if err != nil {
					errs <- err
					return
				}
				if job == nil {
					time.Sleep(time.Millisecond)
					continue
				}
				mu.Lock()
				if seen[job.ID] {
					errs <- fmt.Errorf("job %s claimed more than once", job.ID)
				}
				seen[job.ID] = true
				mu.Unlock()
				claimed.Add(1)
			}
		}(store)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	if got := len(seen); got != jobsCount {
		t.Fatalf("unique jobs claimed = %d, want %d", got, jobsCount)
	}
}
