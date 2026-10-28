package worker

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"queueforge/internal/queue"
	sqlitestore "queueforge/internal/store/sqlite"
)

func TestProcessSuccessAndHandlerPanicRetry(t *testing.T) {
	store, err := sqlitestore.Open(context.Background(), filepath.Join(t.TempDir(), "worker.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	handlers := map[string]HandlerFunc{
		"ok": func(context.Context, json.RawMessage) (any, error) {
			return map[string]string{"result": "done"}, nil
		},
		"panic": func(context.Context, json.RawMessage) (any, error) {
			panic("test panic")
		},
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	pool := NewPool(store, handlers, 1, time.Millisecond, time.Second, logger)

	success := &queue.Job{Type: "ok", Payload: json.RawMessage(`{}`), MaxAttempts: 1}
	if err := store.Insert(context.Background(), success); err != nil {
		t.Fatal(err)
	}
	claimed, err := store.Claim(context.Background())
	if err != nil || claimed == nil {
		t.Fatalf("claim success job: job=%v error=%v", claimed, err)
	}
	pool.process(claimed)
	finished, err := store.Get(context.Background(), success.ID)
	if err != nil {
		t.Fatal(err)
	}
	if finished.Status != queue.StatusSucceeded || string(finished.Result) != `{"result":"done"}` {
		t.Fatalf("unexpected successful job: status=%s result=%s", finished.Status, finished.Result)
	}

	failed := &queue.Job{Type: "panic", Payload: json.RawMessage(`{}`), MaxAttempts: 2}
	if err := store.Insert(context.Background(), failed); err != nil {
		t.Fatal(err)
	}
	claimed, err = store.Claim(context.Background())
	if err != nil || claimed == nil {
		t.Fatalf("claim panic job: job=%v error=%v", claimed, err)
	}
	pool.process(claimed)
	retrying, err := store.Get(context.Background(), failed.ID)
	if err != nil {
		t.Fatal(err)
	}
	if retrying.Status != queue.StatusQueued || retrying.Attempts != 1 || retrying.LastError == "" {
		t.Fatalf("panic was not safely scheduled for retry: %#v", retrying)
	}
	if !retrying.RunAt.After(time.Now()) {
		t.Fatal("retry run_at should be delayed")
	}
}
