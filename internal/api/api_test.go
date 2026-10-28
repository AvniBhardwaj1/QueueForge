package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"queueforge/internal/queue"
	sqlitestore "queueforge/internal/store/sqlite"
	"queueforge/internal/worker"
)

func newTestAPI(t *testing.T) http.Handler {
	t.Helper()
	store, err := sqlitestore.Open(context.Background(), filepath.Join(t.TempDir(), "api.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return New(store, worker.DemoHandlers(0), nil, nil).Handler()
}

func TestCreateGetListAndValidation(t *testing.T) {
	handler := newTestAPI(t)
	request := httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewBufferString(`{"type":"echo","payload":{"message":"hello"}}`))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusCreated {
		t.Fatalf("POST status = %d, body=%s", response.Code, response.Body)
	}
	var job queue.Job
	if err := json.Unmarshal(response.Body.Bytes(), &job); err != nil {
		t.Fatal(err)
	}
	get := httptest.NewRecorder()
	handler.ServeHTTP(get, httptest.NewRequest(http.MethodGet, "/jobs/"+job.ID, nil))
	if get.Code != http.StatusOK {
		t.Fatalf("GET status = %d, body=%s", get.Code, get.Body)
	}
	list := httptest.NewRecorder()
	handler.ServeHTTP(list, httptest.NewRequest(http.MethodGet, "/jobs?status=queued", nil))
	if list.Code != http.StatusOK {
		t.Fatalf("list status = %d, body=%s", list.Code, list.Body)
	}
	bad := httptest.NewRecorder()
	handler.ServeHTTP(bad, httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewBufferString(`{"type":"unknown"}`)))
	if bad.Code != http.StatusBadRequest {
		t.Fatalf("unknown type status = %d, want 400", bad.Code)
	}
	invalidPriority := httptest.NewRecorder()
	handler.ServeHTTP(invalidPriority, httptest.NewRequest(http.MethodPost, "/jobs",
		bytes.NewBufferString(`{"type":"echo","priority":3}`)))
	if invalidPriority.Code != http.StatusBadRequest {
		t.Fatalf("invalid priority status = %d, want 400", invalidPriority.Code)
	}
	oversized := httptest.NewRecorder()
	body := `{"type":"echo","payload":"` + strings.Repeat("x", 1<<20) + `"}`
	handler.ServeHTTP(oversized, httptest.NewRequest(http.MethodPost, "/jobs", strings.NewReader(body)))
	if oversized.Code != http.StatusBadRequest {
		t.Fatalf("oversized body status = %d, want 400", oversized.Code)
	}
}

func TestCancelUnknownJobReturnsNotFound(t *testing.T) {
	handler := newTestAPI(t)
	cancel := httptest.NewRecorder()
	handler.ServeHTTP(cancel, httptest.NewRequest(http.MethodDelete, "/jobs/missing", nil))
	if cancel.Code != http.StatusNotFound {
		t.Fatalf("cancel missing status = %d, want 404", cancel.Code)
	}
}
