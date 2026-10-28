package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"queueforge/internal/queue"
	"queueforge/internal/ratelimit"
	"queueforge/internal/worker"
)

type API struct {
	store    queue.Store
	handlers map[string]worker.HandlerFunc
	logger   *slog.Logger
	limiter  *ratelimit.Limiter
}

func New(store queue.Store, handlers map[string]worker.HandlerFunc, logger *slog.Logger, limiter *ratelimit.Limiter) *API {
	if logger == nil {
		logger = slog.Default()
	}
	return &API{store: store, handlers: handlers, logger: logger, limiter: limiter}
}

func (a *API) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /jobs", a.createJob)
	mux.HandleFunc("GET /jobs/{id}", a.getJob)
	mux.HandleFunc("GET /jobs", a.listJobs)
	mux.HandleFunc("POST /jobs/{id}/retry", a.retryJob)
	mux.HandleFunc("DELETE /jobs/{id}", a.cancelJob)
	mux.HandleFunc("GET /stats", a.stats)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /metrics", a.metrics)
	return a.cors(a.rateLimit(a.requestLog(mux)))
}

type createRequest struct {
	Type        string          `json:"type"`
	Payload     json.RawMessage `json:"payload"`
	Priority    *int            `json:"priority,omitempty"`
	MaxAttempts *int            `json:"max_attempts,omitempty"`
	Delay       int             `json:"delay_seconds,omitempty"`
}

func (a *API) createJob(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	var input createRequest
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_json", err.Error())
		return
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		writeError(w, http.StatusBadRequest, "invalid_json", "request body must contain one JSON value")
		return
	}
	if strings.TrimSpace(input.Type) == "" {
		writeError(w, http.StatusBadRequest, "invalid_type", "type is required")
		return
	}
	if _, ok := a.handlers[input.Type]; !ok {
		writeError(w, http.StatusBadRequest, "unknown_type", "unknown job type")
		return
	}
	if len(input.Payload) == 0 {
		input.Payload = json.RawMessage(`{}`)
	}
	if !json.Valid(input.Payload) {
		writeError(w, http.StatusBadRequest, "invalid_payload", "payload must be valid JSON")
		return
	}
	priority := 1
	if input.Priority != nil {
		priority = *input.Priority
	}
	if priority < 0 || priority > 2 {
		writeError(w, http.StatusBadRequest, "invalid_priority", "priority must be 0, 1, or 2")
		return
	}
	maxAttempts := 5
	if input.MaxAttempts != nil {
		maxAttempts = *input.MaxAttempts
	}
	if maxAttempts < 1 {
		writeError(w, http.StatusBadRequest, "invalid_max_attempts", "max_attempts must be a positive integer")
		return
	}
	delaySeconds := int64(input.Delay)
	const maxDelaySeconds = int64((1<<63 - 1) / int64(time.Second))
	if delaySeconds < 0 || delaySeconds > maxDelaySeconds {
		writeError(w, http.StatusBadRequest, "invalid_delay", "delay_seconds is outside the supported range")
		return
	}
	now := time.Now().UTC()
	job := &queue.Job{
		Type: input.Type, Payload: input.Payload, Priority: priority, Status: queue.StatusQueued,
		MaxAttempts: maxAttempts, RunAt: now.Add(time.Duration(delaySeconds) * time.Second),
		CreatedAt: now, UpdatedAt: now,
	}
	if err := a.store.Insert(r.Context(), job); err != nil {
		a.logger.Error("create job", "error", err)
		writeError(w, http.StatusInternalServerError, "internal_error", "could not create job")
		return
	}
	writeJSON(w, http.StatusCreated, job)
}

func (a *API) getJob(w http.ResponseWriter, r *http.Request) {
	job, err := a.store.Get(r.Context(), r.PathValue("id"))
	if errors.Is(err, queue.ErrNotFound) {
		writeError(w, http.StatusNotFound, "not_found", "job not found")
		return
	}
	if err != nil {
		a.internalError(w, "get job", err)
		return
	}
	writeJSON(w, http.StatusOK, job)
}

func (a *API) listJobs(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	limit := 50
	offset := 0
	var err error
	if value := query.Get("limit"); value != "" {
		limit, err = strconv.Atoi(value)
		if err != nil || limit < 1 || limit > 200 {
			writeError(w, http.StatusBadRequest, "invalid_limit", "limit must be between 1 and 200")
			return
		}
	}
	if value := query.Get("offset"); value != "" {
		offset, err = strconv.Atoi(value)
		if err != nil || offset < 0 {
			writeError(w, http.StatusBadRequest, "invalid_offset", "offset must be a non-negative integer")
			return
		}
	}
	status := query.Get("status")
	if status != "" && !validStatus(status) {
		writeError(w, http.StatusBadRequest, "invalid_status", "status is not recognized")
		return
	}
	jobs, err := a.store.List(r.Context(), queue.Filter{Status: status, Type: query.Get("type"), Limit: limit, Offset: offset})
	if err != nil {
		a.internalError(w, "list jobs", err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"jobs": jobs, "limit": limit, "offset": offset})
}

func (a *API) retryJob(w http.ResponseWriter, r *http.Request) {
	job, err := a.store.Retry(r.Context(), r.PathValue("id"))
	if errors.Is(err, queue.ErrNotFound) {
		writeError(w, http.StatusNotFound, "not_found", "job not found")
		return
	}
	if errors.Is(err, queue.ErrNotRetryable) {
		writeError(w, http.StatusConflict, "not_retryable", "only failed or dead jobs can be retried")
		return
	}
	if err != nil {
		a.internalError(w, "retry job", err)
		return
	}
	writeJSON(w, http.StatusOK, job)
}

func (a *API) cancelJob(w http.ResponseWriter, r *http.Request) {
	err := a.store.Cancel(r.Context(), r.PathValue("id"))
	if errors.Is(err, queue.ErrNotFound) {
		writeError(w, http.StatusNotFound, "not_found", "job not found")
		return
	}
	if errors.Is(err, queue.ErrNotCancellable) {
		writeError(w, http.StatusConflict, "not_cancellable", "only queued jobs can be cancelled")
		return
	}
	if err != nil {
		a.internalError(w, "cancel job", err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (a *API) stats(w http.ResponseWriter, r *http.Request) {
	stats, err := a.store.Stats(r.Context())
	if err != nil {
		a.internalError(w, "get stats", err)
		return
	}
	writeJSON(w, http.StatusOK, stats)
}

func (a *API) metrics(w http.ResponseWriter, r *http.Request) {
	stats, err := a.store.Stats(r.Context())
	if err != nil {
		a.internalError(w, "get metrics", err)
		return
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	for _, status := range []queue.Status{queue.StatusQueued, queue.StatusRunning, queue.StatusSucceeded, queue.StatusDead} {
		_, _ = fmt.Fprintf(w, "queueforge_jobs{status=%q} %d\n", status, stats.Counts[status])
	}
}

func (a *API) internalError(w http.ResponseWriter, operation string, err error) {
	a.logger.Error(operation, "error", err)
	writeError(w, http.StatusInternalServerError, "internal_error", "request could not be completed")
}

func (a *API) requestLog(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		recorder := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(recorder, r)
		a.logger.Info("http request", "method", r.Method, "path", r.URL.Path, "status", recorder.status,
			"duration", time.Since(start), "remote_addr", r.RemoteAddr)
	})
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(status int) {
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

func (a *API) rateLimit(next http.Handler) http.Handler {
	if a.limiter == nil {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := strings.TrimSpace(r.Header.Get("X-API-Key"))
		if key == "" {
			host, _, err := net.SplitHostPort(r.RemoteAddr)
			if err == nil {
				key = host
			} else {
				key = r.RemoteAddr
			}
		}
		if allowed, retry := a.limiter.Allow(key); !allowed {
			seconds := int(math.Ceil(retry.Seconds()))
			if seconds < 1 {
				seconds = 1
			}
			w.Header().Set("Retry-After", strconv.Itoa(seconds))
			writeError(w, http.StatusTooManyRequests, "rate_limited", "too many requests")
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (a *API) cors(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		if origin == "http://localhost:5173" || origin == "http://127.0.0.1:5173" {
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Vary", "Origin")
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type, X-API-Key")
		}
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func validStatus(status string) bool {
	switch queue.Status(status) {
	case queue.StatusQueued, queue.StatusRunning, queue.StatusSucceeded, queue.StatusFailed, queue.StatusDead, queue.StatusCancelled:
		return true
	default:
		return false
	}
}

type apiError struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	var body apiError
	body.Error.Code, body.Error.Message = code, message
	writeJSON(w, status, body)
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(value); err != nil {
		slog.Error("encode HTTP response", "error", err)
	}
}
