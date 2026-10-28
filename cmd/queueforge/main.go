package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"queueforge/internal/api"
	"queueforge/internal/queue"
	"queueforge/internal/ratelimit"
	sqlitestore "queueforge/internal/store/sqlite"
	"queueforge/internal/worker"
)

type config struct {
	workers      int
	pollInterval time.Duration
	port         int
	dbPath       string
	jobTimeout   time.Duration
	rate         float64
	burst        int
	flakyChance  float64
}

func main() {
	cfg := loadConfig()
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	rootCtx, stopSignals := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopSignals()
	store, err := sqlitestore.Open(rootCtx, cfg.dbPath)
	if err != nil {
		logger.Error("open store", "error", err)
		os.Exit(1)
	}
	defer store.Close()
	// Recover only stale locks; active workers refresh their lock timestamp.
	if count, err := store.Reap(rootCtx, 2*time.Minute); err != nil {
		logger.Error("recover stale jobs", "error", err)
		os.Exit(1)
	} else if count > 0 {
		logger.Info("recovered stale jobs", "count", count)
	}

	go runReaper(rootCtx, store, logger)
	limiter := ratelimit.New(cfg.rate, cfg.burst, 10*time.Minute)
	go cleanupLimiter(rootCtx, limiter)
	handlers := worker.DemoHandlers(cfg.flakyChance)
	pool := worker.NewPool(store, handlers, cfg.workers, cfg.pollInterval, cfg.jobTimeout, logger)
	pool.Start()

	server := &http.Server{
		Addr:              fmt.Sprintf(":%d", cfg.port),
		Handler:           api.New(store, handlers, logger, limiter).Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	serveErr := make(chan error, 1)
	go func() {
		logger.Info("QueueForge listening", "address", server.Addr, "workers", cfg.workers, "database", cfg.dbPath)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serveErr <- err
		}
	}()

	select {
	case <-rootCtx.Done():
		logger.Info("shutdown requested")
	case err := <-serveErr:
		logger.Error("HTTP server failed", "error", err)
	}
	pool.StopClaims()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Error("HTTP shutdown", "error", err)
	}

	done := make(chan struct{})
	go func() {
		pool.Wait()
		close(done)
	}()
	select {
	case <-done:
		// The dispatcher may have claimed one final job just as claims stopped.
		ctx, dbCancel := context.WithTimeout(context.Background(), 2*time.Second)
		if err := store.RequeueRunning(ctx); err != nil {
			logger.Error("requeue unprocessed jobs", "error", err)
		}
		dbCancel()
		logger.Info("workers drained")
	case <-shutdownCtx.Done():
		logger.Warn("worker drain deadline exceeded; requeueing active jobs")
		pool.CancelWork()
		ctx, dbCancel := context.WithTimeout(context.Background(), 2*time.Second)
		if err := store.RequeueRunning(ctx); err != nil {
			logger.Error("requeue active jobs", "error", err)
		}
		dbCancel()
	}
}

func runReaper(ctx context.Context, store queue.Store, logger *slog.Logger) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			count, err := store.Reap(ctx, 2*time.Minute)
			if err != nil {
				logger.Error("reap stale jobs", "error", err)
			} else if count > 0 {
				logger.Warn("recovered stale jobs", "count", count)
			}
		}
	}
}

func cleanupLimiter(ctx context.Context, limiter *ratelimit.Limiter) {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			limiter.Cleanup()
		}
	}
}

func loadConfig() config {
	cfg := config{
		workers: envInt("WORKERS", 4), pollInterval: envDuration("POLL_INTERVAL", 200*time.Millisecond),
		port: envInt("PORT", 8080), dbPath: envString("DB_PATH", "queueforge.db"),
		jobTimeout: envDuration("JOB_TIMEOUT", 30*time.Second), rate: envFloat("RATE_LIMIT_PER_SEC", 20),
		burst: envInt("RATE_LIMIT_BURST", 40), flakyChance: envFloat("FLAKY_PROBABILITY", 0.5),
	}
	flag.IntVar(&cfg.workers, "workers", cfg.workers, "number of worker goroutines")
	flag.DurationVar(&cfg.pollInterval, "poll-interval", cfg.pollInterval, "dispatcher polling interval")
	flag.IntVar(&cfg.port, "port", cfg.port, "HTTP port")
	flag.StringVar(&cfg.dbPath, "db-path", cfg.dbPath, "SQLite database path")
	flag.DurationVar(&cfg.jobTimeout, "job-timeout", cfg.jobTimeout, "per-job execution timeout")
	flag.Float64Var(&cfg.rate, "rate-limit", cfg.rate, "requests per second per client")
	flag.IntVar(&cfg.burst, "rate-burst", cfg.burst, "request burst per client")
	flag.Float64Var(&cfg.flakyChance, "flaky-probability", cfg.flakyChance, "failure probability for flaky jobs")
	flag.Parse()
	if cfg.workers < 1 {
		cfg.workers = 4
	}
	if cfg.port < 1 || cfg.port > 65535 {
		cfg.port = 8080
	}
	if cfg.pollInterval <= 0 {
		cfg.pollInterval = 200 * time.Millisecond
	}
	if cfg.jobTimeout <= 0 {
		cfg.jobTimeout = 30 * time.Second
	}
	if cfg.rate <= 0 {
		cfg.rate = 20
	}
	if cfg.burst < 1 {
		cfg.burst = 40
	}
	if cfg.flakyChance < 0 {
		cfg.flakyChance = 0
	} else if cfg.flakyChance > 1 {
		cfg.flakyChance = 1
	}
	return cfg
}

func envString(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func envInt(key string, fallback int) int {
	value, err := strconv.Atoi(os.Getenv(key))
	if err != nil {
		return fallback
	}
	return value
}

func envFloat(key string, fallback float64) float64 {
	value, err := strconv.ParseFloat(os.Getenv(key), 64)
	if err != nil {
		return fallback
	}
	return value
}

func envDuration(key string, fallback time.Duration) time.Duration {
	value, err := time.ParseDuration(os.Getenv(key))
	if err != nil {
		return fallback
	}
	return value
}
