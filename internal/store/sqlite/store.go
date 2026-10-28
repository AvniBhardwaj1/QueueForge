package sqlite

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"queueforge/internal/queue"

	_ "modernc.org/sqlite"
)

type Store struct {
	db *sql.DB
}

func Open(ctx context.Context, path string) (*Store, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	s := &Store{db: db}
	for _, statement := range []string{
		`PRAGMA journal_mode=WAL`,
		`PRAGMA busy_timeout=5000`,
		`PRAGMA foreign_keys=ON`,
		`CREATE TABLE IF NOT EXISTS jobs (
			id TEXT PRIMARY KEY,
			type TEXT NOT NULL,
			payload TEXT NOT NULL,
			priority INTEGER NOT NULL DEFAULT 1 CHECK(priority BETWEEN 0 AND 2),
			status TEXT NOT NULL CHECK(status IN ('queued','running','succeeded','failed','dead','cancelled')),
			attempts INTEGER NOT NULL DEFAULT 0,
			max_attempts INTEGER NOT NULL DEFAULT 5,
			run_at INTEGER NOT NULL,
			locked_at INTEGER,
			started_at INTEGER,
			last_error TEXT,
			result TEXT,
			created_at INTEGER NOT NULL,
			updated_at INTEGER NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_jobs_status_priority_run_at ON jobs(status, priority, run_at)`,
		`CREATE INDEX IF NOT EXISTS idx_jobs_created_at ON jobs(created_at)`,
	} {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("initialize sqlite: %w", err)
		}
	}
	return s, nil
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) Insert(ctx context.Context, job *queue.Job) error {
	if job.ID == "" {
		id, err := newID()
		if err != nil {
			return fmt.Errorf("generate job id: %w", err)
		}
		job.ID = id
	}
	now := time.Now().UTC()
	if job.CreatedAt.IsZero() {
		job.CreatedAt = now
	}
	if job.UpdatedAt.IsZero() {
		job.UpdatedAt = now
	}
	if job.Status == "" {
		job.Status = queue.StatusQueued
	}
	if job.MaxAttempts < 1 {
		job.MaxAttempts = 5
	}
	if job.RunAt.IsZero() {
		job.RunAt = now
	}
	payload := job.Payload
	if len(payload) == 0 {
		payload = json.RawMessage(`{}`)
		job.Payload = payload
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO jobs
		(id,type,payload,priority,status,attempts,max_attempts,run_at,created_at,updated_at)
		VALUES(?,?,?,?,?,?,?,?,?,?)`,
		job.ID, job.Type, string(payload), job.Priority, job.Status, job.Attempts, job.MaxAttempts,
		job.RunAt.UnixNano(), job.CreatedAt.UnixNano(), job.UpdatedAt.UnixNano())
	if err != nil {
		return fmt.Errorf("insert job: %w", err)
	}
	return nil
}

const jobColumns = `id,type,payload,priority,status,attempts,max_attempts,run_at,locked_at,last_error,result,created_at,updated_at`

func scanJob(row interface{ Scan(...any) error }) (*queue.Job, error) {
	var job queue.Job
	var payload string
	var result, lastError sql.NullString
	var lockedAt sql.NullInt64
	var runAt, createdAt, updatedAt int64
	err := row.Scan(&job.ID, &job.Type, &payload, &job.Priority, &job.Status, &job.Attempts,
		&job.MaxAttempts, &runAt, &lockedAt, &lastError, &result, &createdAt, &updatedAt)
	if err != nil {
		return nil, err
	}
	job.Payload = json.RawMessage(payload)
	job.RunAt = time.Unix(0, runAt).UTC()
	job.CreatedAt = time.Unix(0, createdAt).UTC()
	job.UpdatedAt = time.Unix(0, updatedAt).UTC()
	if lockedAt.Valid {
		t := time.Unix(0, lockedAt.Int64).UTC()
		job.LockedAt = &t
	}
	if lastError.Valid {
		job.LastError = lastError.String
	}
	if result.Valid {
		job.Result = json.RawMessage(result.String)
	}
	return &job, nil
}

func (s *Store) Get(ctx context.Context, id string) (*queue.Job, error) {
	job, err := scanJob(s.db.QueryRowContext(ctx, `SELECT `+jobColumns+` FROM jobs WHERE id=?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, queue.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get job: %w", err)
	}
	return job, nil
}

func (s *Store) List(ctx context.Context, filter queue.Filter) ([]queue.Job, error) {
	var where []string
	var args []any
	if filter.Status != "" {
		where, args = append(where, "status=?"), append(args, filter.Status)
	}
	if filter.Type != "" {
		where, args = append(where, "type=?"), append(args, filter.Type)
	}
	query := `SELECT ` + jobColumns + ` FROM jobs`
	if len(where) > 0 {
		query += ` WHERE ` + strings.Join(where, " AND ")
	}
	query += ` ORDER BY created_at DESC LIMIT ? OFFSET ?`
	args = append(args, filter.Limit, filter.Offset)
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list jobs: %w", err)
	}
	defer rows.Close()
	jobs := make([]queue.Job, 0)
	for rows.Next() {
		job, err := scanJob(rows)
		if err != nil {
			return nil, fmt.Errorf("scan job: %w", err)
		}
		jobs = append(jobs, *job)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate jobs: %w", err)
	}
	return jobs, nil
}

func (s *Store) Claim(ctx context.Context) (*queue.Job, error) {
	// A single UPDATE ... RETURNING both selects and locks the job, so concurrent dispatchers cannot claim it twice.
	now := time.Now().UTC().UnixNano()
	job, err := scanJob(s.db.QueryRowContext(ctx, `UPDATE jobs
		SET status='running', locked_at=?, started_at=?, attempts=attempts+1, updated_at=?
		WHERE id=(SELECT id FROM jobs
			WHERE status='queued' AND run_at<=?
			ORDER BY priority ASC, created_at ASC LIMIT 1)
		RETURNING `+jobColumns, now, now, now, now))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("claim job: %w", err)
	}
	return job, nil
}

func (s *Store) Heartbeat(ctx context.Context, id string) error {
	now := time.Now().UTC().UnixNano()
	res, err := s.db.ExecContext(ctx, `UPDATE jobs SET locked_at=? WHERE id=? AND status='running'`, now, id)
	return updateResult(res, err, "heartbeat job")
}

func (s *Store) Complete(ctx context.Context, id string, result []byte) error {
	if len(result) == 0 || !json.Valid(result) {
		result = []byte(`null`)
	}
	res, err := s.db.ExecContext(ctx, `UPDATE jobs SET status='succeeded', result=?, last_error=NULL, updated_at=? WHERE id=? AND status='running'`,
		string(result), time.Now().UTC().UnixNano(), id)
	return updateResult(res, err, "complete job")
}

func (s *Store) Fail(ctx context.Context, id, message string, runAt time.Time, dead bool) error {
	status := queue.StatusQueued
	if dead {
		status = queue.StatusDead
	}
	res, err := s.db.ExecContext(ctx, `UPDATE jobs SET status=?, run_at=?, locked_at=NULL, started_at=NULL, last_error=?, updated_at=? WHERE id=? AND status='running'`,
		status, runAt.UTC().UnixNano(), message, time.Now().UTC().UnixNano(), id)
	return updateResult(res, err, "fail job")
}

func updateResult(result sql.Result, err error, operation string) error {
	if err != nil {
		return fmt.Errorf("%s: %w", operation, err)
	}
	n, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("%s rows affected: %w", operation, err)
	}
	if n == 0 {
		return queue.ErrNotFound
	}
	return nil
}

func (s *Store) Retry(ctx context.Context, id string) (*queue.Job, error) {
	now := time.Now().UTC()
	res, err := s.db.ExecContext(ctx, `UPDATE jobs SET status='queued', attempts=0, run_at=?, locked_at=NULL, started_at=NULL, last_error=NULL, result=NULL, updated_at=?
		WHERE id=? AND status IN ('failed','dead')`, now.UnixNano(), now.UnixNano(), id)
	if err != nil {
		return nil, fmt.Errorf("retry job: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return nil, fmt.Errorf("retry job rows affected: %w", err)
	}
	if n == 0 {
		_, getErr := s.Get(ctx, id)
		if getErr != nil {
			return nil, getErr
		}
		return nil, queue.ErrNotRetryable
	}
	return s.Get(ctx, id)
}

func (s *Store) Cancel(ctx context.Context, id string) error {
	res, err := s.db.ExecContext(ctx, `UPDATE jobs SET status='cancelled', updated_at=? WHERE id=? AND status='queued'`,
		time.Now().UTC().UnixNano(), id)
	if err != nil {
		return fmt.Errorf("cancel job: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("cancel job rows affected: %w", err)
	}
	if n > 0 {
		return nil
	}
	var exists int
	if err := s.db.QueryRowContext(ctx, `SELECT 1 FROM jobs WHERE id=?`, id).Scan(&exists); errors.Is(err, sql.ErrNoRows) {
		return queue.ErrNotFound
	} else if err != nil {
		return fmt.Errorf("check job for cancellation: %w", err)
	}
	return queue.ErrNotCancellable
}

func (s *Store) Reap(ctx context.Context, staleAfter time.Duration) (int64, error) {
	now := time.Now().UTC()
	res, err := s.db.ExecContext(ctx, `UPDATE jobs SET status='queued', locked_at=NULL, started_at=NULL, run_at=?, updated_at=?
		WHERE status='running' AND locked_at < ?`, now.UnixNano(), now.UnixNano(), now.Add(-staleAfter).UnixNano())
	if err != nil {
		return 0, fmt.Errorf("reap stale jobs: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("reap rows affected: %w", err)
	}
	return n, nil
}

func (s *Store) RequeueRunning(ctx context.Context) error {
	now := time.Now().UTC().UnixNano()
	_, err := s.db.ExecContext(ctx, `UPDATE jobs SET status='queued', locked_at=NULL, started_at=NULL, run_at=?, updated_at=? WHERE status='running'`, now, now)
	if err != nil {
		return fmt.Errorf("requeue running jobs: %w", err)
	}
	return nil
}

func (s *Store) Stats(ctx context.Context) (*queue.Stats, error) {
	stats := &queue.Stats{Counts: make(map[queue.Status]int)}
	rows, err := s.db.QueryContext(ctx, `SELECT status,COUNT(*) FROM jobs GROUP BY status`)
	if err != nil {
		return nil, fmt.Errorf("query job counts: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var status queue.Status
		var count int
		if err := rows.Scan(&status, &count); err != nil {
			return nil, fmt.Errorf("scan job counts: %w", err)
		}
		stats.Counts[status] = count
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate job counts: %w", err)
	}
	err = s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM jobs WHERE created_at>=?`, time.Now().UTC().Add(-time.Minute).UnixNano()).Scan(&stats.JobsPerMinute)
	if err != nil {
		return nil, fmt.Errorf("query job rate: %w", err)
	}
	err = s.db.QueryRowContext(ctx, `SELECT COALESCE(AVG((updated_at-started_at)/1000000.0),0) FROM jobs
		WHERE status='succeeded' AND started_at IS NOT NULL`).Scan(&stats.AverageDurationMS)
	if err != nil {
		return nil, fmt.Errorf("query average duration: %w", err)
	}
	return stats, nil
}

func newID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	encoded := hex.EncodeToString(b[:])
	return encoded[:8] + "-" + encoded[8:12] + "-" + encoded[12:16] + "-" + encoded[16:20] + "-" + encoded[20:], nil
}
