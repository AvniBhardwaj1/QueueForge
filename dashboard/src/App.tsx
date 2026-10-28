import { useEffect, useState, type FormEvent } from "react";

type JobStatus =
  | "queued"
  | "running"
  | "succeeded"
  | "failed"
  | "dead"
  | "cancelled";

type Job = {
  id: string;
  type: string;
  payload: unknown;
  priority: number;
  status: JobStatus;
  attempts: number;
  max_attempts: number;
  run_at: string;
  created_at: string;
  updated_at: string;
  last_error?: string;
};

type JobStats = {
  counts: Partial<Record<JobStatus, number>>;
  jobs_per_minute: number;
  average_duration_ms: number;
};

type JobList = {
  jobs: Job[];
  limit: number;
  offset: number;
};

type APIErrorBody = {
  error?: {
    message?: string;
  };
};

const statuses: Array<"all" | JobStatus> = [
  "all",
  "queued",
  "running",
  "succeeded",
  "failed",
  "dead",
  "cancelled",
];

const statusNames: Record<JobStatus, string> = {
  queued: "Queued",
  running: "Running",
  succeeded: "Succeeded",
  failed: "Failed",
  dead: "Dead",
  cancelled: "Cancelled",
};

const payloadExamples: Record<string, string> = {
  echo: '{\n  "message": "Hello from QueueForge"\n}',
  sleep: '{\n  "seconds": 2\n}',
  flaky: '{\n  "message": "This may need a retry"\n}',
};

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(path, {
    ...init,
    headers: {
      ...(init?.body ? { "Content-Type": "application/json" } : {}),
      ...init?.headers,
    },
  });

  if (!response.ok) {
    let message = `Request failed (${response.status})`;
    try {
      const body = (await response.json()) as APIErrorBody;
      message = body.error?.message || message;
    } catch {
      // The response may be empty, for example when a server is unavailable.
    }
    throw new Error(message);
  }

  if (response.status === 204) {
    return undefined as T;
  }
  return (await response.json()) as T;
}

function formatTime(value: string): string {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "—";
  return new Intl.DateTimeFormat(undefined, {
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  }).format(date);
}

function formatDuration(value: number): string {
  if (value < 1) return "<1 ms";
  if (value < 1000) return `${Math.round(value)} ms`;
  return `${(value / 1000).toFixed(1)} s`;
}

function compactID(id: string): string {
  return id.length > 12 ? `${id.slice(0, 8)}…${id.slice(-3)}` : id;
}

function App() {
  const [jobs, setJobs] = useState<Job[]>([]);
  const [stats, setStats] = useState<JobStats | null>(null);
  const [filter, setFilter] = useState<"all" | JobStatus>("all");
  const [type, setType] = useState("echo");
  const [payload, setPayload] = useState(payloadExamples.echo);
  const [priority, setPriority] = useState(1);
  const [maxAttempts, setMaxAttempts] = useState(5);
  const [loading, setLoading] = useState(true);
  const [pending, setPending] = useState<string | null>(null);
  const [error, setError] = useState("");
  const [notice, setNotice] = useState("");
  const [lastUpdated, setLastUpdated] = useState<Date | null>(null);

  useEffect(() => {
    let active = true;
    let inFlight = false;

    const refresh = async () => {
      if (inFlight) return;
      inFlight = true;
      try {
        const statusQuery = filter === "all" ? "" : `&status=${filter}`;
        const [statsResponse, jobsResponse] = await Promise.all([
          request<JobStats>("/stats"),
          request<JobList>(`/jobs?limit=50${statusQuery}`),
        ]);
        if (!active) return;
        setStats(statsResponse);
        setJobs(jobsResponse.jobs);
        setLastUpdated(new Date());
        setError("");
      } catch (cause) {
        if (active) {
          setError(cause instanceof Error ? cause.message : "Could not load queue data.");
        }
      } finally {
        inFlight = false;
        if (active) setLoading(false);
      }
    };

    setLoading(true);
    void refresh();
    const timer = window.setInterval(() => void refresh(), 2000);
    return () => {
      active = false;
      window.clearInterval(timer);
    };
  }, [filter]);

  const createJob = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    setError("");
    setNotice("");

    let parsedPayload: unknown;
    try {
      parsedPayload = JSON.parse(payload);
    } catch {
      setError("Payload must be valid JSON.");
      return;
    }

    setPending("create");
    try {
      const job = await request<Job>("/jobs", {
        method: "POST",
        body: JSON.stringify({
          type,
          payload: parsedPayload,
          priority,
          max_attempts: maxAttempts,
        }),
      });
      setNotice(`Job ${compactID(job.id)} added to the queue.`);
      setPayload(payloadExamples[type]);
      setFilter("all");
      await refreshNow("all");
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "Could not create job.");
    } finally {
      setPending(null);
    }
  };

  const refreshNow = async (statusFilter = filter) => {
    try {
      const [nextStats, nextJobs] = await Promise.all([
        request<JobStats>("/stats"),
        request<JobList>(`/jobs?limit=50${statusFilter === "all" ? "" : `&status=${statusFilter}`}`),
      ]);
      setStats(nextStats);
      setJobs(nextJobs.jobs);
      setLastUpdated(new Date());
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "Could not refresh queue data.");
    }
  };

  const updateJob = async (job: Job, action: "retry" | "cancel") => {
    setPending(job.id);
    setError("");
    setNotice("");
    try {
      await request<void>(`/jobs/${job.id}${action === "retry" ? "/retry" : ""}`, {
        method: action === "retry" ? "POST" : "DELETE",
      });
      setNotice(action === "retry" ? "Job queued for another attempt." : "Queued job cancelled.");
      await refreshNow();
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "Could not update job.");
    } finally {
      setPending(null);
    }
  };

  const count = (status: JobStatus) => stats?.counts[status] ?? 0;

  return (
    <div className="app-shell">
      <header className="topbar">
        <a className="brand" href="/" aria-label="QueueForge dashboard">
          <span className="brand-mark" aria-hidden="true">
            <i />
            <i />
            <i />
          </span>
          <span className="brand-name">QueueForge</span>
          <span className="brand-divider" />
          <span className="brand-section">Operations</span>
        </a>
        <div className="topbar-right">
          <span className="connection-indicator">
            <span className={`connection-dot ${error ? "is-error" : ""}`} />
            {error ? "Connection issue" : "Auto-refreshing"}
          </span>
          <span className="refresh-label">
            {lastUpdated
              ? `Updated ${lastUpdated.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" })}`
              : "Connecting to queue"}
          </span>
        </div>
      </header>

      <main className="workspace">
        <section className="page-heading">
          <div>
            <div className="eyebrow">QUEUE / OVERVIEW</div>
            <h1>Queue overview</h1>
            <p className="page-description">Monitor work in progress and submit a new job.</p>
          </div>
          <div className="refresh-note">
            <span className="refresh-glyph" aria-hidden="true">↻</span>
            Live · refreshes every 2 seconds
          </div>
        </section>

        {error && (
          <div className="feedback feedback-error" role="alert">
            <span>{error}</span>
            <button type="button" className="text-button" onClick={() => void refreshNow()}>
              Try again
            </button>
          </div>
        )}
        {notice && (
          <div className="feedback feedback-success" role="status">
            <span>{notice}</span>
            <button type="button" className="dismiss-button" aria-label="Dismiss message" onClick={() => setNotice("")}>×</button>
          </div>
        )}

        <section className="summary-grid" aria-label="Queue statistics">
          <StatCard label="Queued" value={count("queued")} tone="queued" loading={loading && !stats} />
          <StatCard label="Running" value={count("running")} tone="running" loading={loading && !stats} />
          <StatCard label="Succeeded" value={count("succeeded")} tone="succeeded" loading={loading && !stats} />
          <StatCard label="Dead" value={count("dead")} tone="dead" loading={loading && !stats} />
          <div className="metric-card">
            <span className="metric-label">Created / min</span>
            <strong className="metric-value">{stats?.jobs_per_minute ?? "—"}</strong>
            <span className="metric-footnote">Last 60 seconds</span>
          </div>
          <div className="metric-card">
            <span className="metric-label">Avg. run time</span>
            <strong className="metric-value">
              {stats ? formatDuration(stats.average_duration_ms) : "—"}
            </strong>
            <span className="metric-footnote">Successful jobs</span>
          </div>
        </section>

        <div className="content-grid">
          <section className="queue-panel" aria-labelledby="jobs-title">
            <div className="panel-heading">
              <div>
                <h2 id="jobs-title">Recent jobs</h2>
                <p>{jobs.length} {jobs.length === 1 ? "job" : "jobs"} shown · newest first</p>
              </div>
              <label className="filter-control">
                <span>Filter</span>
                <select
                  aria-label="Filter jobs by status"
                  value={filter}
                  onChange={(event) => setFilter(event.target.value as "all" | JobStatus)}
                >
                  {statuses.map((status) => (
                    <option key={status} value={status}>
                      {status === "all" ? "All statuses" : statusNames[status]}
                    </option>
                  ))}
                </select>
              </label>
            </div>
            <div className="table-scroll">
              <table>
                <thead>
                  <tr>
                    <th scope="col">Job</th>
                    <th scope="col">Status</th>
                    <th scope="col">Priority</th>
                    <th scope="col">Attempts</th>
                    <th scope="col">Created</th>
                    <th scope="col"><span className="sr-only">Actions</span></th>
                  </tr>
                </thead>
                <tbody>
                  {loading && jobs.length === 0 ? (
                    <tr>
                      <td className="table-message" colSpan={6}>Loading queue…</td>
                    </tr>
                  ) : jobs.length === 0 ? (
                    <tr>
                      <td className="table-message empty-state" colSpan={6}>
                        <span className="empty-mark" aria-hidden="true">—</span>
                        <strong>No jobs in this view</strong>
                        <span>Try another status or submit a job.</span>
                      </td>
                    </tr>
                  ) : (
                    jobs.map((job) => (
                      <tr key={job.id}>
                        <td>
                          <div className="job-identity">
                            <span className="job-type">{job.type}</span>
                            <code title={job.id}>{compactID(job.id)}</code>
                            {job.last_error && <span className="job-error" title={job.last_error}>{job.last_error}</span>}
                          </div>
                        </td>
                        <td><StatusBadge status={job.status} /></td>
                        <td><PriorityLabel priority={job.priority} /></td>
                        <td className="attempt-count">{job.attempts}<span> / {job.max_attempts}</span></td>
                        <td className="created-time" title={new Date(job.created_at).toLocaleString()}>{formatTime(job.created_at)}</td>
                        <td className="actions-cell">
                          {job.status === "dead" || job.status === "failed" ? (
                            <button
                              className="row-action retry-action"
                              type="button"
                              disabled={pending === job.id}
                              onClick={() => void updateJob(job, "retry")}
                            >
                              {pending === job.id ? "Working…" : "Retry"}
                            </button>
                          ) : job.status === "queued" ? (
                            <button
                              className="row-action cancel-action"
                              type="button"
                              disabled={pending === job.id}
                              onClick={() => void updateJob(job, "cancel")}
                            >
                              {pending === job.id ? "Working…" : "Cancel"}
                            </button>
                          ) : (
                            <span className="no-action">—</span>
                          )}
                        </td>
                      </tr>
                    ))
                  )}
                </tbody>
              </table>
            </div>
            <div className="table-footer">
              <span>Showing up to 50 jobs</span>
              <span>Updates automatically</span>
            </div>
          </section>

          <aside className="submit-panel" aria-labelledby="submit-title">
            <div className="submit-heading">
              <span className="submit-index">NEW JOB <span>01</span></span>
              <h2 id="submit-title">Add to the queue</h2>
              <p>Give a worker something to do.</p>
            </div>
            <form onSubmit={createJob}>
              <label className="field-label" htmlFor="job-type">Job type</label>
              <select
                id="job-type"
                value={type}
                onChange={(event) => {
                  const nextType = event.target.value;
                  setType(nextType);
                  setPayload(payloadExamples[nextType]);
                }}
              >
                <option value="echo">Echo</option>
                <option value="sleep">Sleep</option>
                <option value="flaky">Flaky</option>
              </select>
              <label className="field-label payload-label" htmlFor="job-payload">
                Payload <span>JSON</span>
              </label>
              <textarea
                id="job-payload"
                value={payload}
                onChange={(event) => setPayload(event.target.value)}
                rows={5}
                spellCheck={false}
                required
              />
              <div className="form-row">
                <div>
                  <label className="field-label" htmlFor="job-priority">Priority</label>
                  <select
                    id="job-priority"
                    value={priority}
                    onChange={(event) => setPriority(Number(event.target.value))}
                  >
                    <option value={0}>High</option>
                    <option value={1}>Normal</option>
                    <option value={2}>Low</option>
                  </select>
                </div>
                <div>
                  <label className="field-label" htmlFor="job-attempts">Max attempts</label>
                  <input
                    id="job-attempts"
                    type="number"
                    min={1}
                    value={maxAttempts}
                    onChange={(event) => setMaxAttempts(Math.max(1, Number(event.target.value)))}
                    required
                  />
                </div>
              </div>
              <button className="submit-button" type="submit" disabled={pending === "create"}>
                <span>{pending === "create" ? "Submitting…" : "Submit job"}</span>
                <span aria-hidden="true">↗</span>
              </button>
              <p className="form-help">Jobs are saved to SQLite and picked up by an available worker.</p>
            </form>
          </aside>
        </div>
        <footer className="workspace-footer">
          <span>QueueForge <span className="footer-divider">/</span> Local queue</span>
          <span>SQLite · {stats ? `${stats.counts.running ?? 0} jobs running` : "Waiting for API"}</span>
        </footer>
      </main>
    </div>
  );
}

function StatCard({
  label,
  value,
  tone,
  loading,
}: {
  label: string;
  value: number;
  tone: JobStatus;
  loading: boolean;
}) {
  return (
    <div className={`stat-card stat-${tone}`}>
      <span className="stat-topline">
        <span className={`stat-dot dot-${tone}`} />
        {label}
      </span>
      <strong className="stat-value">{loading ? <span className="loading-bar" /> : value}</strong>
      <span className="stat-caption">Jobs {label.toLowerCase()}</span>
    </div>
  );
}

function StatusBadge({ status }: { status: JobStatus }) {
  return (
    <span className={`status-badge status-${status}`}>
      <span className="status-dot" />
      {statusNames[status]}
    </span>
  );
}

function PriorityLabel({ priority }: { priority: number }) {
  const label = priority === 0 ? "High" : priority === 2 ? "Low" : "Normal";
  return <span className={`priority-label priority-${label.toLowerCase()}`}>{label}</span>;
}

export default App;
