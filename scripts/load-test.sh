#!/bin/sh
set -eu

REQUESTS="${REQUESTS:-1000}"
CONCURRENCY="${CONCURRENCY:-20}"
PORT="${PORT:-18080}"
tmp="$(mktemp -d "${TMPDIR:-/tmp}/queueforge-load.XXXXXX")"
binary="$tmp/queueforge"
db="$tmp/jobs.db"
payload="$tmp/payload.json"
server_log="$tmp/server.log"
hey_output="$tmp/hey.txt"
server_pid=""

cleanup() {
	if [ -n "$server_pid" ]; then
		kill -TERM "$server_pid" 2>/dev/null || true
		wait "$server_pid" 2>/dev/null || true
	fi
	rm -f "$binary" "$db" "$db-wal" "$db-shm" "$payload" "$server_log" "$hey_output"
	rmdir "$tmp" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

if ! command -v hey >/dev/null 2>&1; then
	echo "hey is required; install it from https://github.com/rakyll/hey" >&2
	exit 1
fi

go build -o "$binary" ./cmd/queueforge
printf '%s\n' '{"type":"echo","payload":{"load_test":true}}' > "$payload"
RATE_LIMIT_PER_SEC=100000 RATE_LIMIT_BURST=100000 PORT="$PORT" DB_PATH="$db" "$binary" >"$server_log" 2>&1 &
server_pid=$!
ready=0
i=0
while [ "$i" -lt 60 ]; do
	if curl -fsS "http://127.0.0.1:$PORT/healthz" >/dev/null 2>&1; then
		ready=1
		break
	fi
	i=$((i + 1))
	sleep 1
done
if [ "$ready" -ne 1 ]; then
	echo "QueueForge did not become ready; server log follows:" >&2
	cat "$server_log" >&2
	exit 1
fi

hey -n "$REQUESTS" -c "$CONCURRENCY" -m POST -H 'Content-Type: application/json' -D "$payload" "http://127.0.0.1:$PORT/jobs" | tee "$hey_output"
load_seconds="$(awk '/^  Total:/ { print $2; exit }' "$hey_output")"
attempt=0
succeeded=0
while [ "$attempt" -lt 1200 ]; do
	stats="$(curl -fsS "http://127.0.0.1:$PORT/stats")"
	succeeded="$(printf '%s' "$stats" | sed -n 's/.*"succeeded":\([0-9][0-9]*\).*/\1/p')"
	[ -n "$succeeded" ] || succeeded=0
	if [ "$succeeded" -ge "$REQUESTS" ]; then break; fi
	attempt=$((attempt + 1))
	sleep 0.1
done
echo "HTTP request rate (hey output):"
grep 'Requests/sec' "$hey_output" || true
echo "Completed jobs: $succeeded / $REQUESTS"
awk -v completed="$succeeded" -v load="$load_seconds" -v wait="$attempt" 'BEGIN {
	elapsed = load + wait / 10
	if (elapsed < 0.0001) elapsed = 0.0001
	printf "Job completion throughput: %.2f jobs/sec over %.3f sec\n", completed/elapsed, elapsed
}'
if [ "$succeeded" -lt "$REQUESTS" ]; then
	echo "Timed out waiting for all jobs to complete." >&2
	exit 1
fi
