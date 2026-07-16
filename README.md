# tt-device-queue

A durable, bounded queue that serializes access to one Tenstorrent device. It
keeps the existing HTTP endpoints and MCP tools while replacing the original
in-memory/log-mirroring internals.

## Compatibility surface

The MCP tools retain their names and primary arguments:

- `queue(cmd, cwd="", repeat=1)`
- `queue_python(script, cwd="", repeat=1, python="python3", args=None)`
- `job(job_id)`
- `logs(job_id, offset=0, limit=16384)`
- `result(job_id)`
- `status()`
- `kill(job_id="")`
- `reset(job_id="")`

The HTTP service retains `POST /queue`, `/cancel`, `/kill`, `/reset` and
`GET /status`, `/breakage`, `/job/<id>`, `/logs/<id>`, `/result/<id>`.
Responses add worker-health and log-truncation fields but preserve the original
fields used by the MCP adapter.

## What changed internally

- SQLite is the durable source for job/device metadata. Output files are the
  sole log source, avoiding duplicate BLOB storage and per-chunk transactions.
- Only queued and running jobs live in memory. Completed jobs are loaded on
  demand and never cached indefinitely.
- Queued jobs recover after a server crash. The interrupted running job is
  failed clearly; it is never silently replayed.
- Device state, reset epoch, pending reset, and boot ID are durable. A same-boot
  service restart cannot clear a dead device. A real host reboot can.
- Every job has a 128-bit UUID, a default runtime ceiling, an output cap, and
  validated request fields.
- Reset requests can interrupt a hung current job, then run before further
  dispatch. Reset success requires a separate health-check command to pass.
- The worker is supervised internally. Persistence failures fail closed, retain
  dirty transitions for retry, and appear in `/status` rather than leaving a
  silently dead worker behind a healthy HTTP process.
- HTTP request bodies, concurrent handlers, queue depth, log reads, output
  returned by MCP, environment size, repeats, and timeouts are bounded.
- Queue/status scheduling is linear in pending jobs.
- MCP result polling is genuinely asynchronous and does not hold an executor
  thread for the duration of each job.
- Completed metadata/logs and generated MCP scripts have retention policies.

## Setup for development

```bash
cd ~/tenstorrent/tt-device-queue
./install.sh
PYTHONPATH=. .venv/bin/python3 -m unittest discover -s tests -p 'test_*.py'
```

Run an isolated development server on a non-production port:

```bash
TT_DEVICE_PORT=5742 TT_DEVICE_LOG_DIR=/tmp/tt-device-queue-test \
  .venv/bin/python3 server.py
```

Register the MCP adapter against that port:

```bash
TT_DEVICE_PORT=5742 codex mcp add tt-device-queue-test -- \
  ~/tenstorrent/tt-device-queue/.venv/bin/python3 \
  ~/tenstorrent/tt-device-queue/mcp_server.py
```

## Important defaults

| Setting | Default | Environment variable |
|---|---:|---|
| Job timeout | 3600 seconds | `TT_DEVICE_DEFAULT_TIMEOUT` |
| Absolute timeout limit | 86400 seconds | `TT_DEVICE_MAX_TIMEOUT` |
| Queue depth | 1000 | `TT_DEVICE_MAX_QUEUED_JOBS` |
| Output per job | 16 MiB | `TT_DEVICE_MAX_LOG_BYTES` |
| MCP `result` output | 1 MiB | `TT_DEVICE_MCP_RESULT_BYTES` |
| Request body | 1 MiB | `TT_DEVICE_MAX_REQUEST_BYTES` |
| Concurrent HTTP handlers | 16 | `TT_DEVICE_HTTP_WORKERS` |
| Metadata retention | 30 days / 10,000 jobs | `TT_DEVICE_RETENTION_DAYS`, `TT_DEVICE_MAX_COMPLETED_JOBS` |

Raw HTTP `timeout: 0` and omitted timeout both select the configured default;
unbounded jobs are intentionally not supported. MCP keeps its old signature
and reports the effective timeout returned by the server.

## Reset configuration and privilege boundary

The defaults are:

```text
TT_DEVICE_RESET_CMD=~/tenstorrent/.venv/bin/tt-smi -r
TT_DEVICE_HEALTH_CHECK_CMD=~/tenstorrent/.venv/bin/tt-smi -s
TT_DEVICE_DEEP_RESET_CMD=
```

The normal recovery path is unprivileged: `tt-smi -r` is retried, then
`tt-smi -s` must independently confirm that the device is available. If that
does not recover the device, the queue enters durable dead state, aborts queued
work, and requires a host reboot or operator recovery. The optional deep-reset
command remains empty in this installation.

This queue is an operational serialization mechanism, not a sandbox for
hostile commands. Strong enforcement requires running jobs and the reset
controller under separate OS identities or isolation domains.

## Storage

The service stores durable state in `logs-v2/`. The pre-v2 SQLite schema is
incompatible; do not point the new server at an old log directory.
