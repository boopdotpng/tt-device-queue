# tt-device-queue Spec

This document describes the behavior currently implemented in this repository.
It is intended to be the source of truth for feature scope and runtime semantics.

## Purpose

- Serialize access to a shared hardware resource through a local job queue.
- Schedule fairly across many concurrent agents: round-robin across `client_id`s,
  FIFO within a `client_id`, so no single agent can dominate the queue.
- Manage device health centrally: coalesce reset requests, hold the queue while
  a reset runs, and stop everything with a reboot-required message when the
  device does not come back.
- Provide two user-facing surfaces:
  - an HTTP queue server in `server.py`
  - an MCP wrapper in `mcp_server.py`

## Process Model

- `server.py` runs a single worker thread and executes queued jobs one at a time.
- The queue server listens on `TT_DEVICE_HOST` / `TT_DEVICE_PORT` and defaults to `127.0.0.1:5741`.
- Jobs are persisted in memory only for the lifetime of the server process.
- The installed systemd user service runs `server.py`, not `mcp_server.py`.
- `mcp_server.py` runs over stdio and is started by the MCP client on demand.

## Client Identity and Fair Scheduling

- Every submission carries a `client_id` (default `anon` when omitted).
- One MCP server process equals one agent session: `mcp_server.py` generates a
  stable `CLIENT_ID` at startup (`TT_QUEUE_CLIENT_ID` env override, otherwise
  `agent-<8 hex>`), and attaches it to every queue submission.
- The scheduler keeps one FIFO subqueue per `client_id` and serves clients in
  round-robin order: serve the head of the rotation, then rotate that client to
  the back. Clients with empty subqueues leave the rotation.
- Within a single client, submission order is preserved (FIFO).
- Reported `position` and `estimated_wait_sec` are computed by simulating the
  round-robin dispatch order over the current subqueues.
- Identity is cooperative, not authenticated. Raw HTTP callers that omit
  `client_id` all share the `anon` bucket, which participates in the rotation
  as one client.

## Device Health State Machine

- States: `healthy` -> `resetting` -> `healthy` or `dead`.
- The server owns resets. `POST /reset` is a report/request, not a queued job.
- Every job records the `reset_epoch` it ran under (set when the job starts).
  The epoch increments after each successful reset.
- Coalescing rules for `POST /reset {"job_id": ...}`:
  - job's epoch < current epoch -> `already_reset` (no new reset; resubmit)
  - reset pending or in progress -> `joined`
  - otherwise -> `scheduled` (the worker runs it before dispatching more jobs)
- The currently running job is allowed to finish (bounded by the run timeout,
  when one is configured);
  the reset runs before the next dispatch.
- Reset reports record `last_breakage`, exposed in `/reset`, `/status`, and
  `/breakage`. The suspected culprit is the reported `job_id` when provided,
  otherwise the currently running non-reset job, otherwise the latest completed
  non-reset job. The record includes the command, client, output file, reset
  epoch, reporter, and reset job/result when known.
- Reset procedure (worker thread): run `TT_DEVICE_RESET_CMD`. If it exits
  non-zero, retry up to `TT_DEVICE_RESET_RETRIES` more times (default 1). Each
  reset run is recorded as a synthetic job with `mode=reset` and
  `client_id=system`, visible in status/history/logs.
- Reset command success: epoch increments, state returns to `healthy`, queue resumes.
- Reset command failure after retries: state becomes `dead` after the deep reset
  escalation also fails:
  - all queued jobs are failed with exit code `-1` and a
    `DEVICE UNRECOVERABLE ... host reboot is required` message appended to
    their logs (agents blocked on `result` receive it via the normal poll)
  - new submissions are rejected with HTTP 503 and the same message
  - further `/reset` requests are rejected with HTTP 503
  - `/status` reports `device.state = "dead"` with `dead_since`/`dead_reason`
- Dead state is in-memory only: a server restart (e.g. after host reboot)
  starts back at `healthy` with `reset_epoch = 0`.
- The default reset command is `~/tenstorrent/blackhole-py/reset.py -r`,
  overridable via environment for testing.

## Job Model

Each queued job has:

- `job_id`: 8 hex chars, unique per server lifetime
- `cmd`: shell command string executed with `shell=True`
- `cwd`: working directory passed to `subprocess.Popen`
- `timeout`: total timeout budget for the whole job; `0` means no timeout
- `repeat`: number of sequential executions inside the same job, minimum `1`
- `env`: per-job environment variables merged into the subprocess environment
- `mode`: `run` for normal jobs, `reset` for server-managed device resets
- `client_id`: fairness unit for scheduling; defaults to `anon`, max 128 chars
- `reset_epoch`: the device reset epoch the job ran under
- `status`: `queued`, `running`, or `done`
- `output_file`: `./logs/<job_id>/output` by default, overridable via `TT_DEVICE_LOG_DIR`
- timestamps: `submitted_at`, `started_at`, `finished_at`
- results: `exit_code`, `elapsed`, `timed_out`, `timeout_message`
- repeat progress: `repeat_current`, `repeat_completed`, `first_iteration_elapsed`, `per_iter_estimate_sec`

## Repeat Semantics

- `repeat=1` behaves like a normal single command execution.
- `repeat>1` executes the same command sequentially inside one queued job.
- The queue creates exactly one `job_id` and one output file for the entire repeated run.
- Before each repeated iteration, the server appends a marker line:
  - `[tt-device-queue] Repeat N/M`
- If any iteration exits non-zero, the job stops immediately and later iterations are not run.
- If the job times out, the server sends `SIGKILL` to the process group immediately. Timed-out jobs end with exit code `-9`.
- Explicit stop requests send Ctrl+C first, then escalate to `SIGKILL` if needed.
- Timeout applies to the full repeated job, not to each iteration independently.
- MCP queue tools do not expose a timeout argument; submitted MCP commands run without a timeout.

## Output and Metadata Files

- Each job writes stdout/stderr combined into `output_file`.
- The same output bytes are appended live into `./logs/jobs.sqlite3` by default.
- Repeated runs append all iterations to the same `output_file`.
- When a job finishes, `meta.json` is written next to the output file.
- `meta.json` contains final execution metadata including repeat progress and timing estimates.
- Completed job metadata and log bytes remain queryable from SQLite after a server restart.

## Estimation Semantics

- Initial per-iteration estimate is `10` seconds.
- Initial submit-time `estimated_run_sec` is `repeat * 10`.
- Initial queue wait is the sum of estimated remaining runtimes of jobs ahead, including the currently running job.
- After the first successful repeat iteration, `per_iter_estimate_sec` is updated to that iteration's runtime.
- Remaining runtime for a running repeat job is estimated as:
  - remaining time in the current iteration, plus
  - `per_iter_estimate_sec * remaining_iterations_after_current`
- Completed jobs report `estimated_remaining_sec = 0`.

## HTTP API

### POST `/queue`

Request JSON:

```json
{
  "cmd": "python3 script.py",
  "cwd": "/path/to/repo",
  "repeat": 1,
  "mode": "run",
  "env": {"TT_USB": "1"},
  "client_id": "agent-1a2b3c4d"
}
```

Behavior:

- `cmd` is required and must be non-empty after stripping.
- `repeat` must be `>= 1`.
- `mode` defaults to `run`; new submissions must use `run`.
- `timeout` is optional for raw HTTP callers; omitted or `0` means no timeout. MCP callers cannot set it.
- `env` defaults to `{}` and must be an object whose names and values are strings.
- `client_id` defaults to `anon`; must be a non-empty string of at most 128 chars.
- Returns HTTP 503 with the reboot-required message when the device is dead.
- Returns HTTP 200 with:
  - `job_id`
  - `output_file`
  - `position` where `0` means starts immediately if idle/current slot is free;
    computed against the simulated round-robin dispatch order
  - `estimated_wait_sec`
  - `estimated_run_sec`
  - `repeat`
  - `mode`
  - `timeout`

### POST `/reset`

Request JSON: `{"job_id": "<failing job, optional>", "client_id": "agent-1a2b3c4d"}`

- Reports a broken device / requests a reset. Never queues a job.
- Returns HTTP 200 with `action` one of:
  - `already_reset`: the referenced job ran before the latest reset; resubmit
  - `joined`: a reset is already pending or in progress
  - `scheduled`: a reset will run before the next job dispatch
- Plus `device_state`, `reset_epoch`, `breakage`, and a human-readable `hint`.
- Unknown `job_id` returns HTTP 404. Dead device returns HTTP 503.

### GET `/breakage`

Returns `{"last_breakage": ...}` where the value is `null` until a reset has
been requested. This is the direct lookup for the suspected device-breaking
job and its output file.

### POST `/cancel`

Request JSON: `{"job_id": "..."}`

- Cancels a queued (not yet running) job: removes it from its client's
  subqueue, appends a `Cancelled while queued` log line, and marks it done
  with exit code `-1`.
- Running or finished jobs return HTTP 409 (use `/kill` for running jobs).
- Unknown jobs return HTTP 404. Missing `job_id` returns HTTP 400.

### GET `/result/<job_id>`

Returns one of:

- queued:
  - `status=queued`
  - `mode`
  - `position`
  - `estimated_wait_sec`
  - `estimated_remaining_sec`
  - repeat metadata
- running:
  - `status=running`
  - `mode`
  - `position=0`
  - `estimated_wait_sec=0`
  - `estimated_remaining_sec`
  - repeat metadata
- done:
  - `status=done`
  - `mode`
  - `exit_code`
  - `output_file`
  - `elapsed`
  - `estimated_remaining_sec=0`
  - repeat metadata
  - `timed_out`; when true, `timeout_message` is included

Unknown jobs return HTTP 404.

### GET `/job/<job_id>`

Returns structured metadata for the job, including:

- lifecycle status and timestamps
- command, cwd, timeout
- repeat progress and ETA fields
- queue position when queued
- `running_sec` when running
- `timed_out`; when true, `timeout_message` is included

Unknown jobs return HTTP 404.

### GET `/logs/<job_id>?offset=<n>&limit=<n>`

Behavior:

- Reads from SQLite using byte offsets, with a file fallback for legacy rows.
- `offset < 0` is clamped to `0`.
- `limit` is clamped to `[1, 65536]`.
- Returns:
  - `job_id`
  - `status`
  - `output_file`
  - `offset`
  - `next_offset`
  - `content`
  - `truncated`
  - `complete`
- `complete` means job is done and the returned chunk reaches EOF.

Unknown jobs return HTTP 404.

### GET `/status`

Returns global queue state:

- `current`: currently running job summary (including `client`) or `null`
- `pending`: queued jobs in simulated round-robin dispatch order with wait and
  run estimates and the owning `client`
- `recent`: last 10 completed jobs from persistent SQLite history
- `device`: health block with `state` (`healthy|resetting|dead`),
  `reset_epoch`, `reset_pending`, `last_reset_at`, `dead_since`, `dead_reason`

### POST `/kill`

Behavior:

- Accepts optional JSON body `{"job_id": "..."}`.
- If the requested job is currently running, sends Ctrl+C to its process group.
- The worker waits up to a short grace window, then escalates to `SIGKILL` if needed.
- Returns `{"killed": {...}}` when something was killed.
- Returns HTTP 409 when the requested job is not the currently running job.
- Returns `{"error": "Nothing running"}` when idle.

## MCP Surface

Implemented tools in `mcp_server.py`:

- `queue(cmd, cwd, repeat)`
- `queue_python(script, cwd, repeat, python, args)`
- `job(job_id)`
- `logs(job_id, offset, limit)`
- `result(job_id)`
- `status()`
- `kill(job_id="")`
- `cancel(job_id)`
- `reset(job_id="")`

MCP behavior notes:

- Queue-backed tools call the HTTP server through shared code in `queue_client.py`.
- The MCP process generates a per-session `CLIENT_ID` at startup
  (`TT_QUEUE_CLIENT_ID` env override) and attaches it to every submission;
  this is the fairness unit for round-robin scheduling.
- `reset(job_id)` reports a broken device. The server coalesces resets per
  epoch; the expected agent protocol is: job fails strangely -> `reset(job_id)`
  -> check `action` -> resubmit the job once the device is healthy.
- `cancel(job_id)` cancels one of the agent's queued jobs.
- `status()` prints a banner when the device is resetting or dead.
- The MCP server is only for commands that touch Tenstorrent hardware. CPU-only
  and general development work should use normal shell/tools instead.
- The HTTP server automatically adds `.` to `PYTHONPATH`; MCP callers do not
  need a separate env field for the common `PYTHONPATH=.` case.
- Leading shell assignments such as `MATMUL_PROFILE=1 python3 ...` work.
- `queue_python` stores large one-off Python snippets as files before queueing,
  keeping queue metadata readable.
- `result` waits until completion, then returns the full output text.
- `reset` is not queued work and not a direct bypass path; it is a health
  report handled by the server's device state machine.
- The deep-reset sudo helper refuses direct agent-shell invocation; it only
  accepts sudo calls spawned directly by the queue server reset worker.

## Shared Client Layer

`queue_client.py` provides shared helper behavior for MCP:

- HTTP GET/POST with uniform error translation
- blocking wait/poll loop for jobs
- output-file reading for completed results

This file keeps HTTP client behavior separate from MCP transport code.

## Installation and Service Behavior

- `install.sh` creates `.venv` and installs `mcp`.
- `install.sh` removes legacy `tt-device-queue` CLI symlinks from `~/.local/bin`.
- `install.sh` installs and enables the user systemd service from `tt-device-queue.service`.
- The systemd unit starts only the HTTP queue server.
- Updating `server.py` requires restarting the service.
- Updating `mcp_server.py` requires reconnecting/restarting the MCP client session.

## Test Coverage Currently Present

Implemented tests cover:

- repeated success writes one job and one output file
- repeated failure stops on first failing iteration
- job timeout kills the repeated run and stops later iterations
- per-job queued/running/done metadata
- log chunk reading with offsets and completion detection
- repeat-aware ETA initialization and refinement after first iteration
- `repeat` defaulting to `1`
- round-robin interleaving across clients and FIFO within a client
- `client_id` defaulting, validation, and visibility in job/status payloads
- cancellation of queued jobs (and 404/409 for unknown/running jobs)
- legacy SQLite databases gaining `client_id`/`reset_epoch` columns on startup
- reset coalescing: concurrent requests run exactly one reset; stale-epoch
  reports are no-ops
- the queue holding jobs during a reset and resuming after success
- failed reset draining the queue with the reboot message, rejecting new
  submissions and reset requests with HTTP 503, and recovery after restart
- MCP payloads carrying `CLIENT_ID`, the report-style `reset` tool, `cancel`,
  and device banners in `status`

## Not Guaranteed by the Current Implementation

- Queued or running jobs do not resume after a server restart; they are marked done with exit code `-1`.
- No authentication or remote access controls beyond binding to localhost by default
- Client identity is cooperative (self-reported), not authenticated
- No preemption: a reset waits for the currently running job to finish or time out
- Device health state is not persisted; a restart returns to `healthy`
- No multi-device support yet; one worker, one device, one health state
- No streaming MCP transport for partial `result` output
- No server-side enforcement that a command actually targets device hardware
