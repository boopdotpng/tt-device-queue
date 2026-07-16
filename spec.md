# tt-device-queue v2 behavior

## Core invariants

1. At most one normal job or reset owns the device worker at a time.
2. A job is durably inserted before a successful queue response or in-memory
   publication.
3. A normal job is never replayed after it reached `running`; queued jobs are
   recovered in FIFO-per-client order.
4. A same-boot restart preserves `dead`, reset epoch, and an interrupted reset.
5. The worker stops dispatching when durable state cannot be updated. Dirty
   transitions are retried before dispatch resumes.
6. Memory, request bodies, queue depth, log reads, stored output, and MCP result
   output are bounded by configuration.

## Scheduling

Jobs are FIFO within `client_id` and round-robin across clients. The recovered
rotation is ordered by the oldest queued job for each client. Queue position and
ETA simulation use deques and a single cumulative pass.

## Lifecycle

Normal jobs transition `queued -> running -> done`. Server failure while
`queued` leaves the job queued. Server failure while `running` marks it done
with exit code `-1` and an interruption message on restart.

Every job receives a configured timeout even when the caller omits one. Timeout
kills the whole process group and reports exit code `-9`. `kill` sends SIGINT,
then the worker escalates to SIGKILL after the configured grace period.

Logs are read directly from a bounded per-job output file. Once its cap is
reached, output is drained and discarded so the child cannot block; a
truncation marker and dropped-byte count are recorded.

## Device recovery

`POST /reset` coalesces by durable reset epoch. By default it requests a stop
of the current normal job so a hung job cannot indefinitely prevent recovery.
The reset command must succeed and the independent health-check command must
also succeed. Optional deep reset is attempted only when explicitly configured.

Failed recovery enters durable `dead` state and drains queued jobs. A service
restart during the same Linux boot does not clear `dead`. A new boot ID starts
healthy while retaining history and the epoch counter.

## Degraded operation

Persistence errors set `worker.degraded_reason`, disable submissions, and stop
dispatch. The worker periodically probes SQLite and flushes dirty job/device
transitions; dispatch resumes only after those writes succeed. Unknown worker
exceptions remain fail-closed and require operator attention.

Retention errors are reported as maintenance warnings and do not stop device
work.

## Compatibility additions

Existing HTTP endpoints remain. The MCP surface omits the redundant
`last_breakage` tool and queued-job `cancel` tool. New response fields include:

- `/status.worker.alive`, `heartbeat_age_sec`, `degraded_reason`, and
  `maintenance_warning`
- `log_size`, `log_truncated`, and `dropped_log_bytes` on job/log/result data
- effective `timeout` in queue responses
