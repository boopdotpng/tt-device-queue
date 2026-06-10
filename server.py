#!/usr/bin/env python3
"""
tt-device-queue server — serializes access to the Tenstorrent device.

HTTP API on localhost:5741. Jobs run one at a time. Scheduling is round-robin
across client_ids (one agent = one client) and FIFO within a client, so no
single agent can dominate the queue. Output is saved to ./logs/<job_id>/output
by default.

Device health is managed by the server: resets are coalesced (per reset epoch),
the queue is held while a reset runs, and if the device does not come back the
server enters a "dead" state — all queued jobs are failed with a reboot-required
message and new submissions are rejected with HTTP 503.

Endpoints:
  POST /queue   {"cmd": "...", "cwd": "...", "timeout": 120, "repeat": 1,
                 "client_id": "agent-xyz"}
                -> {"job_id", "output_file", "position", "estimated_wait_sec"}

  POST /reset   {"job_id": "<failing job, optional>"}
                -> {"action": "scheduled|joined|already_reset", ...}

  POST /cancel  {"job_id": "..."}  (queued jobs only; use /kill for running)

  GET  /result/<job_id>
                -> {"status": "queued|running|done", "position", "estimated_wait_sec"}
                   or {"status": "done", "exit_code", "output_file", "elapsed"}

  GET  /job/<job_id>
                -> full job metadata including repeat progress, timestamps, and
                   queue position when still pending

  GET  /status  -> {"current", "pending", "recent", "device"}
"""

import contextlib
import json
import os
import resource
import select
import signal
import sqlite3
import subprocess
import threading
import time
import uuid
from collections import deque
from http.server import HTTPServer, BaseHTTPRequestHandler
from pathlib import Path
from dataclasses import dataclass, field
from urllib.parse import parse_qs, urlparse

HOST = os.environ.get("TT_DEVICE_HOST", "127.0.0.1")
PORT = int(os.environ.get("TT_DEVICE_PORT", "5741"))
DEFAULT_TIMEOUT = int(os.environ.get("TT_DEVICE_TIMEOUT", "120"))
DEFAULT_OPEN_TIMEOUT = int(os.environ.get("TT_DEVICE_OPEN_TIMEOUT", "180"))
REPO_ROOT = Path(__file__).resolve().parent
LOG_DIR = Path(os.environ.get("TT_DEVICE_LOG_DIR", str(REPO_ROOT / "logs")))
DB_PATH = LOG_DIR / "jobs.sqlite3"
DEFAULT_ITER_ESTIMATE_SEC = 10
MAX_LOG_READ = 64 * 1024
STOP_GRACE_SEC = 8
DEFAULT_CHILD_OOM_SCORE_ADJ = "500"
DEFAULT_CLIENT_ID = "anon"
MAX_CLIENT_ID_LEN = 128

TT_SMI_PATH = os.environ.get(
  "TT_DEVICE_TT_SMI", os.path.expanduser("~/tenstorrent/blackhole-py/tt-smi.py")
)
RESET_CMD = os.environ.get("TT_DEVICE_RESET_CMD", f"{TT_SMI_PATH} -r 0")
PROBE_CMD = os.environ.get("TT_DEVICE_PROBE_CMD", f"{TT_SMI_PATH} --snapshot")
RESET_RETRIES = int(os.environ.get("TT_DEVICE_RESET_RETRIES", "1"))
HEALTH_CMD_TIMEOUT = int(os.environ.get("TT_DEVICE_HEALTH_CMD_TIMEOUT", "60"))
REBOOT_REQUIRED_MSG = (
  "DEVICE UNRECOVERABLE: reset did not bring the device back (tt-smi probe "
  "failing). A host reboot is required. All queued jobs were aborted — end "
  "your turn and do not submit further jobs."
)

LOG_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH.parent.mkdir(parents=True, exist_ok=True)


def _job_env(extra_env: dict[str, str] | None = None) -> dict[str, str]:
  env = os.environ.copy()
  if extra_env:
    env.update(extra_env)
  pythonpath = env.get("PYTHONPATH", "")
  parts = [part for part in pythonpath.split(os.pathsep) if part]
  if "." not in parts:
    env["PYTHONPATH"] = os.pathsep.join(["."] + parts)
  return env


def _job_shell_script(cmd: str) -> str:
  return "\n".join([
    "printf '%s\\n' \"${TT_DEVICE_CHILD_OOM_SCORE_ADJ:-"
    f"{DEFAULT_CHILD_OOM_SCORE_ADJ}"
    "}\" > /proc/$$/oom_score_adj 2>/dev/null || true",
    cmd,
  ])


def _format_timestamp(ts: float | None) -> str | None:
  if ts is None:
    return None
  return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts))


class DeviceDeadError(RuntimeError):
  """Raised when the device is dead and a host reboot is required."""


@dataclass
class Job:
  id: str
  cmd: str
  cwd: str
  timeout: int
  repeat: int
  env: dict[str, str] = field(default_factory=dict)
  mode: str = "run"
  client_id: str = DEFAULT_CLIENT_ID
  reset_epoch: int = 0
  submitted: float = field(default_factory=time.time)
  # Filled in by worker
  status: str = "queued"        # queued -> running -> done
  exit_code: int | None = None
  elapsed: float | None = None
  output_file: str = ""
  started_at: float | None = None
  finished_at: float | None = None
  repeat_current: int = 0
  repeat_completed: int = 0
  current_iteration_started_at: float | None = None
  first_iteration_elapsed: float | None = None
  per_iter_estimate_sec: float = DEFAULT_ITER_ESTIMATE_SEC
  stop_requested_at: float | None = None
  stop_escalated_at: float | None = None
  log_size: int = 0


class JobStore:
  def __init__(self, db_path: Path):
    self.db_path = db_path
    self._init_schema()
    self.mark_abandoned_jobs()

  @contextlib.contextmanager
  def _connect(self):
    """Yield a connection wrapped in a transaction, always closing it.

    NOTE: `with sqlite3.connect(...)` only manages the transaction — it does
    NOT close the connection. On Python >= 3.13 an unclosed connection is no
    longer closed when garbage collected (it just emits a ResourceWarning), so
    every connection MUST be closed explicitly or the process leaks one fd per
    DB operation until it hits RLIMIT_NOFILE ("Too many open files").
    """
    conn = sqlite3.connect(self.db_path, timeout=30)
    conn.row_factory = sqlite3.Row
    try:
      with conn:
        yield conn
    finally:
      conn.close()

  def _init_schema(self):
    with self._connect() as conn:
      conn.execute("PRAGMA journal_mode=WAL")
      conn.execute("""
        CREATE TABLE IF NOT EXISTS jobs (
          id TEXT PRIMARY KEY,
          cmd TEXT NOT NULL,
          cwd TEXT NOT NULL,
          timeout INTEGER NOT NULL,
          repeat INTEGER NOT NULL,
          env_json TEXT NOT NULL,
          mode TEXT NOT NULL,
          status TEXT NOT NULL,
          submitted REAL NOT NULL,
          started_at REAL,
          finished_at REAL,
          exit_code INTEGER,
          elapsed REAL,
          output_file TEXT NOT NULL,
          repeat_current INTEGER NOT NULL,
          repeat_completed INTEGER NOT NULL,
          first_iteration_elapsed REAL,
          per_iter_estimate_sec REAL NOT NULL,
          stop_requested_at REAL,
          stop_escalated_at REAL,
          log_size INTEGER NOT NULL DEFAULT 0,
          updated_at REAL NOT NULL,
          client_id TEXT NOT NULL DEFAULT 'anon',
          reset_epoch INTEGER NOT NULL DEFAULT 0
        )
      """)
      existing = {
        row[1] for row in conn.execute("PRAGMA table_info(jobs)").fetchall()
      }
      if "client_id" not in existing:
        conn.execute(
          "ALTER TABLE jobs ADD COLUMN client_id TEXT NOT NULL DEFAULT 'anon'"
        )
      if "reset_epoch" not in existing:
        conn.execute(
          "ALTER TABLE jobs ADD COLUMN reset_epoch INTEGER NOT NULL DEFAULT 0"
        )
      conn.execute("""
        CREATE TABLE IF NOT EXISTS log_chunks (
          job_id TEXT NOT NULL,
          offset INTEGER NOT NULL,
          data BLOB NOT NULL,
          created_at REAL NOT NULL,
          PRIMARY KEY (job_id, offset),
          FOREIGN KEY (job_id) REFERENCES jobs(id) ON DELETE CASCADE
        )
      """)
      conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_jobs_finished_at ON jobs(finished_at DESC)"
      )
      conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_log_chunks_job_offset ON log_chunks(job_id, offset)"
      )

  def mark_abandoned_jobs(self):
    now = time.time()
    with self._connect() as conn:
      rows = conn.execute(
        "SELECT id, log_size FROM jobs WHERE status IN ('queued', 'running')"
      ).fetchall()
      for row in rows:
        message = (
          b"\n[tt-device-queue] Server restarted before this job completed\n"
        )
        offset = int(row["log_size"] or 0)
        conn.execute(
          """
          INSERT OR IGNORE INTO log_chunks(job_id, offset, data, created_at)
          VALUES (?, ?, ?, ?)
          """,
          (row["id"], offset, message, now),
        )
        conn.execute(
          """
          UPDATE jobs
          SET status = 'done',
              exit_code = -1,
              finished_at = ?,
              elapsed = CASE
                WHEN started_at IS NULL THEN NULL
                ELSE ROUND(? - started_at, 2)
              END,
              log_size = log_size + ?,
              updated_at = ?
          WHERE id = ?
          """,
          (now, now, len(message), now, row["id"]),
        )

  def save_job(self, job: Job):
    with self._connect() as conn:
      conn.execute(
        """
        INSERT INTO jobs (
          id, cmd, cwd, timeout, repeat, env_json, mode, status, submitted,
          started_at, finished_at, exit_code, elapsed, output_file,
          repeat_current, repeat_completed, first_iteration_elapsed,
          per_iter_estimate_sec, stop_requested_at, stop_escalated_at,
          log_size, updated_at, client_id, reset_epoch
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
          cmd = excluded.cmd,
          cwd = excluded.cwd,
          timeout = excluded.timeout,
          repeat = excluded.repeat,
          env_json = excluded.env_json,
          mode = excluded.mode,
          status = excluded.status,
          submitted = excluded.submitted,
          started_at = excluded.started_at,
          finished_at = excluded.finished_at,
          exit_code = excluded.exit_code,
          elapsed = excluded.elapsed,
          output_file = excluded.output_file,
          repeat_current = excluded.repeat_current,
          repeat_completed = excluded.repeat_completed,
          first_iteration_elapsed = excluded.first_iteration_elapsed,
          per_iter_estimate_sec = excluded.per_iter_estimate_sec,
          stop_requested_at = excluded.stop_requested_at,
          stop_escalated_at = excluded.stop_escalated_at,
          log_size = excluded.log_size,
          updated_at = excluded.updated_at,
          client_id = excluded.client_id,
          reset_epoch = excluded.reset_epoch
        """,
        (
          job.id, job.cmd, job.cwd, job.timeout, job.repeat,
          json.dumps(job.env, sort_keys=True), job.mode, job.status,
          job.submitted, job.started_at, job.finished_at, job.exit_code,
          job.elapsed, job.output_file, job.repeat_current, job.repeat_completed,
          job.first_iteration_elapsed, job.per_iter_estimate_sec,
          job.stop_requested_at, job.stop_escalated_at, job.log_size,
          time.time(), job.client_id, job.reset_epoch,
        ),
      )

  def append_log(self, job_id: str, offset: int, data: bytes, log_size: int):
    if not data:
      return
    with self._connect() as conn:
      conn.execute(
        """
        INSERT OR IGNORE INTO log_chunks(job_id, offset, data, created_at)
        VALUES (?, ?, ?, ?)
        """,
        (job_id, offset, data, time.time()),
      )
      conn.execute(
        "UPDATE jobs SET log_size = ?, updated_at = ? WHERE id = ?",
        (log_size, time.time(), job_id),
      )

  def load_job(self, job_id: str) -> Job | None:
    with self._connect() as conn:
      row = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
    if row is None:
      return None
    return self._row_to_job(row)

  def recent_completed(self, limit: int = 10) -> list[dict]:
    with self._connect() as conn:
      rows = conn.execute(
        """
        SELECT * FROM jobs
        WHERE status = 'done'
        ORDER BY finished_at DESC, submitted DESC
        LIMIT ?
        """,
        (limit,),
      ).fetchall()
    return [self._history_row(row) for row in reversed(rows)]

  def read_logs(self, job_id: str, offset: int, limit: int) -> tuple[bytes, int, bool]:
    with self._connect() as conn:
      row = conn.execute(
        "SELECT log_size FROM jobs WHERE id = ?",
        (job_id,),
      ).fetchone()
      if row is None:
        return b"", 0, False
      file_size = int(row["log_size"] or 0)
      chunks = conn.execute(
        """
        SELECT offset, data FROM log_chunks
        WHERE job_id = ? AND offset + length(data) > ?
        ORDER BY offset
        """,
        (job_id, offset),
      ).fetchall()

    data = bytearray()
    for chunk in chunks:
      chunk_offset = int(chunk["offset"])
      chunk_data = bytes(chunk["data"])
      start = max(0, offset - chunk_offset)
      if start >= len(chunk_data):
        continue
      data.extend(chunk_data[start:])
      if len(data) > limit:
        break

    truncated = len(data) > limit
    return bytes(data[:limit]), file_size, truncated

  def _row_to_job(self, row: sqlite3.Row) -> Job:
    try:
      env = json.loads(row["env_json"] or "{}")
    except json.JSONDecodeError:
      env = {}
    return Job(
      id=row["id"],
      cmd=row["cmd"],
      cwd=row["cwd"],
      timeout=int(row["timeout"]),
      repeat=int(row["repeat"]),
      env=env,
      mode=row["mode"],
      client_id=row["client_id"] or DEFAULT_CLIENT_ID,
      reset_epoch=int(row["reset_epoch"] or 0),
      submitted=float(row["submitted"]),
      status=row["status"],
      exit_code=row["exit_code"],
      elapsed=row["elapsed"],
      output_file=row["output_file"],
      started_at=row["started_at"],
      finished_at=row["finished_at"],
      repeat_current=int(row["repeat_current"]),
      repeat_completed=int(row["repeat_completed"]),
      first_iteration_elapsed=row["first_iteration_elapsed"],
      per_iter_estimate_sec=float(row["per_iter_estimate_sec"]),
      stop_requested_at=row["stop_requested_at"],
      stop_escalated_at=row["stop_escalated_at"],
      log_size=int(row["log_size"] or 0),
    )

  def _history_row(self, row: sqlite3.Row) -> dict:
    return {
      "id": row["id"],
      "cmd": row["cmd"][:120],
      "exit_code": row["exit_code"],
      "elapsed": row["elapsed"],
      "finished": (
        time.strftime("%H:%M:%S", time.localtime(row["finished_at"]))
        if row["finished_at"] else None
      ),
      "output_file": row["output_file"],
      "repeat": row["repeat"],
      "mode": row["mode"],
      "client": row["client_id"] or DEFAULT_CLIENT_ID,
      "repeat_completed": row["repeat_completed"],
      "per_iter_estimate_sec": round(row["per_iter_estimate_sec"], 2),
    }


class DeviceQueue:
  def __init__(self, store: JobStore):
    self._store = store
    self._jobs: dict[str, Job] = {}          # all jobs by id
    self._pending: dict[str, deque[str]] = {}  # client_id -> queued job ids (FIFO)
    self._rr_clients: list[str] = []         # round-robin rotation of clients
    self._current: Job | None = None
    self._current_proc: subprocess.Popen | None = None
    self._lock = threading.Lock()
    self._cond = threading.Condition(self._lock)
    # Device health state machine: healthy -> resetting -> healthy | dead
    self._device_state = "healthy"
    self._reset_epoch = 0
    self._reset_pending = False
    self._last_reset_at: float | None = None
    self._dead_since: float | None = None
    self._dead_reason: str | None = None

  def _dispatch_order_locked(self) -> list[str]:
    """Simulate the round-robin scheduler over current subqueues.

    Returns the flat list of pending job ids in predicted dispatch order.
    """
    queues = {c: list(q) for c, q in self._pending.items() if q}
    rotation = [c for c in self._rr_clients if c in queues]
    order: list[str] = []
    while rotation:
      client = rotation.pop(0)
      order.append(queues[client].pop(0))
      if queues[client]:
        rotation.append(client)
    return order

  def _pick_job_locked(self) -> Job | None:
    """Pop the next job per round-robin. Serve rotation head, rotate client."""
    while self._rr_clients:
      client = self._rr_clients.pop(0)
      q = self._pending.get(client)
      if not q:
        self._pending.pop(client, None)
        continue
      job_id = q.popleft()
      if q:
        self._rr_clients.append(client)
      else:
        self._pending.pop(client, None)
      return self._jobs[job_id]
    return None

  def _estimated_remaining_locked(self, job: Job, now: float | None = None) -> int:
    now = now or time.time()

    if job.status == "done":
      return 0

    per_iter = max(1.0, job.per_iter_estimate_sec)
    if job.status == "queued":
      if job.mode == "open" and job.timeout <= 0:
        return 0
      return int(round(job.repeat * per_iter))

    if job.mode == "open":
      if job.stop_requested_at is not None:
        grace_elapsed = max(0.0, now - job.stop_requested_at)
        return max(0, int(round(STOP_GRACE_SEC - grace_elapsed)))
      if job.timeout <= 0:
        return 0
      deadline = (job.started_at or now) + job.timeout
      return max(0, int(round(deadline - now)))

    current_started = job.current_iteration_started_at or job.started_at or now
    current_elapsed = max(0.0, now - current_started)
    remaining_current = max(0.0, per_iter - current_elapsed)
    remaining_after = max(0, job.repeat - job.repeat_current) * per_iter
    return int(round(remaining_current + remaining_after))

  def _estimate_wait_locked(self, pending_ids: list[str], include_current: bool) -> int:
    now = time.time()
    total = 0
    if include_current and self._current is not None:
      total += self._estimated_remaining_locked(self._current, now=now)
    for jid in pending_ids:
      total += self._estimated_remaining_locked(self._jobs[jid], now=now)
    return total

  def estimated_remaining(self, job: Job) -> int:
    with self._lock:
      return self._estimated_remaining_locked(job)

  def queue_position(self, job_id: str) -> tuple[int, int]:
    """(0-indexed dispatch position, estimated wait sec). (-1, 0) if not pending."""
    with self._lock:
      order = self._dispatch_order_locked()
      try:
        pos = order.index(job_id)
      except ValueError:
        return -1, 0
      return pos, self._estimate_wait_locked(order[:pos], include_current=True)

  def submit(
      self,
      cmd: str,
      cwd: str,
      timeout: int,
      repeat: int,
      mode: str = "run",
      env: dict[str, str] | None = None,
      client_id: str = DEFAULT_CLIENT_ID,
  ) -> tuple["Job", int, int]:
    """Submit a job. Returns (job, position, estimated_wait_sec) computed atomically."""
    if repeat < 1:
      raise ValueError("repeat must be >= 1")
    if mode not in ("run", "open"):
      raise ValueError("mode must be 'run' or 'open'")
    if mode == "open" and repeat != 1:
      raise ValueError("open jobs do not support repeat")
    if env is None:
      env = {}
    if not isinstance(env, dict):
      raise ValueError("env must be an object mapping names to values")
    for key, value in env.items():
      if not isinstance(key, str) or not key:
        raise ValueError("env names must be non-empty strings")
      if "=" in key:
        raise ValueError("env names must not contain '='")
      if not isinstance(value, str):
        raise ValueError("env values must be strings")
    if not isinstance(client_id, str) or not client_id.strip():
      raise ValueError("client_id must be a non-empty string")
    client_id = client_id.strip()
    if len(client_id) > MAX_CLIENT_ID_LEN:
      raise ValueError(f"client_id must be at most {MAX_CLIENT_ID_LEN} characters")

    job_id = uuid.uuid4().hex[:8]
    output_dir = LOG_DIR / job_id
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = str(output_dir / "output")

    job = Job(
      id=job_id, cmd=cmd, cwd=cwd, timeout=timeout, repeat=repeat, mode=mode,
      env=dict(env), client_id=client_id, output_file=output_file,
    )

    with self._cond:
      if self._device_state == "dead":
        raise DeviceDeadError(self._dead_reason or REBOOT_REQUIRED_MSG)
      job.reset_epoch = self._reset_epoch
      self._jobs[job_id] = job
      if client_id not in self._pending:
        self._pending[client_id] = deque()
        self._rr_clients.append(client_id)
      self._pending[client_id].append(job_id)
      # Compute position while still holding the lock, before the worker can dequeue
      order = self._dispatch_order_locked()
      pos = order.index(job_id)
      jobs_ahead = pos + (1 if self._current else 0)
      wait_sec = self._estimate_wait_locked(order[:pos], include_current=True)
      self._store.save_job(job)
      self._cond.notify_all()

    return job, jobs_ahead, wait_sec

  def cancel_job(self, job_id: str) -> dict:
    """Cancel a queued (not yet running) job.

    Raises KeyError for unknown jobs and ValueError for non-queued jobs.
    """
    with self._cond:
      job = self._jobs.get(job_id)
      if job is None:
        if self._store.load_job(job_id) is None:
          raise KeyError(job_id)
        raise ValueError(f"Job {job_id} is not queued")
      if job.status != "queued":
        raise ValueError(
          f"Job {job_id} is not queued (status: {job.status}); use /kill for running jobs"
        )
      q = self._pending.get(job.client_id)
      if q is not None:
        try:
          q.remove(job_id)
        except ValueError:
          pass

    # Job can no longer be picked by the worker — finish it outside the lock.
    try:
      with open(job.output_file, "ab") as f:
        self._append_output(job, f, b"\n[tt-device-queue] Cancelled while queued\n")
    except OSError:
      pass
    with self._cond:
      job.status = "done"
      job.exit_code = -1
      job.finished_at = time.time()
      self._store.save_job(job)
    return {"id": job.id, "cmd": job.cmd[:120], "client": job.client_id}

  def request_reset(self, job_id: str | None = None) -> dict:
    """Request a device reset, coalescing duplicates via reset epochs.

    Raises DeviceDeadError when the device is dead and KeyError for unknown jobs.
    """
    with self._cond:
      if self._device_state == "dead":
        raise DeviceDeadError(self._dead_reason or REBOOT_REQUIRED_MSG)
      epoch = self._reset_epoch
      if job_id:
        job = self._jobs.get(job_id) or self._store.load_job(job_id)
        if job is None:
          raise KeyError(job_id)
        if job.reset_epoch < epoch:
          return {
            "action": "already_reset",
            "device_state": self._device_state,
            "reset_epoch": epoch,
            "hint": "Device was already reset after this job ran. Just resubmit your job.",
          }
      if self._device_state == "resetting" or self._reset_pending:
        return {
          "action": "joined",
          "device_state": self._device_state,
          "reset_epoch": epoch,
          "hint": "A reset is already pending or in progress. Wait for it, then resubmit.",
        }
      self._reset_pending = True
      self._cond.notify_all()
      return {
        "action": "scheduled",
        "device_state": self._device_state,
        "reset_epoch": epoch,
        "hint": "Reset will run before the next job. Resubmit once the device is healthy.",
      }

  def get_job(self, job_id: str) -> Job | None:
    job = self._jobs.get(job_id)
    if job is not None:
      return job
    stored = self._store.load_job(job_id)
    if stored is not None and stored.status == "done":
      self._jobs[job_id] = stored
      return stored
    return None

  def position_of(self, job_id: str) -> int:
    """0-indexed position in the predicted dispatch order. -1 if not pending."""
    with self._lock:
      try:
        return self._dispatch_order_locked().index(job_id)
      except ValueError:
        return -1

  def queue_length(self) -> int:
    with self._lock:
      return sum(len(q) for q in self._pending.values())

  def _device_status_locked(self) -> dict:
    return {
      "state": self._device_state,
      "reset_epoch": self._reset_epoch,
      "reset_pending": self._reset_pending,
      "last_reset_at": _format_timestamp(self._last_reset_at),
      "dead_since": _format_timestamp(self._dead_since),
      "dead_reason": self._dead_reason,
    }

  def device_status(self) -> dict:
    with self._lock:
      return self._device_status_locked()

  def status(self) -> dict:
    with self._lock:
      current = None
      if self._current:
        j = self._current
        current = {
          "id": j.id, "cmd": j.cmd[:120],
          "client": j.client_id,
          "running_sec": round(time.time() - (j.started_at or j.submitted), 1),
          "estimated_remaining_sec": self._estimated_remaining_locked(j),
          "repeat": j.repeat,
          "mode": j.mode,
          "repeat_current": j.repeat_current,
          "repeat_completed": j.repeat_completed,
        }
      pending = []
      order = self._dispatch_order_locked()
      for index, jid in enumerate(order):
        j = self._jobs[jid]
        pending.append({
          "id": j.id, "cmd": j.cmd[:120],
          "client": j.client_id,
          "waiting_sec": round(time.time() - j.submitted, 1),
          "estimated_wait_sec": self._estimate_wait_locked(order[:index], include_current=True),
          "estimated_run_sec": self._estimated_remaining_locked(j),
          "repeat": j.repeat,
          "mode": j.mode,
        })
      return {
        "current": current,
        "pending": pending,
        "recent": self._store.recent_completed(10),
        "device": self._device_status_locked(),
      }

  def snapshot(self, job: Job) -> dict:
    with self._lock:
      position = None
      estimated_wait_sec = None
      running_sec = None
      estimated_remaining_sec = None
      if job.status == "queued":
        order = self._dispatch_order_locked()
        try:
          pos = order.index(job.id)
        except ValueError:
          pos = -1
        position = pos + 1
        estimated_wait_sec = self._estimate_wait_locked(order[:max(pos, 0)], include_current=True)
        estimated_remaining_sec = self._estimated_remaining_locked(job)
      elif job.status == "running":
        running_sec = round(time.time() - (job.started_at or job.submitted), 1)
        position = 0
        estimated_wait_sec = 0
        estimated_remaining_sec = self._estimated_remaining_locked(job)
      elif job.status == "done":
        estimated_remaining_sec = 0

      data = {
        "job_id": job.id,
        "status": job.status,
        "client_id": job.client_id,
        "cmd": job.cmd,
        "cwd": job.cwd,
        "timeout": job.timeout,
        "repeat": job.repeat,
        "mode": job.mode,
        "repeat_current": job.repeat_current,
        "repeat_completed": job.repeat_completed,
        "first_iteration_elapsed": job.first_iteration_elapsed,
        "per_iter_estimate_sec": round(job.per_iter_estimate_sec, 2),
        "submitted_at": _format_timestamp(job.submitted),
        "started_at": _format_timestamp(job.started_at),
        "finished_at": _format_timestamp(job.finished_at),
        "output_file": job.output_file,
        "exit_code": job.exit_code,
        "elapsed": job.elapsed,
      }

      if position is not None:
        data["position"] = position
      if estimated_wait_sec is not None:
        data["estimated_wait_sec"] = estimated_wait_sec
      if estimated_remaining_sec is not None:
        data["estimated_remaining_sec"] = estimated_remaining_sec
      if running_sec is not None:
        data["running_sec"] = running_sec
      if job.stop_requested_at is not None:
        data["stop_requested_at"] = _format_timestamp(job.stop_requested_at)

      return data

  def read_logs(self, job: Job, offset: int, limit: int) -> dict:
    limit = max(1, min(limit, MAX_LOG_READ))
    offset = max(0, offset)

    data, file_size, truncated = self._store.read_logs(job.id, offset, limit + 1)
    if file_size == 0 and not data:
      try:
        with open(job.output_file, "rb") as f:
          f.seek(offset)
          chunk = f.read(limit + 1)
        file_size = os.path.getsize(job.output_file)
        truncated = len(chunk) > limit
        data = chunk[:limit]
      except FileNotFoundError:
        data = b""
        file_size = 0
        truncated = False
    else:
      truncated = len(data) > limit
      data = data[:limit]
    next_offset = offset + len(data)

    return {
      "job_id": job.id,
      "status": job.status,
      "output_file": job.output_file,
      "offset": offset,
      "next_offset": next_offset,
      "content": data.decode("utf-8", errors="replace"),
      "truncated": truncated,
      "complete": job.status == "done" and next_offset >= file_size,
    }

  def stop_job(self, job_id: str | None = None) -> dict | None:
    """Request a graceful stop for the current running job."""
    with self._lock:
      proc = self._current_proc
      job = self._current
      if not proc or not job:
        return None
      if job_id and job.id != job_id:
        raise ValueError(f"Job {job_id} is not currently running")
      now = time.time()
      if job.stop_requested_at is None:
        job.stop_requested_at = now
        self._store.save_job(job)
      info = {"id": job.id, "cmd": job.cmd[:120], "signal": "SIGINT"}

    # Send Ctrl+C-equivalent to the whole process group first.
    self._send_sigint(proc)
    return info

  def kill_current(self) -> dict | None:
    """Force-kill the currently running job immediately. Returns info or None."""
    with self._lock:
      proc = self._current_proc
      job = self._current
      if not proc or not job:
        return None
      info = {"id": job.id, "cmd": job.cmd[:120], "signal": "SIGKILL"}

    try:
      os.killpg(proc.pid, signal.SIGKILL)
    except (ProcessLookupError, PermissionError):
      try:
        proc.kill()
      except (ProcessLookupError, PermissionError):
        pass
    return info

  def _process_group_alive(self, pgid: int) -> bool:
    try:
      os.killpg(pgid, 0)
      return True
    except ProcessLookupError:
      return False
    except PermissionError:
      return True

  def _send_sigint(self, proc: subprocess.Popen):
    try:
      os.killpg(proc.pid, signal.SIGINT)
    except (ProcessLookupError, PermissionError):
      try:
        proc.send_signal(signal.SIGINT)
      except (ProcessLookupError, PermissionError):
        pass

  def _send_sigkill(self, proc: subprocess.Popen):
    try:
      os.killpg(proc.pid, signal.SIGKILL)
    except (ProcessLookupError, PermissionError):
      try:
        proc.kill()
      except (ProcessLookupError, PermissionError):
        pass

  def _interrupt_then_kill(self, proc: subprocess.Popen, grace_sec: float = STOP_GRACE_SEC) -> bool:
    self._send_sigint(proc)
    deadline = time.time() + grace_sec
    while time.time() < deadline:
      if not self._process_group_alive(proc.pid):
        proc.poll()
        return True
      time.sleep(0.1)
    self._send_sigkill(proc)
    proc.wait()
    return False

  def _kill_for_timeout(self, proc: subprocess.Popen):
    self._send_sigkill(proc)
    proc.wait()

  def _append_output(self, job: Job, out_f, data: bytes):
    if not data:
      return
    out_f.write(data)
    out_f.flush()
    with self._lock:
      offset = job.log_size
      job.log_size += len(data)
      log_size = job.log_size
    self._store.append_log(job.id, offset, data, log_size)

  def _drain_process_output(self, job: Job, proc: subprocess.Popen, out_f):
    if proc.stdout is None:
      return
    fd = proc.stdout.fileno()
    while True:
      try:
        data = os.read(fd, 64 * 1024)
      except BlockingIOError:
        return
      if not data:
        return
      self._append_output(job, out_f, data)

  def _read_ready_process_output(self, job: Job, proc: subprocess.Popen, out_f, timeout: float):
    if proc.stdout is None:
      time.sleep(timeout)
      return
    fd = proc.stdout.fileno()
    # select.poll() instead of select.select(): select() raises ValueError for
    # fds >= 1024 (FD_SETSIZE), which would wedge output draining under fd
    # pressure or with a raised RLIMIT_NOFILE.
    poller = select.poll()
    poller.register(fd, select.POLLIN)
    if not poller.poll(max(0, int(timeout * 1000))):
      return
    self._drain_process_output(job, proc, out_f)

  def _wait_for_process(self, job: Job, proc: subprocess.Popen, out_f, deadline: float | None) -> int:
    if proc.stdout is not None:
      os.set_blocking(proc.stdout.fileno(), False)

    while True:
      timeout_remaining = None
      if deadline is not None:
        timeout_remaining = deadline - time.time()
        if timeout_remaining <= 0:
          raise subprocess.TimeoutExpired(job.cmd, job.timeout)

      poll_window = 0.2
      if timeout_remaining is not None:
        poll_window = min(poll_window, max(0.01, timeout_remaining))

      self._read_ready_process_output(job, proc, out_f, poll_window)

      ret = proc.poll()
      if ret is not None:
        self._drain_process_output(job, proc, out_f)
        with self._lock:
          stop_requested_at = job.stop_requested_at
          stop_escalated_at = job.stop_escalated_at
        if stop_requested_at is not None and self._process_group_alive(proc.pid):
          if stop_escalated_at is None and time.time() - stop_requested_at >= STOP_GRACE_SEC:
            with self._lock:
              if job.stop_escalated_at is None:
                job.stop_escalated_at = time.time()
                self._store.save_job(job)
            self._send_sigkill(proc)
          time.sleep(0.1)
          continue
        if stop_escalated_at is not None:
          return -9
        return ret

      should_kill = False
      with self._lock:
        if job.stop_requested_at is not None and job.stop_escalated_at is None:
          if time.time() - job.stop_requested_at >= STOP_GRACE_SEC:
            job.stop_escalated_at = time.time()
            self._store.save_job(job)
            should_kill = True

      if should_kill:
        self._send_sigkill(proc)

  def _next_work(self) -> tuple[str, Job | None]:
    """Block until there is work. Returns ("reset", None) or ("job", job)."""
    with self._cond:
      while True:
        if self._reset_pending and self._device_state != "dead":
          self._reset_pending = False
          self._device_state = "resetting"
          return "reset", None
        if self._device_state == "healthy":
          job = self._pick_job_locked()
          if job is not None:
            job.status = "running"
            job.started_at = time.time()
            job.reset_epoch = self._reset_epoch
            self._current = job
            self._store.save_job(job)
            return "job", job
        self._cond.wait()

  def _run_logged_command(self, job: Job, out_f, cmd: str, timeout: int) -> int:
    """Run a health command (reset/probe), appending its output to the job log."""
    self._append_output(job, out_f, f"[tt-device-queue] $ {cmd}\n".encode())
    try:
      proc = subprocess.run(
        ["/bin/bash", "-lc", cmd],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        timeout=timeout, env=_job_env(),
      )
    except subprocess.TimeoutExpired as exc:
      if exc.stdout:
        self._append_output(job, out_f, exc.stdout)
      self._append_output(
        job, out_f,
        f"[tt-device-queue] Command timed out after {timeout}s\n".encode(),
      )
      return -9
    self._append_output(job, out_f, proc.stdout or b"")
    if proc.returncode != 0:
      self._append_output(
        job, out_f, f"[tt-device-queue] exit {proc.returncode}\n".encode()
      )
    return proc.returncode

  def _drain_pending(self, message: str):
    """Fail every queued job with `message` appended to its log."""
    with self._cond:
      drained = [
        self._jobs[jid]
        for q in self._pending.values()
        for jid in q
      ]
      self._pending.clear()
      self._rr_clients.clear()
      self._cond.notify_all()

    for job in drained:
      try:
        with open(job.output_file, "ab") as f:
          self._append_output(job, f, f"\n[tt-device-queue] {message}\n".encode())
      except OSError:
        pass
      with self._cond:
        job.status = "done"
        job.exit_code = -1
        job.finished_at = time.time()
        self._store.save_job(job)

  def _execute_reset(self):
    """Run the reset + probe sequence. Transitions to healthy or dead."""
    job_id = uuid.uuid4().hex[:8]
    output_dir = LOG_DIR / job_id
    output_dir.mkdir(parents=True, exist_ok=True)
    job = Job(
      id=job_id, cmd=f"[device reset] {RESET_CMD}", cwd="",
      timeout=HEALTH_CMD_TIMEOUT, repeat=1, mode="reset", client_id="system",
      output_file=str(output_dir / "output"),
    )
    job.status = "running"
    job.started_at = time.time()
    with self._lock:
      self._jobs[job_id] = job
      self._current = job
      self._store.save_job(job)

    print(f"[{job_id}] Device reset starting")
    healthy = False
    attempts = max(1, RESET_RETRIES + 1)
    try:
      with open(job.output_file, "wb") as out_f:
        for attempt in range(1, attempts + 1):
          self._append_output(
            job, out_f,
            f"[tt-device-queue] Reset attempt {attempt}/{attempts}\n".encode(),
          )
          self._run_logged_command(job, out_f, RESET_CMD, HEALTH_CMD_TIMEOUT)
          probe_rc = self._run_logged_command(job, out_f, PROBE_CMD, HEALTH_CMD_TIMEOUT)
          if probe_rc == 0:
            healthy = True
            break
        if healthy:
          self._append_output(
            job, out_f, b"[tt-device-queue] Device healthy after reset\n"
          )
        else:
          self._append_output(
            job, out_f, f"\n[tt-device-queue] {REBOOT_REQUIRED_MSG}\n".encode()
          )
    except Exception as exc:
      healthy = False
      try:
        with open(job.output_file, "ab") as out_f:
          self._append_output(
            job, out_f, f"\n[tt-device-queue] Reset error: {exc}\n".encode()
          )
      except OSError:
        pass

    now = time.time()
    with self._cond:
      job.status = "done"
      job.exit_code = 0 if healthy else 1
      job.finished_at = now
      job.elapsed = round(now - (job.started_at or now), 2)
      self._current = None
      if healthy:
        self._reset_epoch += 1
        self._device_state = "healthy"
        self._last_reset_at = now
      else:
        self._device_state = "dead"
        self._dead_since = now
        self._dead_reason = REBOOT_REQUIRED_MSG
      self._store.save_job(job)
      self._cond.notify_all()

    if healthy:
      print(f"[{job_id}] Device healthy after reset (epoch {self._reset_epoch})")
    else:
      print(f"[{job_id}] DEVICE DEAD — draining queue, reboot required")
      self._drain_pending(REBOOT_REQUIRED_MSG)

  def worker_loop(self):
    """Runs forever in a dedicated thread. Processes jobs one at a time."""
    while True:
      kind, job = self._next_work()
      if kind == "reset":
        self._execute_reset()
        continue

      print(f"[{job.id}] Running: {job.cmd[:100]}")

      try:
        deadline = job.started_at + job.timeout if job.timeout > 0 else None
        exit_code = 0
        with open(job.output_file, "wb") as out_f:
          iterations = range(1, job.repeat + 1) if job.mode == "run" else range(1, 2)
          for iteration in iterations:
            with self._lock:
              job.repeat_current = iteration
              job.current_iteration_started_at = time.time()
              self._store.save_job(job)

            if job.mode == "open":
              self._append_output(
                job,
                out_f,
                f"[tt-device-queue] Open job started (timeout={job.timeout}s)\n".encode(),
              )
            elif job.repeat > 1:
              self._append_output(
                job,
                out_f,
                f"\n[tt-device-queue] Repeat {iteration}/{job.repeat}\n".encode(),
              )

            proc = subprocess.Popen(
              ["/bin/bash", "-lc", _job_shell_script(job.cmd)],
              stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
              cwd=job.cwd or None,
              env=_job_env(job.env),
              start_new_session=True,  # own process group for clean kills
            )
            with self._lock:
              self._current_proc = proc
              self._store.save_job(job)

            try:
              exit_code = self._wait_for_process(job, proc, out_f, deadline)
            except subprocess.TimeoutExpired:
              self._kill_for_timeout(proc)
              self._drain_process_output(job, proc, out_f)
              exit_code = -9
              self._append_output(
                job,
                out_f,
                f"\n[tt-device-queue] Timed out after {job.timeout}s — sent SIGKILL\n".encode(),
              )
            finally:
              if proc.stdout is not None:
                proc.stdout.close()

            if job.stop_requested_at is not None and exit_code == -2:
              self._append_output(job, out_f, b"\n[tt-device-queue] Stopped with Ctrl+C\n")

            if exit_code != 0:
              break

            iteration_elapsed = time.time() - (job.current_iteration_started_at or time.time())
            with self._lock:
              job.repeat_completed = iteration
              if job.first_iteration_elapsed is None:
                job.first_iteration_elapsed = round(iteration_elapsed, 2)
                job.per_iter_estimate_sec = max(0.1, iteration_elapsed)
              job.current_iteration_started_at = None
              self._store.save_job(job)

          if exit_code == 0:
            with self._lock:
              job.repeat_completed = job.repeat
              job.current_iteration_started_at = None
              self._store.save_job(job)
      except Exception as e:
        exit_code = -1
        # Never let error reporting kill the worker thread (e.g. when the
        # error itself is fd exhaustion and opening the log file also fails).
        try:
          with open(job.output_file, "ab") as f:
            self._append_output(job, f, f"\n[tt-device-queue] Error: {e}\n".encode())
        except Exception as log_exc:
          print(f"[{job.id}] Error: {e} (and failed to record it: {log_exc})")

      elapsed = round(time.time() - job.started_at, 2)

      with self._lock:
        job.status = "done"
        job.exit_code = exit_code
        job.elapsed = elapsed
        job.finished_at = time.time()
        job.current_iteration_started_at = None
        self._current = None
        self._current_proc = None
        self._store.save_job(job)

      # Write metadata alongside output
      try:
        self._write_meta(job, exit_code, elapsed)
      except OSError as exc:
        print(f"[{job.id}] Failed to write meta.json: {exc}")

      status = "OK" if exit_code == 0 else f"FAIL({exit_code})"
      print(f"[{job.id}] {status} in {elapsed}s -> {job.output_file}")

  def _write_meta(self, job: Job, exit_code: int, elapsed: float):
      meta_path = Path(job.output_file).parent / "meta.json"
      with open(meta_path, "w") as f:
        json.dump({
          "id": job.id, "cmd": job.cmd, "cwd": job.cwd,
          "exit_code": exit_code, "elapsed": elapsed, "repeat": job.repeat,
          "mode": job.mode,
          "repeat_completed": job.repeat_completed,
          "first_iteration_elapsed": job.first_iteration_elapsed,
          "per_iter_estimate_sec": round(job.per_iter_estimate_sec, 2),
          "started": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(job.started_at)),
          "finished": _format_timestamp(job.finished_at),
          "output_file": job.output_file,
        }, f, indent=2)


dq = DeviceQueue(JobStore(DB_PATH))


class Handler(BaseHTTPRequestHandler):
  def log_message(self, fmt, *args):
    # Suppress default access log noise
    pass

  def _json_response(self, code: int, data: dict):
    body = json.dumps(data).encode()
    self.send_response(code)
    self.send_header("Content-Type", "application/json")
    self.send_header("Content-Length", str(len(body)))
    self.end_headers()
    self.wfile.write(body)

  def do_GET(self):
    parsed = urlparse(self.path)
    path = parsed.path.rstrip("/")
    query = parse_qs(parsed.query)

    if path == "/status":
      self._json_response(200, dq.status())
      return

    if path.startswith("/job/"):
      job_id = path[len("/job/"):]
      job = dq.get_job(job_id)
      if not job:
        self._json_response(404, {"error": f"Unknown job: {job_id}"})
        return
      self._json_response(200, dq.snapshot(job))
      return

    if path.startswith("/logs/"):
      job_id = path[len("/logs/"):]
      job = dq.get_job(job_id)
      if not job:
        self._json_response(404, {"error": f"Unknown job: {job_id}"})
        return

      try:
        offset = int(query.get("offset", ["0"])[0])
        limit = int(query.get("limit", [str(MAX_LOG_READ)])[0])
      except ValueError:
        self._json_response(400, {"error": "offset and limit must be integers"})
        return

      self._json_response(200, dq.read_logs(job, offset=offset, limit=limit))
      return

    if path.startswith("/result/"):
      job_id = path[len("/result/"):]
      job = dq.get_job(job_id)
      if not job:
        self._json_response(404, {"error": f"Unknown job: {job_id}"})
        return

      if job.status == "done":
        self._json_response(200, {
          "status": "done",
          "exit_code": job.exit_code,
          "output_file": job.output_file,
          "elapsed": job.elapsed,
          "mode": job.mode,
          "estimated_remaining_sec": 0,
          "repeat": job.repeat,
          "repeat_completed": job.repeat_completed,
          "started_at": _format_timestamp(job.started_at),
          "finished_at": _format_timestamp(job.finished_at),
        })
      elif job.status == "running":
        running_for = round(time.time() - (job.started_at or job.submitted), 1)
        self._json_response(200, {
          "status": "running",
          "mode": job.mode,
          "position": 0,
          "estimated_wait_sec": 0,
          "estimated_remaining_sec": dq.estimated_remaining(job),
          "repeat": job.repeat,
          "repeat_current": job.repeat_current,
          "repeat_completed": job.repeat_completed,
          "first_iteration_elapsed": job.first_iteration_elapsed,
          "per_iter_estimate_sec": round(job.per_iter_estimate_sec, 2),
          "started_at": _format_timestamp(job.started_at),
        })
      else:
        pos, wait_sec = dq.queue_position(job_id)
        self._json_response(200, {
          "status": "queued",
          "mode": job.mode,
          "position": pos + 1,  # 1-indexed for humans
          "estimated_wait_sec": wait_sec,
          "estimated_remaining_sec": dq.estimated_remaining(job),
          "repeat": job.repeat,
          "repeat_current": job.repeat_current,
          "repeat_completed": job.repeat_completed,
          "first_iteration_elapsed": job.first_iteration_elapsed,
          "per_iter_estimate_sec": round(job.per_iter_estimate_sec, 2),
          "submitted_at": _format_timestamp(job.submitted),
        })
      return

    self._json_response(404, {"error": "Not found"})

  def _read_json_body(self) -> dict | None:
    length = int(self.headers.get("Content-Length", 0))
    if length == 0:
      return {}
    try:
      return json.loads(self.rfile.read(length))
    except json.JSONDecodeError:
      self._json_response(400, {"error": "Invalid JSON"})
      return None

  def do_POST(self):
    path = self.path.rstrip("/")

    if path == "/kill":
      body = self._read_json_body()
      if body is None:
        return
      job_id = body.get("job_id", "").strip() or None
      try:
        stopped = dq.stop_job(job_id)
      except ValueError as exc:
        self._json_response(409, {"error": str(exc)})
        return
      if stopped:
        self._json_response(200, {"killed": stopped})
      else:
        self._json_response(200, {"error": "Nothing running"})
      return

    if path == "/reset":
      body = self._read_json_body()
      if body is None:
        return
      job_id = (body.get("job_id") or "").strip() or None
      try:
        result = dq.request_reset(job_id)
      except DeviceDeadError as exc:
        self._json_response(503, {"error": str(exc), "device_state": "dead"})
        return
      except KeyError:
        self._json_response(404, {"error": f"Unknown job: {job_id}"})
        return
      self._json_response(200, result)
      return

    if path == "/cancel":
      body = self._read_json_body()
      if body is None:
        return
      job_id = (body.get("job_id") or "").strip()
      if not job_id:
        self._json_response(400, {"error": "Missing 'job_id'"})
        return
      try:
        cancelled = dq.cancel_job(job_id)
      except KeyError:
        self._json_response(404, {"error": f"Unknown job: {job_id}"})
        return
      except ValueError as exc:
        self._json_response(409, {"error": str(exc)})
        return
      self._json_response(200, {"cancelled": cancelled})
      return

    if path == "/queue":
      length = int(self.headers.get("Content-Length", 0))
      if length == 0:
        self._json_response(400, {"error": "Empty body"})
        return
      try:
        body = json.loads(self.rfile.read(length))
      except json.JSONDecodeError:
        self._json_response(400, {"error": "Invalid JSON"})
        return

      cmd = body.get("cmd", "").strip()
      mode = body.get("mode", "run")
      if not cmd:
        self._json_response(400, {"error": "Missing 'cmd'"})
        return

      timeout = body.get("timeout")
      if timeout is None:
        timeout = DEFAULT_OPEN_TIMEOUT if mode == "open" else DEFAULT_TIMEOUT

      try:
        job, jobs_ahead, wait_sec = dq.submit(
          cmd=cmd,
          cwd=body.get("cwd", ""),
          timeout=timeout,
          repeat=body.get("repeat", 1),
          mode=mode,
          env=body.get("env", {}),
          client_id=body.get("client_id", DEFAULT_CLIENT_ID),
        )
      except DeviceDeadError as exc:
        self._json_response(503, {"error": str(exc), "device_state": "dead"})
        return
      except ValueError as exc:
        self._json_response(400, {"error": str(exc)})
        return

      self._json_response(200, {
        "job_id": job.id,
        "output_file": job.output_file,
        "position": jobs_ahead,
        "estimated_wait_sec": wait_sec,
        "estimated_run_sec": dq.estimated_remaining(job),
        "repeat": job.repeat,
        "mode": job.mode,
        "timeout": job.timeout,
      })
      return

    self._json_response(404, {"error": "Not found"})


def _raise_nofile_limit(target: int = 65536):
  """Raise the soft RLIMIT_NOFILE (often 1024 under systemd) toward `target`."""
  try:
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    wanted = target if hard == resource.RLIM_INFINITY else min(hard, target)
    if soft < wanted:
      resource.setrlimit(resource.RLIMIT_NOFILE, (wanted, hard))
  except (ValueError, OSError) as exc:
    print(f"Could not raise RLIMIT_NOFILE: {exc}")


def main():
  _raise_nofile_limit()
  # Start worker thread
  worker = threading.Thread(target=dq.worker_loop, daemon=True)
  worker.start()

  server = HTTPServer((HOST, PORT), Handler)
  print(f"tt-device-queue listening on http://{HOST}:{PORT}")
  print(f"Default timeout: {DEFAULT_TIMEOUT}s")
  print(f"Output dir: {LOG_DIR}")
  print(f"SQLite db: {DB_PATH}")
  try:
    server.serve_forever()
  except KeyboardInterrupt:
    print("\nShutting down...")
    server.shutdown()


if __name__ == "__main__":
  main()
