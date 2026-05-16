#!/usr/bin/env python3
"""
tt-device-queue server — serializes access to the Tenstorrent device.

HTTP API on localhost:5741. Jobs run one at a time (FIFO).
Output is saved to /tmp/tt-device-logs/<job_id>/output.

Endpoints:
  POST /queue   {"cmd": "...", "cwd": "...", "timeout": 120, "repeat": 1}
                -> {"job_id", "output_file", "position", "estimated_wait_sec"}

  GET  /result/<job_id>
                -> {"status": "queued|running|done", "position", "estimated_wait_sec"}
                   or {"status": "done", "exit_code", "output_file", "elapsed"}

  GET  /job/<job_id>
                -> full job metadata including repeat progress, timestamps, and
                   queue position when still pending

  GET  /status  -> {"current", "pending", "recent"}
"""

import json
import os
import signal
import subprocess
import threading
import time
import uuid
from http.server import HTTPServer, BaseHTTPRequestHandler
from pathlib import Path
from queue import Queue
from dataclasses import dataclass, field
from urllib.parse import parse_qs, urlparse

HOST = os.environ.get("TT_DEVICE_HOST", "127.0.0.1")
PORT = int(os.environ.get("TT_DEVICE_PORT", "5741"))
DEFAULT_TIMEOUT = int(os.environ.get("TT_DEVICE_TIMEOUT", "120"))
DEFAULT_OPEN_TIMEOUT = int(os.environ.get("TT_DEVICE_OPEN_TIMEOUT", "180"))
LOG_DIR = Path(os.environ.get("TT_DEVICE_LOG_DIR", "/tmp/tt-device-logs"))
DEFAULT_ITER_ESTIMATE_SEC = 10
MAX_LOG_READ = 64 * 1024
STOP_GRACE_SEC = 8

LOG_DIR.mkdir(parents=True, exist_ok=True)


def _job_env() -> dict[str, str]:
  env = os.environ.copy()
  pythonpath = env.get("PYTHONPATH", "")
  parts = [part for part in pythonpath.split(os.pathsep) if part]
  if "." not in parts:
    env["PYTHONPATH"] = os.pathsep.join(["."] + parts)
  return env


def _format_timestamp(ts: float | None) -> str | None:
  if ts is None:
    return None
  return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts))


@dataclass
class Job:
  id: str
  cmd: str
  cwd: str
  timeout: int
  repeat: int
  mode: str = "run"
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


class DeviceQueue:
  def __init__(self):
    self._queue: Queue[Job] = Queue()
    self._jobs: dict[str, Job] = {}          # all jobs by id
    self._pending_ids: list[str] = []        # ordered list of queued job ids
    self._current: Job | None = None
    self._current_proc: subprocess.Popen | None = None
    self._history: list[dict] = []
    self._lock = threading.Lock()

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

  def estimated_wait_for_position(self, position: int) -> int:
    with self._lock:
      pending_slice = self._pending_ids[:max(position, 0)]
      return self._estimate_wait_locked(pending_slice, include_current=True)

  def submit(self, cmd: str, cwd: str, timeout: int, repeat: int, mode: str = "run") -> tuple["Job", int, int]:
    """Submit a job. Returns (job, position, estimated_wait_sec) computed atomically."""
    if repeat < 1:
      raise ValueError("repeat must be >= 1")
    if mode not in ("run", "open"):
      raise ValueError("mode must be 'run' or 'open'")
    if mode == "open" and repeat != 1:
      raise ValueError("open jobs do not support repeat")

    job_id = uuid.uuid4().hex[:8]
    output_dir = LOG_DIR / job_id
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = str(output_dir / "output")

    job = Job(
      id=job_id, cmd=cmd, cwd=cwd, timeout=timeout, repeat=repeat, mode=mode,
      output_file=output_file,
    )

    with self._lock:
      self._jobs[job_id] = job
      self._pending_ids.append(job_id)
      # Compute position while still holding the lock, before the worker can dequeue
      pos = self._pending_ids.index(job_id)
      jobs_ahead = pos + (1 if self._current else 0)
      wait_sec = self._estimate_wait_locked(self._pending_ids[:pos], include_current=True)

    self._queue.put(job)
    return job, jobs_ahead, wait_sec

  def get_job(self, job_id: str) -> Job | None:
    return self._jobs.get(job_id)

  def position_of(self, job_id: str) -> int:
    """0-indexed position in the pending queue. -1 if not pending."""
    with self._lock:
      try:
        return self._pending_ids.index(job_id)
      except ValueError:
        return -1

  def queue_length(self) -> int:
    with self._lock:
      return len(self._pending_ids)

  def status(self) -> dict:
    with self._lock:
      current = None
      if self._current:
        j = self._current
        current = {
          "id": j.id, "cmd": j.cmd[:120],
          "running_sec": round(time.time() - (j.started_at or j.submitted), 1),
          "estimated_remaining_sec": self._estimated_remaining_locked(j),
          "repeat": j.repeat,
          "mode": j.mode,
          "repeat_current": j.repeat_current,
          "repeat_completed": j.repeat_completed,
        }
      pending = []
      for index, jid in enumerate(self._pending_ids):
        j = self._jobs[jid]
        pending.append({
          "id": j.id, "cmd": j.cmd[:120],
          "waiting_sec": round(time.time() - j.submitted, 1),
          "estimated_wait_sec": self._estimate_wait_locked(self._pending_ids[:index], include_current=True),
          "estimated_run_sec": self._estimated_remaining_locked(j),
          "repeat": j.repeat,
          "mode": j.mode,
        })
      return {
        "current": current,
        "pending": pending,
        "recent": self._history[-10:],
      }

  def snapshot(self, job: Job) -> dict:
    with self._lock:
      position = None
      estimated_wait_sec = None
      running_sec = None
      estimated_remaining_sec = None
      if job.status == "queued":
        try:
          pos = self._pending_ids.index(job.id)
        except ValueError:
          pos = -1
        position = pos + 1
        estimated_wait_sec = self._estimate_wait_locked(self._pending_ids[:max(pos, 0)], include_current=True)
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

    try:
      with open(job.output_file, "rb") as f:
        f.seek(offset)
        chunk = f.read(limit + 1)
      file_size = os.path.getsize(job.output_file)
    except FileNotFoundError:
      chunk = b""
      file_size = 0

    truncated = len(chunk) > limit
    data = chunk[:limit]
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

  def _wait_for_process(self, job: Job, proc: subprocess.Popen, deadline: float | None) -> int:
    while True:
      timeout_remaining = None
      if deadline is not None:
        timeout_remaining = deadline - time.time()
        if timeout_remaining <= 0:
          raise subprocess.TimeoutExpired(job.cmd, job.timeout)

      poll_window = 0.2
      if timeout_remaining is not None:
        poll_window = min(poll_window, max(0.01, timeout_remaining))

      try:
        proc.wait(timeout=poll_window)
      except subprocess.TimeoutExpired:
        pass

      ret = proc.poll()
      if ret is not None:
        with self._lock:
          stop_requested_at = job.stop_requested_at
          stop_escalated_at = job.stop_escalated_at
        if stop_requested_at is not None and self._process_group_alive(proc.pid):
          if stop_escalated_at is None and time.time() - stop_requested_at >= STOP_GRACE_SEC:
            with self._lock:
              if job.stop_escalated_at is None:
                job.stop_escalated_at = time.time()
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
            should_kill = True

      if should_kill:
        self._send_sigkill(proc)

  def worker_loop(self):
    """Runs forever in a dedicated thread. Processes jobs one at a time."""
    while True:
      job = self._queue.get()

      with self._lock:
        job.status = "running"
        job.started_at = time.time()
        self._current = job
        if job.id in self._pending_ids:
          self._pending_ids.remove(job.id)

      print(f"[{job.id}] Running: {job.cmd[:100]}")

      try:
        deadline = job.started_at + job.timeout if job.timeout > 0 else None
        exit_code = 0
        with open(job.output_file, "w") as out_f:
          iterations = range(1, job.repeat + 1) if job.mode == "run" else range(1, 2)
          for iteration in iterations:
            with self._lock:
              job.repeat_current = iteration
              job.current_iteration_started_at = time.time()

            if job.mode == "open":
              out_f.write(f"[claude-collide] Open job started (timeout={job.timeout}s)\n")
              out_f.flush()
            elif job.repeat > 1:
              out_f.write(f"\n[claude-collide] Repeat {iteration}/{job.repeat}\n")
              out_f.flush()

            proc = subprocess.Popen(
              f"exec {job.cmd}", shell=True, executable="/bin/bash",
              stdout=out_f, stderr=subprocess.STDOUT,
              cwd=job.cwd or None,
              env=_job_env(),
              start_new_session=True,  # own process group for clean kills
            )
            with self._lock:
              self._current_proc = proc

            try:
              exit_code = self._wait_for_process(job, proc, deadline)
            except subprocess.TimeoutExpired:
              self._kill_for_timeout(proc)
              exit_code = -9
              out_f.write(f"\n[claude-collide] Timed out after {job.timeout}s — sent SIGKILL\n")
            finally:
              out_f.flush()

            if job.stop_requested_at is not None and exit_code == -2:
              out_f.write("\n[claude-collide] Stopped with Ctrl+C\n")
              out_f.flush()

            if exit_code != 0:
              break

            iteration_elapsed = time.time() - (job.current_iteration_started_at or time.time())
            with self._lock:
              job.repeat_completed = iteration
              if job.first_iteration_elapsed is None:
                job.first_iteration_elapsed = round(iteration_elapsed, 2)
                job.per_iter_estimate_sec = max(0.1, iteration_elapsed)
              job.current_iteration_started_at = None

          if exit_code == 0:
            with self._lock:
              job.repeat_completed = job.repeat
              job.current_iteration_started_at = None
      except Exception as e:
        exit_code = -1
        with open(job.output_file, "a") as f:
          f.write(f"\n[claude-collide] Error: {e}\n")

      elapsed = round(time.time() - job.started_at, 2)

      with self._lock:
        job.status = "done"
        job.exit_code = exit_code
        job.elapsed = elapsed
        job.finished_at = time.time()
        job.current_iteration_started_at = None
        self._current = None
        self._current_proc = None
        self._history.append({
          "id": job.id, "cmd": job.cmd[:120],
          "exit_code": exit_code, "elapsed": elapsed,
          "finished": time.strftime("%H:%M:%S"),
          "output_file": job.output_file,
          "repeat": job.repeat,
          "mode": job.mode,
          "repeat_completed": job.repeat_completed,
          "per_iter_estimate_sec": round(job.per_iter_estimate_sec, 2),
        })
        if len(self._history) > 50:
          self._history = self._history[-50:]

      # Write metadata alongside output
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

      status = "OK" if exit_code == 0 else f"FAIL({exit_code})"
      print(f"[{job.id}] {status} in {elapsed}s -> {job.output_file}")
      self._queue.task_done()


dq = DeviceQueue()


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
        pos = dq.position_of(job_id)
        self._json_response(200, {
          "status": "queued",
          "mode": job.mode,
          "position": pos + 1,  # 1-indexed for humans
          "estimated_wait_sec": dq.estimated_wait_for_position(pos),
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
        )
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


def main():
  # Start worker thread
  worker = threading.Thread(target=dq.worker_loop, daemon=True)
  worker.start()

  server = HTTPServer((HOST, PORT), Handler)
  print(f"tt-device-queue listening on http://{HOST}:{PORT}")
  print(f"Default timeout: {DEFAULT_TIMEOUT}s")
  print(f"Output dir: {LOG_DIR}")
  try:
    server.serve_forever()
  except KeyboardInterrupt:
    print("\nShutting down...")
    server.shutdown()


if __name__ == "__main__":
  main()
