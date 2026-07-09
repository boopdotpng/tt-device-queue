import json
import os
import shlex
import socket
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest
import urllib.error
import urllib.request
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
SERVER_PATH = REPO_ROOT / "server.py"
DEEP_RESET_HELPER = REPO_ROOT / "tt-pci-deep-reset"
POLL_INTERVAL = 0.01


def free_port() -> int:
  with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
    sock.bind(("127.0.0.1", 0))
    return sock.getsockname()[1]


class QueueServerTestBase(unittest.TestCase):
  def setUp(self):
    self.temp_dir = tempfile.TemporaryDirectory()
    self.port = free_port()
    self.server_env = os.environ.copy()
    self.server_env["TT_DEVICE_PORT"] = str(self.port)
    self.server_env["TT_DEVICE_LOG_DIR"] = self.temp_dir.name
    self.server_env["TT_DEVICE_PROCESS_POLL_INTERVAL"] = "0.02"
    self.server_env.pop("PYTHONPATH", None)
    self.server_env.update(self.extra_server_env())
    self._start_server()

  def extra_server_env(self) -> dict:
    return {}

  def _start_server(self):
    self.server = subprocess.Popen(
      [sys.executable, str(SERVER_PATH)],
      cwd=REPO_ROOT,
      env=self.server_env,
      stdout=subprocess.PIPE,
      stderr=subprocess.STDOUT,
      text=True,
    )
    self.base = f"http://127.0.0.1:{self.port}"
    self._wait_for_server()

  def tearDown(self):
    self._stop_server()
    self.temp_dir.cleanup()

  def _stop_server(self):
    if self.server.poll() is None:
      self.server.terminate()
      try:
        self.server.wait(timeout=5)
      except subprocess.TimeoutExpired:
        self.server.kill()
        self.server.wait(timeout=5)
    if self.server.stdout is not None:
      self.server.stdout.close()

  def _wait_for_server(self):
    deadline = time.time() + 5
    last_error = None
    while time.time() < deadline:
      try:
        self.get_json("/status")
        return
      except Exception as exc:  # pragma: no cover - best effort startup loop
        last_error = exc
        time.sleep(POLL_INTERVAL)
    output = ""
    if self.server.stdout is not None:
      output = self.server.stdout.read()
    raise AssertionError(f"server did not start: {last_error}\n{output}")

  def get_json(self, path: str) -> dict:
    with urllib.request.urlopen(f"{self.base}{path}", timeout=5) as resp:
      return json.loads(resp.read())

  def post_json(self, path: str, payload: dict) -> dict:
    req = urllib.request.Request(
      f"{self.base}{path}",
      data=json.dumps(payload).encode(),
      headers={"Content-Type": "application/json"},
      method="POST",
    )
    with urllib.request.urlopen(req, timeout=5) as resp:
      return json.loads(resp.read())

  def post_status(self, path: str, payload: dict) -> tuple[int, dict]:
    """POST that returns (status_code, body) instead of raising on HTTP errors."""
    req = urllib.request.Request(
      f"{self.base}{path}",
      data=json.dumps(payload).encode(),
      headers={"Content-Type": "application/json"},
      method="POST",
    )
    try:
      with urllib.request.urlopen(req, timeout=5) as resp:
        return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as exc:
      return exc.code, json.loads(exc.read())

  def submit(
      self,
      cmd: str,
      timeout: int = 5,
      repeat: int = 1,
      env: dict | None = None,
      client: str | None = None,
  ) -> dict:
    payload = {
      "cmd": cmd,
      "cwd": str(REPO_ROOT),
      "timeout": timeout,
      "repeat": repeat,
      "env": env or {},
    }
    if client is not None:
      payload["client_id"] = client
    return self.post_json("/queue", payload)

  def wait_for_done(self, job_id: str, timeout: float = 10.0) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
      result = self.get_json(f"/result/{job_id}")
      if result["status"] == "done":
        return result
      time.sleep(POLL_INTERVAL)
    raise AssertionError(f"job {job_id} did not finish in time")

  def wait_for_logs(self, job_id: str, needle: str, timeout: float = 5.0) -> str:
    deadline = time.time() + timeout
    while time.time() < deadline:
      logs = self.get_json(f"/logs/{job_id}?offset=0&limit=4096")
      content = logs.get("content", "")
      if needle in content:
        return content
      time.sleep(POLL_INTERVAL)
    raise AssertionError(f"log output for {job_id} did not contain {needle!r}")

  def python_cmd(self, code: str) -> str:
    return f"{shlex.quote(sys.executable)} -c {shlex.quote(code)}"

  def seq_cmd(self, name: str, seq_file: Path) -> str:
    return f"/bin/sh -c {shlex.quote(f'echo {name} >> {seq_file}')}"

  def gate_cmd(self, gate_file: Path) -> str:
    return " ".join([
      "/bin/sh",
      "-c",
      shlex.quote(f"while [ ! -e {shlex.quote(str(gate_file))} ]; do sleep 0.01; done"),
    ])

  def wait_for_running(self, job_id: str, timeout: float = 5.0) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
      job = self.get_json(f"/job/{job_id}")
      if job["status"] == "running":
        return job
      time.sleep(POLL_INTERVAL)
    raise AssertionError(f"job {job_id} did not start running in time")

  def wait_for_device(self, state: str, epoch: int | None = None, timeout: float = 8.0) -> dict:
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
      last = self.get_json("/status")["device"]
      if last["state"] == state and (epoch is None or last["reset_epoch"] == epoch):
        return last
      time.sleep(POLL_INTERVAL)
    raise AssertionError(f"device never reached {state} (epoch {epoch}): {last}")


class DeepResetHelperTest(unittest.TestCase):
  def test_help_does_not_attempt_reset(self):
    proc = subprocess.run(
      [str(DEEP_RESET_HELPER), "--help"],
      stdout=subprocess.PIPE,
      stderr=subprocess.STDOUT,
      text=True,
      check=False,
    )
    self.assertEqual(proc.returncode, 0)
    self.assertIn("--queue-server-pid", proc.stdout)

  def test_sudo_invocation_requires_queue_server_parent(self):
    env = os.environ.copy()
    env["SUDO_USER"] = "agent"
    proc = subprocess.run(
      [str(DEEP_RESET_HELPER), "0000:01:00.0"],
      stdout=subprocess.PIPE,
      stderr=subprocess.STDOUT,
      text=True,
      env=env,
      check=False,
    )
    self.assertEqual(proc.returncode, 3)
    self.assertIn("use reset(job_id)", proc.stdout)


class QueueServerTest(QueueServerTestBase):
  def test_repeat_success_uses_one_job_and_one_output_file(self):
    submit = self.submit(self.python_cmd("print('ok')"), repeat=3)
    self.assertEqual(submit["repeat"], 3)
    self.assertEqual(submit["estimated_run_sec"], 30)

    result = self.wait_for_done(submit["job_id"])
    self.assertEqual(result["exit_code"], 0)
    self.assertEqual(result["repeat"], 3)
    self.assertEqual(result["repeat_completed"], 3)

    job = self.get_json(f"/job/{submit['job_id']}")
    self.assertEqual(job["status"], "done")
    self.assertEqual(job["repeat_completed"], 3)
    self.assertEqual(job["output_file"], submit["output_file"])

    logs = self.get_json(f"/logs/{submit['job_id']}?offset=0&limit=4096")
    self.assertEqual(logs["output_file"], submit["output_file"])
    self.assertEqual(logs["content"].count("[tt-device-queue] Repeat"), 3)
    self.assertEqual(logs["content"].count("ok"), 3)
    self.assertTrue(logs["complete"])

  def test_repeat_failure_stops_after_first_error(self):
    count_file = Path(self.temp_dir.name) / "count.txt"
    cmd = " ".join([
      "/bin/sh",
      "-c",
      shlex.quote(
        f"count=$(cat {shlex.quote(str(count_file))} 2>/dev/null || printf 0); "
        f"count=$((count + 1)); "
        f"printf '%s\\n' \"$count\" > {shlex.quote(str(count_file))}; "
        "printf 'iter:%s\\n' \"$count\"; "
        "test \"$count\" -lt 2"
      ),
    ])
    submit = self.submit(cmd, repeat=5)

    result = self.wait_for_done(submit["job_id"])
    self.assertNotEqual(result["exit_code"], 0)
    self.assertEqual(result["repeat_completed"], 1)

    logs = self.get_json(f"/logs/{submit['job_id']}?offset=0&limit=4096")
    self.assertIn("Repeat 1/5", logs["content"])
    self.assertIn("Repeat 2/5", logs["content"])
    self.assertNotIn("Repeat 3/5", logs["content"])
    self.assertEqual(logs["content"].count("iter:"), 2)

  def test_repeat_timeout_stops_the_job(self):
    submit = self.submit("sleep 5", timeout=1, repeat=3)

    result = self.wait_for_done(submit["job_id"])
    self.assertEqual(result["exit_code"], -9)
    self.assertTrue(result["timed_out"])
    self.assertIn("Command timed out after 1s", result["timeout_message"])
    self.assertEqual(result["repeat_completed"], 0)
    self.assertLess(result["elapsed"], 3)

    logs = self.get_json(f"/logs/{submit['job_id']}?offset=0&limit=4096")
    self.assertIn("Repeat 1/3", logs["content"])
    self.assertNotIn("Repeat 2/3", logs["content"])
    self.assertIn("Command timed out after 1s", logs["content"])
    self.assertIn("SIGKILL", logs["content"])

    job = self.get_json(f"/job/{submit['job_id']}")
    self.assertTrue(job["timed_out"])
    self.assertIn("Command timed out after 1s", job["timeout_message"])

  def test_queued_command_gets_pythonpath_dot(self):
    code = "import os; print(os.environ.get('PYTHONPATH'))"
    submit = self.submit(self.python_cmd(code), timeout=5)

    result = self.wait_for_done(submit["job_id"])
    self.assertEqual(result["exit_code"], 0)

    logs = self.get_json(f"/logs/{submit['job_id']}?offset=0&limit=4096")
    self.assertIn(".\n", logs["content"])

  def test_queued_command_merges_env(self):
    code = "import os; print(os.environ.get('TT_USB'))"
    submit = self.submit(self.python_cmd(code), timeout=5, env={"TT_USB": "1"})

    result = self.wait_for_done(submit["job_id"])
    self.assertEqual(result["exit_code"], 0)

    logs = self.get_json(f"/logs/{submit['job_id']}?offset=0&limit=4096")
    self.assertIn("1\n", logs["content"])

  def test_inline_env_assignment_works_under_shell_wrapper(self):
    cmd = "TT_USB=1 " + self.python_cmd("import os; print(os.environ.get('TT_USB'))")
    submit = self.submit(cmd, timeout=5)

    result = self.wait_for_done(submit["job_id"])
    self.assertEqual(result["exit_code"], 0)

    logs = self.get_json(f"/logs/{submit['job_id']}?offset=0&limit=4096")
    self.assertIn("1\n", logs["content"])

  def test_shell_wrapper_preserves_compound_commands(self):
    cmd = "printf 'one\\n'; TT_USB=2 " + self.python_cmd(
      "import os; print(os.environ.get('TT_USB'))"
    )
    submit = self.submit(cmd, timeout=5)

    result = self.wait_for_done(submit["job_id"])
    self.assertEqual(result["exit_code"], 0)

    logs = self.get_json(f"/logs/{submit['job_id']}?offset=0&limit=4096")
    self.assertIn("one\n", logs["content"])
    self.assertIn("2\n", logs["content"])

  def test_queued_command_gets_default_child_oom_score(self):
    code = "from pathlib import Path; print(Path('/proc/self/oom_score_adj').read_text().strip())"
    submit = self.submit(self.python_cmd(code), timeout=5)

    result = self.wait_for_done(submit["job_id"])
    self.assertEqual(result["exit_code"], 0)

    logs = self.get_json(f"/logs/{submit['job_id']}?offset=0&limit=4096")
    self.assertIn("500\n", logs["content"])

  def test_child_oom_score_can_be_overridden_per_job(self):
    code = "from pathlib import Path; print(Path('/proc/self/oom_score_adj').read_text().strip())"
    submit = self.submit(
      self.python_cmd(code),
      timeout=5,
      env={"TT_DEVICE_CHILD_OOM_SCORE_ADJ": "250"},
    )

    result = self.wait_for_done(submit["job_id"])
    self.assertEqual(result["exit_code"], 0)

    logs = self.get_json(f"/logs/{submit['job_id']}?offset=0&limit=4096")
    self.assertIn("250\n", logs["content"])

  def test_job_endpoint_reports_queue_running_and_done_metadata(self):
    gate = Path(self.temp_dir.name) / "job_endpoint_gate"
    first = self.submit(self.gate_cmd(gate), timeout=5)
    second = self.submit(self.python_cmd("print('second')"), timeout=5, repeat=2)

    queued = self.get_json(f"/job/{second['job_id']}")
    self.assertEqual(queued["status"], "queued")
    self.assertEqual(queued["position"], 1)
    self.assertEqual(queued["repeat"], 2)
    self.assertIsNotNone(queued["submitted_at"])

    deadline = time.time() + 5
    running_seen = False
    while time.time() < deadline:
      running = self.get_json(f"/job/{first['job_id']}")
      if running["status"] == "running":
        running_seen = True
        self.assertEqual(running["position"], 0)
        self.assertIsNotNone(running["started_at"])
        break
      time.sleep(POLL_INTERVAL)
    self.assertTrue(running_seen)

    gate.touch()
    self.wait_for_done(first["job_id"])
    self.wait_for_done(second["job_id"])

    done = self.get_json(f"/job/{second['job_id']}")
    self.assertEqual(done["status"], "done")
    self.assertEqual(done["repeat_completed"], 2)
    self.assertIsNotNone(done["started_at"])
    self.assertIsNotNone(done["finished_at"])
    self.assertEqual(done["exit_code"], 0)

  def test_initial_repeat_estimate_scales_with_repeat_count(self):
    gate = Path(self.temp_dir.name) / "repeat_estimate_gate"
    submit = self.submit(self.gate_cmd(gate), timeout=5, repeat=4)

    self.assertEqual(submit["estimated_run_sec"], 40)

    queued = self.submit(self.python_cmd("print('queued')"), timeout=5)
    self.assertGreaterEqual(queued["estimated_wait_sec"], 30)

    gate.touch()
    self.wait_for_done(submit["job_id"])
    self.wait_for_done(queued["job_id"])

  def test_first_iteration_updates_repeat_eta(self):
    submit = self.submit("sleep 0.03", timeout=5, repeat=4)

    refined = None
    deadline = time.time() + 5
    while time.time() < deadline:
      job = self.get_json(f"/job/{submit['job_id']}")
      if job["status"] == "running" and job["repeat_completed"] >= 1:
        refined = job
        break
      time.sleep(POLL_INTERVAL)

    self.assertIsNotNone(refined)
    self.assertLess(refined["per_iter_estimate_sec"], 2.0)
    self.assertIsNotNone(refined["first_iteration_elapsed"])
    self.assertLess(refined["estimated_remaining_sec"], 10)

    self.wait_for_done(submit["job_id"])

  def test_logs_endpoint_supports_offsets_and_completion(self):
    cmd = self.python_cmd(
      "import sys, time; sys.stdout.write('A' * 200); sys.stdout.flush(); time.sleep(0.03); print('done')"
    )
    submit = self.submit(cmd, timeout=5)

    deadline = time.time() + 5
    first_chunk = None
    while time.time() < deadline:
      logs = self.get_json(f"/logs/{submit['job_id']}?offset=0&limit=64")
      if logs["content"]:
        first_chunk = logs
        break
      time.sleep(POLL_INTERVAL)
    self.assertIsNotNone(first_chunk)
    self.assertTrue(first_chunk["truncated"])
    self.assertEqual(first_chunk["next_offset"], len(first_chunk["content"].encode()))

    self.wait_for_done(submit["job_id"])
    second_chunk = self.get_json(
      f"/logs/{submit['job_id']}?offset={first_chunk['next_offset']}&limit=4096"
    )
    self.assertIn("done", second_chunk["content"])
    self.assertTrue(second_chunk["complete"])

  def test_completed_job_logs_survive_server_restart(self):
    submit = self.submit(self.python_cmd("print('persistent')"), timeout=5)
    result = self.wait_for_done(submit["job_id"])
    self.assertEqual(result["exit_code"], 0)

    db_path = Path(self.temp_dir.name) / "jobs.sqlite3"
    self.assertTrue(db_path.exists())
    self.assertTrue(Path(submit["output_file"]).exists())

    self._stop_server()
    self._start_server()

    job = self.get_json(f"/job/{submit['job_id']}")
    self.assertEqual(job["status"], "done")
    self.assertEqual(job["exit_code"], 0)

    logs = self.get_json(f"/logs/{submit['job_id']}?offset=0&limit=4096")
    self.assertIn("persistent", logs["content"])
    self.assertTrue(logs["complete"])

    recent = self.get_json("/status")["recent"]
    self.assertIn(submit["job_id"], [item["id"] for item in recent])

  def test_submit_without_repeat_defaults_to_one(self):
    payload = {
      "cmd": self.python_cmd("print('plain')"),
      "cwd": str(REPO_ROOT),
      "timeout": 5,
    }
    submit = self.post_json("/queue", payload)
    self.assertEqual(submit["repeat"], 1)

    result = self.wait_for_done(submit["job_id"])
    self.assertEqual(result["exit_code"], 0)
    self.assertEqual(result["repeat"], 1)

  def test_open_mode_is_rejected(self):
    code, resp = self.post_status("/queue", {
      "cmd": "true",
      "cwd": str(REPO_ROOT),
      "mode": "open",
    })
    self.assertEqual(code, 400)
    self.assertIn("mode must be 'run'", resp["error"])

  def test_round_robin_interleaves_clients(self):
    seq = Path(self.temp_dir.name) / "seq.txt"
    gate = Path(self.temp_dir.name) / "round_robin_gate"
    blocker = self.submit(self.gate_cmd(gate), client="agent-a")
    self.wait_for_running(blocker["job_id"])

    a1 = self.submit(self.seq_cmd("a1", seq), client="agent-a")
    a2 = self.submit(self.seq_cmd("a2", seq), client="agent-a")
    b1 = self.submit(self.seq_cmd("b1", seq), client="agent-b")

    # b1 should slot in after a1 (round robin), not behind a1 and a2 (FIFO):
    # ahead of it are the running blocker and a1 only.
    self.assertEqual(b1["position"], 2)

    gate.touch()
    for s in (a1, a2, b1):
      self.wait_for_done(s["job_id"])
    self.assertEqual(seq.read_text().split(), ["a1", "b1", "a2"])

  def test_same_client_jobs_stay_fifo(self):
    seq = Path(self.temp_dir.name) / "seq.txt"
    gate = Path(self.temp_dir.name) / "same_client_gate"
    blocker = self.submit(self.gate_cmd(gate), client="agent-a")
    self.wait_for_running(blocker["job_id"])

    submits = [
      self.submit(self.seq_cmd(name, seq), client="agent-a")
      for name in ("a1", "a2", "a3")
    ]
    gate.touch()
    for s in submits:
      self.wait_for_done(s["job_id"])
    self.assertEqual(seq.read_text().split(), ["a1", "a2", "a3"])

  def test_client_id_defaults_to_anon_and_is_visible(self):
    gate = Path(self.temp_dir.name) / "client_id_gate"
    blocker = self.submit(self.gate_cmd(gate), client="agent-x")
    self.wait_for_running(blocker["job_id"])
    queued = self.submit(self.python_cmd("print('hi')"), client="agent-y")
    anon = self.submit(self.python_cmd("print('anon')"))

    job = self.get_json(f"/job/{queued['job_id']}")
    self.assertEqual(job["client_id"], "agent-y")
    self.assertEqual(self.get_json(f"/job/{anon['job_id']}")["client_id"], "anon")

    status = self.get_json("/status")
    self.assertEqual(status["current"]["client"], "agent-x")
    clients = {p["id"]: p["client"] for p in status["pending"]}
    self.assertEqual(clients[queued["job_id"]], "agent-y")
    self.assertEqual(clients[anon["job_id"]], "anon")

    gate.touch()
    self.wait_for_done(queued["job_id"])
    self.wait_for_done(anon["job_id"])

  def test_invalid_client_id_is_rejected(self):
    code, resp = self.post_status("/queue", {
      "cmd": "true", "cwd": "", "timeout": 5, "client_id": "   ",
    })
    self.assertEqual(code, 400)
    self.assertIn("client_id", resp["error"])

  def test_pci_reset_commands_are_rejected(self):
    for cmd in (
        "sudo -n /usr/local/sbin/tt-pci-deep-reset",
        "echo 1 > /sys/bus/pci/rescan",
        "echo 1 > /sys/bus/pci/devices/0000:01:00.0/remove",
    ):
      with self.subTest(cmd=cmd):
        code, resp = self.post_status("/queue", {"cmd": cmd})
        self.assertEqual(code, 400)
        self.assertIn("Refusing to queue command", resp["error"])

  def test_cancel_queued_job(self):
    seq = Path(self.temp_dir.name) / "seq.txt"
    gate = Path(self.temp_dir.name) / "cancel_gate"
    blocker = self.submit(self.gate_cmd(gate))
    self.wait_for_running(blocker["job_id"])
    victim = self.submit(self.seq_cmd("victim", seq))

    # Running jobs cannot be cancelled.
    code, resp = self.post_status("/cancel", {"job_id": blocker["job_id"]})
    self.assertEqual(code, 409)

    # Unknown jobs 404.
    code, resp = self.post_status("/cancel", {"job_id": "nope1234"})
    self.assertEqual(code, 404)

    code, resp = self.post_status("/cancel", {"job_id": victim["job_id"]})
    self.assertEqual(code, 200)
    self.assertEqual(resp["cancelled"]["id"], victim["job_id"])

    result = self.wait_for_done(victim["job_id"])
    self.assertEqual(result["exit_code"], -1)
    logs = self.get_json(f"/logs/{victim['job_id']}?offset=0&limit=4096")
    self.assertIn("Cancelled while queued", logs["content"])

    gate.touch()
    self.wait_for_done(blocker["job_id"])
    self.assertFalse(seq.exists(), "cancelled job must never run")

  def test_status_reports_device_healthy(self):
    device = self.get_json("/status")["device"]
    self.assertEqual(device["state"], "healthy")
    self.assertFalse(device["queue_disabled"])
    self.assertIsNone(device["disabled_reason"])
    self.assertEqual(device["reset_epoch"], 0)
    self.assertFalse(device["reset_pending"])

  def test_migrates_legacy_db_without_client_columns(self):
    self._stop_server()
    db_path = Path(self.temp_dir.name) / "jobs.sqlite3"
    for leftover in (db_path, Path(f"{db_path}-wal"), Path(f"{db_path}-shm")):
      leftover.unlink(missing_ok=True)

    conn = sqlite3.connect(db_path)
    conn.execute("""
      CREATE TABLE jobs (
        id TEXT PRIMARY KEY, cmd TEXT NOT NULL, cwd TEXT NOT NULL,
        timeout INTEGER NOT NULL, repeat INTEGER NOT NULL,
        env_json TEXT NOT NULL, mode TEXT NOT NULL, status TEXT NOT NULL,
        submitted REAL NOT NULL, started_at REAL, finished_at REAL,
        exit_code INTEGER, elapsed REAL, output_file TEXT NOT NULL,
        repeat_current INTEGER NOT NULL, repeat_completed INTEGER NOT NULL,
        first_iteration_elapsed REAL, per_iter_estimate_sec REAL NOT NULL,
        stop_requested_at REAL, stop_escalated_at REAL,
        log_size INTEGER NOT NULL DEFAULT 0, updated_at REAL NOT NULL
      )
    """)
    now = time.time()
    conn.execute(
      """
      INSERT INTO jobs (
        id, cmd, cwd, timeout, repeat, env_json, mode, status, submitted,
        started_at, finished_at, exit_code, elapsed, output_file,
        repeat_current, repeat_completed, first_iteration_elapsed,
        per_iter_estimate_sec, stop_requested_at, stop_escalated_at,
        log_size, updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      """,
      (
        "legacy01", "echo legacy", "", 5, 1, "{}", "run", "done", now,
        now, now, 0, 0.1, str(Path(self.temp_dir.name) / "legacy01" / "output"),
        1, 1, 0.1, 0.1, None, None, 0, now,
      ),
    )
    conn.commit()
    conn.close()

    self._start_server()

    legacy = self.get_json("/job/legacy01")
    self.assertEqual(legacy["status"], "done")
    self.assertEqual(legacy["client_id"], "anon")

    fresh = self.submit(self.python_cmd("print('post-migration')"), client="agent-z")
    result = self.wait_for_done(fresh["job_id"])
    self.assertEqual(result["exit_code"], 0)
    self.assertEqual(self.get_json(f"/job/{fresh['job_id']}")["client_id"], "agent-z")


class DeviceHealthTest(QueueServerTestBase):
  def extra_server_env(self) -> dict:
    base = Path(self.temp_dir.name)
    self.reset_count = base / "reset_count"
    self.reset_rc = base / "reset_rc"
    self.reset_sleep = base / "reset_sleep"

    reset_sh = base / "fake_reset.sh"
    reset_sh.write_text(
      "#!/bin/sh\n"
      f"count=$(cat {self.reset_count} 2>/dev/null || echo 0)\n"
      f"echo $((count + 1)) > {self.reset_count}\n"
      f"sleep $(cat {self.reset_sleep} 2>/dev/null || echo 0)\n"
      "echo reset-done\n"
      f"exit $(cat {self.reset_rc} 2>/dev/null || echo 0)\n"
    )
    # Fake deep reset (PCI remove/rescan stand-in): on success it lets the
    # next first-level reset succeed; deep_rc simulates the helper failing.
    self.deep_count = base / "deep_count"
    self.deep_rc = base / "deep_rc"
    deep_sh = base / "fake_deep_reset.sh"
    deep_sh.write_text(
      "#!/bin/sh\n"
      f"count=$(cat {self.deep_count} 2>/dev/null || echo 0)\n"
      f"echo $((count + 1)) > {self.deep_count}\n"
      f"rc=$(cat {self.deep_rc} 2>/dev/null || echo 0)\n"
      "[ \"$rc\" -ne 0 ] && exit \"$rc\"\n"
      f"echo 0 > {self.reset_rc}\n"
      "echo deep-reset-done\n"
    )
    reset_sh.chmod(0o755)
    deep_sh.chmod(0o755)

    return {
      "TT_DEVICE_RESET_CMD": str(reset_sh),
      "TT_DEVICE_DEEP_RESET_CMD": str(deep_sh),
      "TT_DEVICE_RESET_RETRIES": "0",
    }

  def reset_runs(self) -> int:
    try:
      return int(self.reset_count.read_text().strip())
    except FileNotFoundError:
      return 0

  def deep_reset_runs(self) -> int:
    try:
      return int(self.deep_count.read_text().strip())
    except FileNotFoundError:
      return 0

  def test_concurrent_reset_requests_coalesce_into_one(self):
    self.reset_sleep.write_text("0.1")
    job = self.submit(self.python_cmd("print('boom')"))
    self.wait_for_done(job["job_id"])

    code, first = self.post_status("/reset", {"job_id": job["job_id"]})
    self.assertEqual(code, 200)
    self.assertEqual(first["action"], "scheduled")
    self.assertEqual(first["breakage"]["reported_job"]["id"], job["job_id"])
    self.assertEqual(first["breakage"]["suspect_job"]["id"], job["job_id"])
    self.assertEqual(first["breakage"]["suspect_job"]["output_file"], job["output_file"])

    # Everyone else piling on while the reset is pending/running just joins it.
    for _ in range(5):
      code, again = self.post_status("/reset", {"job_id": job["job_id"]})
      self.assertEqual(code, 200)
      self.assertEqual(again["action"], "joined")
      self.assertEqual(again["breakage"]["suspect_job"]["id"], job["job_id"])

    self.wait_for_device("healthy", epoch=1)
    breakage = self.get_json("/breakage")["last_breakage"]
    self.assertEqual(breakage["suspect_job"]["id"], job["job_id"])
    self.assertEqual(breakage["reset_result"], "healthy")
    self.assertEqual(breakage["reset_job"]["mode"], "reset")

    # Stale report for a pre-reset job: no new reset.
    code, stale = self.post_status("/reset", {"job_id": job["job_id"]})
    self.assertEqual(code, 200)
    self.assertEqual(stale["action"], "already_reset")

    self.assertEqual(self.reset_runs(), 1)

  def test_reset_for_unknown_job_is_404(self):
    code, resp = self.post_status("/reset", {"job_id": "nope1234"})
    self.assertEqual(code, 404)

  def test_queue_is_held_during_reset_then_resumes(self):
    self.reset_sleep.write_text("0.1")
    job = self.submit(self.python_cmd("print('boom')"))
    self.wait_for_done(job["job_id"])

    code, resp = self.post_status("/reset", {"job_id": job["job_id"]})
    self.assertEqual(resp["action"], "scheduled")

    held = self.submit(self.python_cmd("print('after-reset')"))
    time.sleep(POLL_INTERVAL)
    self.assertEqual(self.get_json(f"/job/{held['job_id']}")["status"], "queued")

    result = self.wait_for_done(held["job_id"])
    self.assertEqual(result["exit_code"], 0)
    device = self.get_json("/status")["device"]
    self.assertEqual(device["state"], "healthy")
    self.assertEqual(device["reset_epoch"], 1)

  def test_jobs_record_their_reset_epoch(self):
    job = self.submit(self.python_cmd("print('one')"))
    self.wait_for_done(job["job_id"])
    self.post_status("/reset", {"job_id": job["job_id"]})
    self.wait_for_device("healthy", epoch=1)

    after = self.submit(self.python_cmd("print('two')"))
    self.wait_for_done(after["job_id"])
    # A job that ran after the reset is a fresh report: schedules a new reset.
    code, resp = self.post_status("/reset", {"job_id": after["job_id"]})
    self.assertEqual(resp["action"], "scheduled")
    self.wait_for_device("healthy", epoch=2)

  def test_deep_reset_recovers_device_when_first_level_reset_fails(self):
    # First-level reset fails until the deep reset (PCI remove/rescan) "fixes" it.
    self.reset_rc.write_text("1")
    job = self.submit(self.python_cmd("print('boom')"))
    self.wait_for_done(job["job_id"])

    code, resp = self.post_status("/reset", {"job_id": job["job_id"]})
    self.assertEqual(resp["action"], "scheduled")

    device = self.wait_for_device("healthy", epoch=1)
    self.assertEqual(self.deep_reset_runs(), 1)
    # First-level reset ran once before escalation and once after the deep reset.
    self.assertEqual(self.reset_runs(), 2)

    after = self.submit(self.python_cmd("print('recovered')"))
    result = self.wait_for_done(after["job_id"])
    self.assertEqual(result["exit_code"], 0)

  def test_failed_reset_marks_device_dead_drains_queue_and_blocks_submits(self):
    self.reset_rc.write_text("1")
    self.deep_rc.write_text("1")  # deep reset escalation fails too

    gate = Path(self.temp_dir.name) / "failed_reset_gate"
    blocker = self.submit(self.gate_cmd(gate), client="agent-a")
    self.wait_for_running(blocker["job_id"])
    k1 = self.submit(self.python_cmd("print('k1')"), client="agent-a")
    k2 = self.submit(self.python_cmd("print('k2')"), client="agent-b")

    code, resp = self.post_status("/reset", {})
    self.assertEqual(resp["action"], "scheduled")
    self.assertIsNone(resp["breakage"]["reported_job"])
    self.assertEqual(resp["breakage"]["suspect_job"]["id"], blocker["job_id"])

    # The running job is allowed to finish normally.
    gate.touch()
    blocker_result = self.wait_for_done(blocker["job_id"])
    self.assertEqual(blocker_result["exit_code"], 0)

    # Queued jobs are drained with the reboot-required message.
    for queued in (k1, k2):
      result = self.wait_for_done(queued["job_id"])
      self.assertEqual(result["exit_code"], -1)
      logs = self.get_json(f"/logs/{queued['job_id']}?offset=0&limit=8192")
      self.assertIn("reboot is required", logs["content"])

    device = self.wait_for_device("dead")
    self.assertIn("reboot", device["dead_reason"])
    self.assertEqual(device["last_breakage"]["suspect_job"]["id"], blocker["job_id"])
    self.assertEqual(device["last_breakage"]["reset_result"], "dead")

    # New submissions are rejected with 503 + reboot message.
    code, resp = self.post_status("/queue", {
      "cmd": "true", "cwd": "", "timeout": 5,
    })
    self.assertEqual(code, 503)
    self.assertIn("reboot", resp["error"])

    # Further reset requests are also rejected.
    code, resp = self.post_status("/reset", {})
    self.assertEqual(code, 503)

  def test_restart_recovers_from_dead_state(self):
    self.reset_rc.write_text("1")
    self.deep_rc.write_text("1")  # deep reset escalation fails too
    job = self.submit(self.python_cmd("print('boom')"))
    self.wait_for_done(job["job_id"])
    self.post_status("/reset", {"job_id": job["job_id"]})
    self.wait_for_device("dead")

    # Reboot (simulated by service restart) brings the queue back.
    self._stop_server()
    self._start_server()

    device = self.get_json("/status")["device"]
    self.assertEqual(device["state"], "healthy")
    revived = self.submit(self.python_cmd("print('back')"))
    result = self.wait_for_done(revived["job_id"])
    self.assertEqual(result["exit_code"], 0)


if __name__ == "__main__":
  unittest.main()
