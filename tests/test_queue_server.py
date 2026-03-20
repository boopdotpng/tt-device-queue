import json
import os
import shlex
import socket
import subprocess
import sys
import tempfile
import time
import unittest
import urllib.request
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
SERVER_PATH = REPO_ROOT / "server.py"


def free_port() -> int:
  with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
    sock.bind(("127.0.0.1", 0))
    return sock.getsockname()[1]


class QueueServerTest(unittest.TestCase):
  def setUp(self):
    self.temp_dir = tempfile.TemporaryDirectory()
    self.port = free_port()
    env = os.environ.copy()
    env["TT_DEVICE_PORT"] = str(self.port)
    env["TT_DEVICE_LOG_DIR"] = self.temp_dir.name
    self.server = subprocess.Popen(
      [sys.executable, str(SERVER_PATH)],
      cwd=REPO_ROOT,
      env=env,
      stdout=subprocess.PIPE,
      stderr=subprocess.STDOUT,
      text=True,
    )
    self.base = f"http://127.0.0.1:{self.port}"
    self._wait_for_server()

  def tearDown(self):
    if self.server.poll() is None:
      self.server.terminate()
      try:
        self.server.wait(timeout=5)
      except subprocess.TimeoutExpired:
        self.server.kill()
        self.server.wait(timeout=5)
    self.temp_dir.cleanup()

  def _wait_for_server(self):
    deadline = time.time() + 5
    last_error = None
    while time.time() < deadline:
      try:
        self.get_json("/status")
        return
      except Exception as exc:  # pragma: no cover - best effort startup loop
        last_error = exc
        time.sleep(0.05)
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

  def submit(self, cmd: str, timeout: int = 5, repeat: int = 1) -> dict:
    return self.post_json("/queue", {
      "cmd": cmd,
      "cwd": str(REPO_ROOT),
      "timeout": timeout,
      "repeat": repeat,
    })

  def wait_for_done(self, job_id: str, timeout: float = 10.0) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
      result = self.get_json(f"/result/{job_id}")
      if result["status"] == "done":
        return result
      time.sleep(0.05)
    raise AssertionError(f"job {job_id} did not finish in time")

  def python_cmd(self, code: str) -> str:
    return f"{shlex.quote(sys.executable)} -c {shlex.quote(code)}"

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
    self.assertEqual(logs["content"].count("[claude-collide] Repeat"), 3)
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
    self.assertEqual(result["repeat_completed"], 0)

    logs = self.get_json(f"/logs/{submit['job_id']}?offset=0&limit=4096")
    self.assertIn("Repeat 1/3", logs["content"])
    self.assertNotIn("Repeat 2/3", logs["content"])
    self.assertIn("Timed out after 1s", logs["content"])

  def test_job_endpoint_reports_queue_running_and_done_metadata(self):
    first = self.submit("sleep 1", timeout=5)
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
      time.sleep(0.05)
    self.assertTrue(running_seen)

    self.wait_for_done(first["job_id"])
    self.wait_for_done(second["job_id"])

    done = self.get_json(f"/job/{second['job_id']}")
    self.assertEqual(done["status"], "done")
    self.assertEqual(done["repeat_completed"], 2)
    self.assertIsNotNone(done["started_at"])
    self.assertIsNotNone(done["finished_at"])
    self.assertEqual(done["exit_code"], 0)

  def test_initial_repeat_estimate_scales_with_repeat_count(self):
    submit = self.submit("sleep 1", timeout=5, repeat=4)

    self.assertEqual(submit["estimated_run_sec"], 40)

    queued = self.submit(self.python_cmd("print('queued')"), timeout=5)
    self.assertGreaterEqual(queued["estimated_wait_sec"], 30)

    self.wait_for_done(submit["job_id"])
    self.wait_for_done(queued["job_id"])

  def test_first_iteration_updates_repeat_eta(self):
    submit = self.submit("sleep 0.4", timeout=5, repeat=4)

    refined = None
    deadline = time.time() + 5
    while time.time() < deadline:
      job = self.get_json(f"/job/{submit['job_id']}")
      if job["status"] == "running" and job["repeat_completed"] >= 1:
        refined = job
        break
      time.sleep(0.05)

    self.assertIsNotNone(refined)
    self.assertLess(refined["per_iter_estimate_sec"], 2.0)
    self.assertIsNotNone(refined["first_iteration_elapsed"])
    self.assertLess(refined["estimated_remaining_sec"], 10)

    self.wait_for_done(submit["job_id"])

  def test_logs_endpoint_supports_offsets_and_completion(self):
    cmd = self.python_cmd(
      "import sys, time; sys.stdout.write('A' * 200); sys.stdout.flush(); time.sleep(0.5); print('done')"
    )
    submit = self.submit(cmd, timeout=5)

    deadline = time.time() + 5
    first_chunk = None
    while time.time() < deadline:
      logs = self.get_json(f"/logs/{submit['job_id']}?offset=0&limit=64")
      if logs["content"]:
        first_chunk = logs
        break
      time.sleep(0.05)
    self.assertIsNotNone(first_chunk)
    self.assertTrue(first_chunk["truncated"])
    self.assertEqual(first_chunk["next_offset"], len(first_chunk["content"].encode()))

    self.wait_for_done(submit["job_id"])
    second_chunk = self.get_json(
      f"/logs/{submit['job_id']}?offset={first_chunk['next_offset']}&limit=4096"
    )
    self.assertIn("done", second_chunk["content"])
    self.assertTrue(second_chunk["complete"])

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


if __name__ == "__main__":
  unittest.main()
