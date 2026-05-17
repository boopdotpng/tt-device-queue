#!/usr/bin/env python3

import json
import subprocess
import time
import urllib.error
import urllib.request
from pathlib import Path


class QueueClientError(Exception):
  pass


DEFAULT_TT_SMI = Path("~/tenstorrent/blackhole-py/tt-smi.py").expanduser()


def read_output_file(output_file: str) -> str:
  if not output_file:
    return ""
  try:
    return Path(output_file).read_text()
  except (FileNotFoundError, PermissionError):
    return f"(could not read {output_file})"


def get(base: str, path: str) -> dict:
  try:
    with urllib.request.urlopen(f"{base}{path}", timeout=10) as resp:
      result = json.loads(resp.read())
      if resp.status == 404:
        raise QueueClientError(result.get("error", "Not found"))
      return result
  except urllib.error.HTTPError as exc:
    result = json.loads(exc.read())
    raise QueueClientError(result.get("error", f"HTTP {exc.code}"))
  except urllib.error.URLError:
    raise QueueClientError(
      "tt-device-queue server is not running. "
      "Start it: python ~/tenstorrent/tt-device-queue/server.py &"
    )


def post(base: str, path: str, data: dict) -> dict:
  body = json.dumps(data).encode()
  req = urllib.request.Request(
    f"{base}{path}",
    data=body,
    headers={"Content-Type": "application/json"},
    method="POST",
  )
  try:
    with urllib.request.urlopen(req, timeout=10) as resp:
      return json.loads(resp.read())
  except urllib.error.HTTPError as exc:
    result = json.loads(exc.read())
    raise QueueClientError(result.get("error", f"HTTP {exc.code}"))
  except urllib.error.URLError:
    raise QueueClientError(
      "tt-device-queue server is not running. "
      "Start it: python ~/tenstorrent/tt-device-queue/server.py &"
    )


def wait_for_job(base: str, job_id: str, poll_interval: float = 0.5) -> dict:
  interval = 0.05
  while True:
    result = get(base, f"/result/{job_id}")
    if result["status"] == "done":
      output_file = result.get("output_file", "")
      return {
        "exit_code": result["exit_code"],
        "elapsed": result["elapsed"],
        "output_file": output_file,
        "output": read_output_file(output_file),
      }
    time.sleep(interval)
    interval = min(interval * 2, poll_interval)


def run_tt_smi_snapshot(tt_smi: Path = DEFAULT_TT_SMI, device: int | None = None) -> str:
  cmd = [str(tt_smi), "--snapshot"]
  if device is not None:
    cmd.append(str(device))

  proc = subprocess.run(
    cmd,
    cwd=str(tt_smi.parent),
    capture_output=True,
    text=True,
  )
  output = proc.stdout.strip()
  error = proc.stderr.strip()
  if proc.returncode != 0:
    raise QueueClientError(error or output or "tt-smi snapshot failed")
  return output
