#!/usr/bin/env python3
"""MCP compatibility surface for tt-device-queue."""

from __future__ import annotations

import asyncio
import json
import os
import shlex
import time
import uuid
from pathlib import Path
from urllib.parse import urlencode

from mcp.server.fastmcp import FastMCP

from queue_client import get, post


HOST = "127.0.0.1"
PORT = int(os.environ.get("TT_DEVICE_PORT", "5741"))
BASE = f"http://{HOST}:{PORT}"
CLIENT_ID = os.environ.get("TT_QUEUE_CLIENT_ID") or f"agent-{uuid.uuid4().hex[:12]}"
SCRIPT_DIR = Path(os.environ.get(
    "TT_DEVICE_SCRIPT_DIR", Path(__file__).resolve().parent / "logs-v2" / "mcp-scripts"
))
POLL_INTERVAL = 0.5
RESULT_OUTPUT_LIMIT = int(os.environ.get("TT_DEVICE_MCP_RESULT_BYTES", str(1 << 20)))


server = FastMCP(
    "tt-device-queue",
    instructions="Use only for Tenstorrent device commands. Use normal shell for CPU-only work.",
)


async def _get(path: str) -> dict:
    return await asyncio.to_thread(get, BASE, path)


async def _post(path: str, data: dict) -> dict:
    return await asyncio.to_thread(post, BASE, path, data)


async def _wait_for_job(job_id: str) -> dict:
    """Async polling does not occupy a worker thread for the life of the job."""
    interval = 0.05
    while True:
        result = await _get(f"/result/{job_id}")
        if result["status"] == "done":
            return result
        await asyncio.sleep(interval)
        interval = min(interval * 2, POLL_INTERVAL)


async def _read_result_logs(job_id: str) -> tuple[str, bool]:
    offset = 0
    pieces: list[str] = []
    truncated = False
    while offset < RESULT_OUTPUT_LIMIT:
        limit = min(64 << 10, RESULT_OUTPUT_LIMIT - offset)
        query = urlencode({"offset": offset, "limit": limit})
        chunk = await _get(f"/logs/{job_id}?{query}")
        pieces.append(chunk.get("content", ""))
        next_offset = int(chunk.get("next_offset", offset))
        if chunk.get("complete"):
            truncated = bool(chunk.get("log_truncated"))
            break
        if next_offset <= offset:
            break
        offset = next_offset
    else:
        truncated = True
    if offset >= RESULT_OUTPUT_LIMIT:
        truncated = True
    return "".join(pieces), truncated


def _prune_scripts() -> None:
    cutoff = time.time() - 7 * 86400
    try:
        for path in SCRIPT_DIR.glob("*.py"):
            if path.stat().st_mtime < cutoff:
                path.unlink(missing_ok=True)
    except OSError:
        pass


def _write_python_script(script: str) -> Path:
    SCRIPT_DIR.mkdir(mode=0o700, parents=True, exist_ok=True)
    _prune_scripts()
    path = SCRIPT_DIR / f"{uuid.uuid4().hex}.py"
    path.write_text(script if script.endswith("\n") else script + "\n")
    path.chmod(0o600)
    return path


@server.tool(name="queue")
async def queue(cmd: str, cwd: str = "", repeat: int = 1) -> str:
    """Queue a device command and return immediately."""
    result = await _post("/queue", {
        "cmd": cmd, "cwd": cwd, "repeat": repeat,
        "mode": "run", "client_id": CLIENT_ID,
    })
    return json.dumps({
        "job_id": result["job_id"], "output_file": result["output_file"],
        "position": result["position"],
        "estimated_wait_sec": result["estimated_wait_sec"],
        "estimated_run_sec": result.get("estimated_run_sec"), "repeat": repeat,
        "timeout": result.get("timeout"), "hint": "Use result(job_id) for output.",
    }, indent=2)


@server.tool(name="queue_python")
async def queue_python(
    script: str, cwd: str = "", repeat: int = 1,
    python: str = "python3", args: list[str] | None = None,
) -> str:
    """Write a Python script, then queue it."""
    script_path = await asyncio.to_thread(_write_python_script, script)
    try:
        result = await _post("/queue", {
            "cmd": shlex.join([python, str(script_path), *(args or [])]),
            "cwd": cwd, "repeat": repeat, "mode": "run", "client_id": CLIENT_ID,
        })
    except Exception:
        script_path.unlink(missing_ok=True)
        raise
    return json.dumps({
        "job_id": result["job_id"], "script_file": str(script_path),
        "output_file": result["output_file"], "position": result["position"],
        "estimated_wait_sec": result["estimated_wait_sec"],
        "estimated_run_sec": result.get("estimated_run_sec"), "repeat": repeat,
        "timeout": result.get("timeout"), "hint": "Use result(job_id) for output.",
    }, indent=2)


@server.tool(name="job")
async def job(job_id: str) -> str:
    """Get structured job status."""
    return json.dumps(await _get(f"/job/{job_id}"), indent=2)


@server.tool(name="logs")
async def logs(job_id: str, offset: int = 0, limit: int = 16384) -> str:
    """Read one bounded output chunk."""
    query = urlencode({"offset": offset, "limit": limit})
    return json.dumps(await _get(f"/logs/{job_id}?{query}"), indent=2)


@server.tool(name="result")
async def result(job_id: str) -> str:
    """Wait for completion and return bounded output."""
    metadata = await _wait_for_job(job_id)
    output, truncated = await _read_result_logs(job_id)
    if metadata.get("timed_out"):
        status_text = "TIMED OUT"
    else:
        status_text = "OK" if metadata.get("exit_code") == 0 else f"FAILED (exit code {metadata.get('exit_code')})"
    lines = [
        f"Status: {status_text}", f"Elapsed: {metadata.get('elapsed')}s",
        f"Output file: {metadata.get('output_file', '')}", "", "--- Command Output ---",
        output,
    ]
    if metadata.get("timed_out"):
        lines.insert(1, metadata.get("timeout_message") or "Command timed out.")
    if truncated:
        lines.extend(["", f"[Output truncated at {RESULT_OUTPUT_LIMIT} bytes; use logs(job_id, offset, limit).]"])
    return "\n".join(lines)


def _breakage_lines(breakage: dict | None) -> list[str]:
    if not breakage:
        return []
    lines = ["LAST BREAKAGE REPORT:"]
    suspect = breakage.get("suspect_job") or {}
    if suspect:
        lines.append(f"  suspect [{suspect.get('id')}] ({suspect.get('client')}) {suspect.get('cmd')}")
        if suspect.get("output_file"):
            lines.append(f"  output {suspect['output_file']}")
    reset_job = breakage.get("reset_job") or {}
    if reset_job:
        lines.append(f"  reset [{reset_job.get('id')}] {breakage.get('reset_result', 'running')}")
    return lines


@server.tool(name="status")
async def status() -> str:
    """Show queue and device status."""
    data = await _get("/status")
    lines: list[str] = []
    worker = data.get("worker") or {}
    if not worker.get("alive", True) or worker.get("degraded_reason"):
        lines.append(f"!!! QUEUE DEGRADED — {worker.get('degraded_reason') or 'worker is not alive'}")
    device = data.get("device") or {}
    if device.get("state") == "dead":
        lines.append(f"!!! DEVICE DEAD since {device.get('dead_since')} — {device.get('dead_reason')}")
        lines.extend(_breakage_lines(device.get("last_breakage")))
    elif device.get("state") == "resetting" or device.get("reset_pending"):
        lines.append("!!! DEVICE RESET in progress — jobs are held")
        lines.extend(_breakage_lines(device.get("last_breakage")))
    current = data.get("current")
    if current:
        client = f" ({current['client']})" if current.get("client") else ""
        lines.append(f"RUNNING: [{current['id']}]{client} {current['cmd']}")
        lines.append(f"         {current['running_sec']}s  eta ~{current.get('estimated_remaining_sec', '?')}s")
    else:
        lines.append("RUNNING: (idle)")
    pending = data.get("pending", [])
    if pending:
        lines.append(f"\nQUEUED ({len(pending)}):")
        for item in pending:
            client = f" ({item['client']})" if item.get("client") else ""
            lines.append(f"  [{item['id']}]{client} {item['cmd']}")
            lines.append(f"           waiting {item['waiting_sec']}s  eta ~{item.get('estimated_wait_sec', '?')}s")
    else:
        lines.append("\nQUEUED: (empty)")
    recent = data.get("recent", [])
    if recent:
        lines.append("\nRECENT:")
        for item in recent:
            tag = "TIMEOUT" if item.get("timed_out") else ("OK" if item.get("exit_code") == 0 else f"FAIL({item.get('exit_code')})")
            lines.append(f"  [{item['id']}] {tag} {item.get('elapsed', '?')}s  {item['cmd']}")
    return "\n".join(lines)


@server.tool(name="kill")
async def kill(job_id: str = "") -> str:
    """Gracefully stop the running job, escalating when necessary."""
    result = await _post("/kill", {"job_id": job_id} if job_id else {})
    stopped = result.get("killed")
    return (
        f"Sent {stopped.get('signal', 'SIGINT')} to job [{stopped['id']}] {stopped['cmd']}"
        if stopped else "Nothing running to kill."
    )


@server.tool(name="reset")
async def reset(job_id: str = "") -> str:
    """Report device breakage and schedule a coalesced reset."""
    payload = {"client_id": CLIENT_ID}
    if job_id:
        payload["job_id"] = job_id
    return json.dumps(await _post("/reset", payload), indent=2)


if __name__ == "__main__":
    server.run(transport="stdio")
