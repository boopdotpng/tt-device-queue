#!/usr/bin/env python3
"""
MCP server for tt-device-queue.
"""

import asyncio
import json
import os
import shlex
import uuid
from pathlib import Path

from mcp.server.fastmcp import FastMCP

from queue_client import post, get, wait_for_job

HOST = "127.0.0.1"
PORT = int(os.environ.get("TT_DEVICE_PORT", "5741"))
BASE = f"http://{HOST}:{PORT}"

# One MCP server process == one agent session. This id is the fairness unit:
# the queue server round-robins across client ids so no agent can dominate.
CLIENT_ID = os.environ.get("TT_QUEUE_CLIENT_ID") or f"agent-{uuid.uuid4().hex[:8]}"
SCRIPT_DIR = Path(
    os.environ.get(
        "TT_DEVICE_SCRIPT_DIR",
        str(Path(__file__).resolve().parent / "logs" / "mcp-scripts"),
    )
)

# Poll interval when waiting for job completion — tight because it's localhost
POLL_INTERVAL = 0.5

server = FastMCP(
    "tt-device-queue",
    instructions=(
        "Use only for Tenstorrent device commands. Non-Tenstorrent work should use normal shell."
    ),
)


async def _wait_for_job(job_id: str) -> dict:
    return await asyncio.to_thread(wait_for_job, BASE, job_id, POLL_INTERVAL)


async def _post(path: str, data: dict) -> dict:
    return await asyncio.to_thread(post, BASE, path, data)


async def _get(path: str) -> dict:
    return await asyncio.to_thread(get, BASE, path)


def _write_python_script(script: str) -> Path:
    SCRIPT_DIR.mkdir(parents=True, exist_ok=True)
    script_path = SCRIPT_DIR / f"{uuid.uuid4().hex[:8]}.py"
    if not script.endswith("\n"):
        script += "\n"
    script_path.write_text(script)
    return script_path


@server.tool(name="queue")
async def queue(
    cmd: str,
    cwd: str = "",
    repeat: int = 1,
) -> str:
    """Queue device command. Returns job_id. PYTHONPATH includes "."."""
    result = await _post("/queue", {
        "cmd": cmd, "cwd": cwd, "repeat": repeat,
        "mode": "run", "client_id": CLIENT_ID,
    })

    return json.dumps({
        "job_id": result["job_id"],
        "output_file": result["output_file"],
        "position": result["position"],
        "estimated_wait_sec": result["estimated_wait_sec"],
        "estimated_run_sec": result.get("estimated_run_sec"),
        "repeat": repeat,
        "hint": "Use result(job_id) for output.",
    }, indent=2)


@server.tool(name="queue_python")
async def queue_python(
    script: str,
    cwd: str = "",
    repeat: int = 1,
    python: str = "python3",
    args: list[str] | None = None,
) -> str:
    """Write Python script file, then queue it."""
    script_path = await asyncio.to_thread(_write_python_script, script)
    cmd = shlex.join([python, str(script_path), *(args or [])])
    result = await _post("/queue", {
        "cmd": cmd, "cwd": cwd, "repeat": repeat,
        "mode": "run", "client_id": CLIENT_ID,
    })

    return json.dumps({
        "job_id": result["job_id"],
        "script_file": str(script_path),
        "output_file": result["output_file"],
        "position": result["position"],
        "estimated_wait_sec": result["estimated_wait_sec"],
        "estimated_run_sec": result.get("estimated_run_sec"),
        "repeat": repeat,
        "hint": "Use result(job_id) for output.",
    }, indent=2)


@server.tool(name="job")
async def job(job_id: str) -> str:
    """Get job status."""
    result = await _get(f"/job/{job_id}")

    return json.dumps(result, indent=2)


@server.tool(name="logs")
async def logs(job_id: str, offset: int = 0, limit: int = 16384) -> str:
    """Read job output chunk."""
    result = await _get(f"/logs/{job_id}?offset={offset}&limit={limit}")

    return json.dumps(result, indent=2)


def _breakage_lines(breakage: dict | None) -> list[str]:
    if not breakage:
        return []
    lines = ["LAST BREAKAGE REPORT:"]
    suspect = breakage.get("suspect_job") or {}
    if suspect:
        lines.append(
            f"  suspect [{suspect.get('id')}] ({suspect.get('client')}) {suspect.get('cmd')}"
        )
        if suspect.get("output_file"):
            lines.append(f"  output {suspect['output_file']}")
    reported_job = breakage.get("reported_job") or {}
    if reported_job and reported_job.get("id") != suspect.get("id"):
        lines.append(f"  reported job [{reported_job.get('id')}] {reported_job.get('cmd')}")
    if breakage.get("reported_by") or breakage.get("reported_at"):
        lines.append(
            f"  reported by {breakage.get('reported_by', '?')} at {breakage.get('reported_at', '?')}"
        )
    reset_job = breakage.get("reset_job") or {}
    if reset_job:
        result = breakage.get("reset_result", "running")
        lines.append(f"  reset [{reset_job.get('id')}] {result}")
    return lines


@server.tool(name="result")
async def result(job_id: str) -> str:
    """Wait for job and return output."""
    result = await _wait_for_job(job_id)

    exit_code = result["exit_code"]
    timed_out = bool(result.get("timed_out"))
    if timed_out:
        status = "TIMED OUT"
    else:
        status = "OK" if exit_code == 0 else f"FAILED (exit code {exit_code})"

    lines = [
        f"Status: {status}",
        f"Elapsed: {result['elapsed']}s",
        f"Output file: {result['output_file']}",
        "",
        "--- Command Output ---",
        result["output"],
    ]
    if timed_out:
        lines.insert(1, result.get("timeout_message") or "Command timed out.")
    return "\n".join(lines)


@server.tool(name="status")
async def status() -> str:
    """Show queue status."""
    data = await _get("/status")

    lines = []

    device = data.get("device") or {}
    state = device.get("state", "healthy")
    if state == "dead":
        reason = device.get("dead_reason") or "reboot required"
        lines.append(f"!!! DEVICE DEAD since {device.get('dead_since')} — {reason}")
        lines.extend(_breakage_lines(device.get("last_breakage")))
        lines.append("")
    elif state == "resetting" or device.get("reset_pending"):
        lines.append("!!! DEVICE RESET in progress — jobs are held until the device is healthy")
        lines.extend(_breakage_lines(device.get("last_breakage")))
        lines.append("")

    current = data.get("current")
    if current:
        client = f" ({current['client']})" if current.get("client") else ""
        lines.append(f"RUNNING: [{current['id']}]{client} {current['cmd']}")
        repeat = current.get("repeat", 1)
        if repeat > 1:
            progress = f"  repeat {current.get('repeat_current', 0)}/{repeat}"
        else:
            progress = ""
        eta = current.get("estimated_remaining_sec")
        eta_text = f"  eta ~{eta}s" if eta is not None else ""
        lines.append(f"         {current['running_sec']}s{progress}{eta_text}")
    else:
        lines.append("RUNNING: (idle)")

    pending = data.get("pending", [])
    if pending:
        lines.append(f"\nQUEUED ({len(pending)}):")
        for p in pending:
            client = f" ({p['client']})" if p.get("client") else ""
            lines.append(f"  [{p['id']}]{client} {p['cmd']}")
            repeat = f"  repeat {p['repeat']}x" if p.get('repeat', 1) > 1 else ""
            eta = p.get("estimated_wait_sec")
            eta_text = f"  eta ~{eta}s" if eta is not None else ""
            lines.append(f"           waiting {p['waiting_sec']}s{repeat}{eta_text}")
    else:
        lines.append("\nQUEUED: (empty)")

    recent = data.get("recent", [])
    if recent:
        lines.append(f"\nRECENT:")
        for r in recent:
            if r.get("timed_out"):
                tag = "TIMEOUT"
            else:
                tag = "OK" if r.get("exit_code", 1) == 0 else f"FAIL({r.get('exit_code')})"
            repeat = r.get("repeat", 1)
            suffix = f"  repeat {r.get('repeat_completed', 0)}/{repeat}" if repeat > 1 else ""
            lines.append(f"  [{r['id']}] {tag} {r.get('elapsed', '?')}s  {r['cmd']}{suffix}")

    return "\n".join(lines)


@server.tool(name="last_breakage")
async def last_breakage() -> str:
    """Show the last reported broken-device culprit and reset log."""
    result = await _get("/breakage")
    return json.dumps(result, indent=2)


@server.tool(name="kill")
async def kill(job_id: str = "") -> str:
    """Stop running job."""
    payload = {"job_id": job_id} if job_id else {}
    result = await _post("/kill", payload)

    killed = result.get("killed")
    if killed:
        signal_name = killed.get("signal", "SIGINT")
        return f"Sent {signal_name} to job [{killed['id']}] {killed['cmd']}"
    return "Nothing running to kill."


@server.tool(name="reset")
async def reset(job_id: str = "") -> str:
    """Report a broken device / request a reset. Pass the job_id that failed.

    The server coalesces resets: if the device was already reset since your
    job ran, no new reset happens — just resubmit your job. The queue is held
    while a reset runs.
    """
    payload = {"client_id": CLIENT_ID}
    if job_id:
        payload["job_id"] = job_id
    result = await _post("/reset", payload)

    result.setdefault("hint", "")
    return json.dumps(result, indent=2)


@server.tool(name="cancel")
async def cancel(job_id: str) -> str:
    """Cancel one of your queued (not yet running) jobs. Use kill for running jobs."""
    result = await _post("/cancel", {"job_id": job_id})

    cancelled = result.get("cancelled")
    if cancelled:
        return f"Cancelled queued job [{cancelled['id']}] {cancelled['cmd']}"
    return json.dumps(result, indent=2)


if __name__ == "__main__":
    server.run(transport="stdio")
