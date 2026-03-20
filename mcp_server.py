#!/usr/bin/env python3
"""
MCP server for tt-device-queue.

Exposes the device queue as MCP tools so Claude Code agents can submit jobs
and retrieve results without dumb polling. The agent calls device_submit()
to enqueue a command (returns immediately), then calls device_result() when
it actually needs the output (blocks until done).

Tools:
  device_submit  — Submit a command to the device queue. Returns immediately.
  device_job     — Get non-blocking structured status for a job.
  device_logs    — Read the current output file for a job without blocking.
  device_power   — Sample board power directly without queueing.
  device_result  — Wait for a job to finish and return its full output.
  device_run     — Submit + wait in one call (convenience, blocks until done).
  device_status  — Show what's running, queued, and recently completed.
  device_reset   — Queue a device reset (tt-smi -r).

Talks to the existing tt-device-queue HTTP server on localhost:5741.
"""

import asyncio
import json
import os
from pathlib import Path

import httpx
from mcp.server.fastmcp import FastMCP

HOST = "127.0.0.1"
PORT = int(os.environ.get("TT_DEVICE_PORT", "5741"))
BASE = f"http://{HOST}:{PORT}"
DEFAULT_TIMEOUT = 60
REPO_ROOT = Path(__file__).resolve().parent
POWER_WATCH = REPO_ROOT / "power_watch.py"

# Poll interval when waiting for job completion — tight because it's localhost
POLL_INTERVAL = 0.5

server = FastMCP(
    "claude-collide",
    instructions=(
        "FIFO queue for commands that touch the GPU/device. Other agents may be "
        "using the device concurrently — all device commands MUST go through these "
        "tools, never through Bash directly. This includes: running Python scripts "
        "that use the device (ttnn, tt-metal, CUDA, etc.), pytest/tests that touch "
        "hardware, benchmarks, tt-smi, firmware tools, and anything that could "
        "conflict with another agent's device access."
    ),
)


class DeviceQueueError(Exception):
    pass


async def _post(client: httpx.AsyncClient, path: str, data: dict) -> dict:
    try:
        resp = await client.post(f"{BASE}{path}", json=data, timeout=10)
        result = resp.json()
        if resp.status_code != 200:
            raise DeviceQueueError(result.get("error", f"HTTP {resp.status_code}"))
        return result
    except httpx.ConnectError:
        raise DeviceQueueError(
            "tt-device-queue server is not running. "
            "Start it: python ~/tenstorrent/tt-device-queue/server.py &"
        )


async def _get(client: httpx.AsyncClient, path: str) -> dict:
    try:
        resp = await client.get(f"{BASE}{path}", timeout=10)
        result = resp.json()
        if resp.status_code == 404:
            raise DeviceQueueError(result.get("error", "Not found"))
        return result
    except httpx.ConnectError:
        raise DeviceQueueError(
            "tt-device-queue server is not running. "
            "Start it: python ~/tenstorrent/tt-device-queue/server.py &"
        )


async def _wait_for_job(client: httpx.AsyncClient, job_id: str) -> dict:
    """Poll until the job is done. Returns the full result with output contents.

    Uses fast initial polls (50ms) to catch instant failures, then backs off
    to POLL_INTERVAL (500ms) for longer-running jobs.
    """
    interval = 0.05  # start fast for instant failures
    while True:
        result = await _get(client, f"/result/{job_id}")
        if result["status"] == "done":
            # Read the full output file
            output_file = result.get("output_file", "")
            output_text = ""
            if output_file:
                try:
                    output_text = Path(output_file).read_text()
                except (FileNotFoundError, PermissionError):
                    output_text = f"(could not read {output_file})"

            return {
                "exit_code": result["exit_code"],
                "elapsed": result["elapsed"],
                "output_file": output_file,
                "output": output_text,
            }
        await asyncio.sleep(interval)
        interval = min(interval * 2, POLL_INTERVAL)  # backoff to 500ms


@server.tool()
async def device_submit(
    cmd: str,
    cwd: str = "",
    timeout: int = DEFAULT_TIMEOUT,
    repeat: int = 1,
) -> str:
    """Submit a command to the device queue and return immediately with a job_id.

    Use this instead of Bash for ANY command that uses the GPU/device (python
    scripts using ttnn/tt-metal/CUDA, pytest, benchmarks, tt-smi, etc.). Other
    agents may be using the device — the queue prevents conflicts.

    Returns immediately. Call device_result(job_id) when you need the output.
    Do other work (read files, write code, plan) in the meantime. If you have
    nothing else to do, use device_run() instead.

    Args:
        cmd: Shell command to run (e.g. "pytest tests/" or "python train.py")
        cwd: Working directory for the command
        timeout: Max execution time in seconds (default 120)
        repeat: Run the command this many times sequentially inside one queued job;
            all output is appended to the same output file and execution stops on
            the first failure
    """
    async with httpx.AsyncClient() as client:
        result = await _post(client, "/queue", {
            "cmd": cmd, "cwd": cwd, "timeout": timeout, "repeat": repeat,
        })

    return json.dumps({
        "job_id": result["job_id"],
        "output_file": result["output_file"],
        "position": result["position"],
        "estimated_wait_sec": result["estimated_wait_sec"],
        "estimated_run_sec": result.get("estimated_run_sec"),
        "repeat": repeat,
        "hint": "Call device_result(job_id) when you need the output. Repeat runs still use one job_id and append into one output file.",
    }, indent=2)


@server.tool()
async def device_job(job_id: str) -> str:
    """Get structured status for a queued, running, or completed job.

    This is non-blocking and is the preferred way to poll long-running jobs,
    including repeated runs, without waiting for the final result.

    Args:
        job_id: The job_id returned by device_submit()
    """
    async with httpx.AsyncClient() as client:
        result = await _get(client, f"/job/{job_id}")

    return json.dumps(result, indent=2)


@server.tool()
async def device_logs(job_id: str, offset: int = 0, limit: int = 16384) -> str:
    """Read a chunk of the current output for a job without blocking.

    This is useful for long-running jobs where you want live logs while polling
    device_job(job_id) for structured status.

    Args:
        job_id: The job_id returned by device_submit()
        offset: Byte offset to start reading from
        limit: Maximum bytes to read in one call (capped server-side)
    """
    async with httpx.AsyncClient() as client:
        result = await _get(client, f"/logs/{job_id}?offset={offset}&limit={limit}")

    return json.dumps(result, indent=2)


@server.tool()
async def device_power() -> str:
    """Sample board power directly for 3 seconds without using the queue.

    This tool is safe to run concurrently and does not consume a queue slot.
    It returns average, minimum, and maximum total board power in watts.
    """
    proc = await asyncio.create_subprocess_exec(
        "uv", "run", str(POWER_WATCH),
        cwd=str(REPO_ROOT),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await proc.communicate()
    output = stdout.decode().strip()
    error = stderr.decode().strip()

    if proc.returncode != 0:
        details = error or output or "power sampling failed"
        raise DeviceQueueError(details)

    return output


@server.tool()
async def device_result(job_id: str) -> str:
    """Wait for a previously submitted device job to finish and return its
    full output. Blocks until the job completes.

    Only call this when you actually need the result. If you have other work
    to do (reading files, writing code, planning next steps), do that first
    and call this after — the job runs in the background regardless.

    Args:
        job_id: The job_id returned by device_submit()
    """
    async with httpx.AsyncClient() as client:
        result = await _wait_for_job(client, job_id)

    exit_code = result["exit_code"]
    status = "OK" if exit_code == 0 else f"FAILED (exit code {exit_code})"

    lines = [
        f"Status: {status}",
        f"Elapsed: {result['elapsed']}s",
        f"Output file: {result['output_file']}",
        "",
        "--- Command Output ---",
        result["output"],
    ]
    return "\n".join(lines)


@server.tool()
async def device_run(
    cmd: str,
    cwd: str = "",
    timeout: int = DEFAULT_TIMEOUT,
    repeat: int = 1,
) -> str:
    """Submit a command to the device queue and wait for it to complete.

    Use this instead of Bash for ANY command that uses the GPU/device (python
    scripts using ttnn/tt-metal/CUDA, pytest, benchmarks, tt-smi, etc.). Other
    agents may be using the device — the queue prevents conflicts.

    Blocks until done. Use this when you have nothing else to do while waiting.
    If you want to do other work while the command runs, use device_submit()
    instead and call device_result() later.

    Args:
        cmd: Shell command to run (e.g. "pytest tests/" or "python train.py")
        cwd: Working directory for the command
        timeout: Max execution time in seconds (default 120)
        repeat: Run the command this many times sequentially inside one queued job;
            all output is appended to the same output file and execution stops on
            the first failure
    """
    async with httpx.AsyncClient() as client:
        submit_result = await _post(client, "/queue", {
            "cmd": cmd, "cwd": cwd, "timeout": timeout, "repeat": repeat,
        })

        job_id = submit_result["job_id"]
        result = await _wait_for_job(client, job_id)

    exit_code = result["exit_code"]
    status = "OK" if exit_code == 0 else f"FAILED (exit code {exit_code})"

    lines = [
        f"Job: {job_id}",
        f"Status: {status}",
        f"Elapsed: {result['elapsed']}s",
        f"Output file: {result['output_file']}",
        "",
        "--- Command Output ---",
        result["output"],
    ]
    return "\n".join(lines)


@server.tool()
async def device_status() -> str:
    """Show what's currently running, queued, and recently completed on the device.
    Use this to check if the device is busy before submitting work, or to see
    the history of recent jobs."""
    async with httpx.AsyncClient() as client:
        data = await _get(client, "/status")

    lines = []

    current = data.get("current")
    if current:
        lines.append(f"RUNNING: [{current['id']}] {current['cmd']}")
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
            lines.append(f"  [{p['id']}] {p['cmd']}")
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
            tag = "OK" if r.get("exit_code", 1) == 0 else f"FAIL({r.get('exit_code')})"
            repeat = r.get("repeat", 1)
            suffix = f"  repeat {r.get('repeat_completed', 0)}/{repeat}" if repeat > 1 else ""
            lines.append(f"  [{r['id']}] {tag} {r.get('elapsed', '?')}s  {r['cmd']}{suffix}")

    return "\n".join(lines)


@server.tool()
async def device_kill() -> str:
    """Kill the currently running device job immediately. Use this when a
    command is hung or you need to abort it. The job will be marked as failed
    and the next queued job will start.
    """
    async with httpx.AsyncClient() as client:
        result = await _post(client, "/kill", {})

    killed = result.get("killed")
    if killed:
        return f"Killed job [{killed['id']}] {killed['cmd']}"
    return "Nothing running to kill."


TT_SMI = os.path.expanduser("~/tenstorrent/.venv/bin/tt-smi")


@server.tool()
async def device_reset(device: int = 0) -> str:
    """Reset the Tenstorrent device via tt-smi. Queued through the FIFO like
    any other command — waits for running jobs to finish first, then resets.

    Use this when the device is in a bad state (hangs, errors, firmware
    issues, NaN outputs). Blocks until the reset completes.

    Args:
        device: Device number to reset (default 0)
    """
    cmd = f"{TT_SMI} -r {device}"
    async with httpx.AsyncClient() as client:
        submit_result = await _post(client, "/queue", {
            "cmd": cmd, "cwd": "", "timeout": 30,
        })
        job_id = submit_result["job_id"]
        result = await _wait_for_job(client, job_id)

    exit_code = result["exit_code"]
    status = "OK" if exit_code == 0 else f"FAILED (exit code {exit_code})"

    lines = [
        f"Reset device {device}: {status}",
        f"Elapsed: {result['elapsed']}s",
        "",
        result["output"],
    ]
    return "\n".join(lines)


if __name__ == "__main__":
    server.run(transport="stdio")
