#!/usr/bin/env python3
"""
MCP server for tt-device-queue.

Exposes the device queue as MCP tools so Claude Code agents can submit jobs
and retrieve results without dumb polling. The agent calls submit()
to enqueue a command (returns immediately), then calls result() when
it actually needs the output (blocks until done).

Tools:
  submit         — Submit a command to the device queue. Returns immediately.
  open_forever   — Submit an intentionally long-running command. Returns immediately.
  job            — Get non-blocking structured status for a job.
  logs           — Read the current output file for a job without blocking.
  tt_smi_status  — Print tt-smi telemetry directly without queueing.
  result         — Wait for a job to finish and return its full output.
  run            — Submit + wait in one call (convenience, blocks until done).
  status         — Show what's running, queued, and recently completed.
  kill           — Gracefully stop a running job, then escalate if needed.
  reset          — Queue a device reset (tt-smi -r).

Talks to the existing tt-device-queue HTTP server on localhost:5741.
"""

import asyncio
import json
import os

from mcp.server.fastmcp import FastMCP

from queue_client import QueueClientError, post, get, run_tt_smi_snapshot, wait_for_job

HOST = "127.0.0.1"
PORT = int(os.environ.get("TT_DEVICE_PORT", "5741"))
BASE = f"http://{HOST}:{PORT}"
DEFAULT_TIMEOUT = 60
DEFAULT_OPEN_TIMEOUT = 180

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


async def _wait_for_job(job_id: str) -> dict:
    return await asyncio.to_thread(wait_for_job, BASE, job_id, POLL_INTERVAL)


async def _post(path: str, data: dict) -> dict:
    return await asyncio.to_thread(post, BASE, path, data)


async def _get(path: str) -> dict:
    return await asyncio.to_thread(get, BASE, path)


@server.tool(name="submit")
async def submit(
    cmd: str,
    cwd: str = "",
    timeout: int = DEFAULT_TIMEOUT,
    repeat: int = 1,
) -> str:
    """Submit a command to the device queue and return immediately with a job_id.

    Use this instead of Bash for ANY command that uses the GPU/device (python
    scripts using ttnn/tt-metal/CUDA, pytest, benchmarks, tt-smi, etc.). Other
    agents may be using the device — the queue prevents conflicts.

    Returns immediately. Call result(job_id) when you need the output.
    Do other work (read files, write code, plan) in the meantime. If you have
    nothing else to do, use run() instead.

    Args:
        cmd: Shell command to run (e.g. "pytest tests/" or "python train.py")
        cwd: Working directory for the command
        timeout: Max execution time in seconds (default 120)
        repeat: Run the command this many times sequentially inside one queued job;
            all output is appended to the same output file and execution stops on
            the first failure
    """
    result = await _post("/queue", {
        "cmd": cmd, "cwd": cwd, "timeout": timeout, "repeat": repeat, "mode": "run",
    })

    return json.dumps({
        "job_id": result["job_id"],
        "output_file": result["output_file"],
        "position": result["position"],
        "estimated_wait_sec": result["estimated_wait_sec"],
        "estimated_run_sec": result.get("estimated_run_sec"),
        "repeat": repeat,
        "hint": "Call result(job_id) when you need the output. Repeat runs still use one job_id and append into one output file.",
    }, indent=2)


@server.tool(name="open_forever")
async def open_forever(
    cmd: str,
    cwd: str = "",
    timeout: int = DEFAULT_OPEN_TIMEOUT,
) -> str:
    """Start an intentionally long-running command that should stay open.

    Use this for commands that launch a local web UI or keep streaming logs
    for a while. The queue slot remains occupied until the
    process exits or you call kill(job_id). Do NOT call result() right away
    for these jobs; inspect them with job(job_id) and logs(job_id, offset,
    limit) while they are alive.

    Args:
        cmd: Shell command to run and keep open
        cwd: Working directory for the command
        timeout: Max lifetime in seconds before the server force-kills it;
            defaults to 180s and may be set higher when needed
    """
    result = await _post("/queue", {
        "cmd": cmd, "cwd": cwd, "timeout": timeout, "repeat": 1, "mode": "open",
    })

    return json.dumps({
        "job_id": result["job_id"],
        "output_file": result["output_file"],
        "position": result["position"],
        "estimated_wait_sec": result["estimated_wait_sec"],
        "estimated_run_sec": result.get("estimated_run_sec"),
        "mode": result.get("mode", "open"),
        "timeout": result.get("timeout", timeout),
        "hint": "This job keeps the queue blocked while it runs. Use job/logs to monitor it, then call kill(job_id) when done.",
    }, indent=2)


@server.tool(name="job")
async def job(job_id: str) -> str:
    """Get structured status for a queued, running, or completed job.

    This is non-blocking and is the preferred way to poll long-running jobs,
    including repeated runs, without waiting for the final result.

    Args:
        job_id: The job_id returned by submit()
    """
    result = await _get(f"/job/{job_id}")

    return json.dumps(result, indent=2)


@server.tool(name="logs")
async def logs(job_id: str, offset: int = 0, limit: int = 16384) -> str:
    """Read a chunk of the current output for a job without blocking.

    This is useful for long-running jobs where you want live logs while polling
    job(job_id) for structured status.

    Args:
        job_id: The job_id returned by submit()
        offset: Byte offset to start reading from
        limit: Maximum bytes to read in one call (capped server-side)
    """
    result = await _get(f"/logs/{job_id}?offset={offset}&limit={limit}")

    return json.dumps(result, indent=2)


@server.tool(name="tt_smi_status")
async def tt_smi_status() -> str:
    """Print a one-shot tt-smi telemetry snapshot without using the queue.

    This tool is safe to run concurrently and does not consume a queue slot.
    The snapshot includes power telemetry such as TDP and board power limit.
    """
    return await asyncio.to_thread(run_tt_smi_snapshot)


@server.tool(name="result")
async def result(job_id: str) -> str:
    """Wait for a previously submitted device job to finish and return its
    full output. Blocks until the job completes.

    Only call this when you actually need the result. If you have other work
    to do (reading files, writing code, planning next steps), do that first
    and call this after — the job runs in the background regardless.

    Args:
        job_id: The job_id returned by submit()
    """
    result = await _wait_for_job(job_id)

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


@server.tool(name="run")
async def run(
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
    If you want to do other work while the command runs, use submit()
    instead and call result() later.

    Args:
        cmd: Shell command to run (e.g. "pytest tests/" or "python train.py")
        cwd: Working directory for the command
        timeout: Max execution time in seconds (default 120)
        repeat: Run the command this many times sequentially inside one queued job;
            all output is appended to the same output file and execution stops on
            the first failure
    """
    submit_result = await _post("/queue", {
        "cmd": cmd, "cwd": cwd, "timeout": timeout, "repeat": repeat, "mode": "run",
    })

    job_id = submit_result["job_id"]
    result = await _wait_for_job(job_id)

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


@server.tool(name="status")
async def status() -> str:
    """Show what's currently running, queued, and recently completed on the device.
    Use this to check if the device is busy before submitting work, or to see
    the history of recent jobs."""
    data = await _get("/status")

    lines = []

    current = data.get("current")
    if current:
        lines.append(f"RUNNING: [{current['id']}] {current['cmd']}")
        if current.get("mode") == "open":
            lines.append("         mode open")
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
            if p.get("mode") == "open":
                lines.append("           mode open")
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


@server.tool(name="kill")
async def kill(job_id: str = "") -> str:
    """Stop a running device job, sending Ctrl+C first and escalating only if
    it refuses to exit.

    This is the normal way to stop open_forever jobs. If job_id is provided,
    it must match the currently running job. If omitted, the current running
    job is stopped.

    Args:
        job_id: Optional running job id to stop
    """
    payload = {"job_id": job_id} if job_id else {}
    result = await _post("/kill", payload)

    killed = result.get("killed")
    if killed:
        signal_name = killed.get("signal", "SIGINT")
        return f"Sent {signal_name} to job [{killed['id']}] {killed['cmd']}"
    return "Nothing running to kill."


TT_SMI = os.path.expanduser("~/tenstorrent/blackhole-py/tt-smi.py")


@server.tool(name="reset")
async def reset(device: int = 0) -> str:
    """Reset the Tenstorrent device via the blackhole-py tt-smi.py script
    (does NOT require tt-kmd). Queued through the FIFO like any other
    command — waits for running jobs to finish first, then resets.

    Use this when the device is in a bad state (hangs, errors, firmware
    issues, NaN outputs). Blocks until the reset completes.

    Args:
        device: Device number to reset (default 0)
    """
    cmd = f"{TT_SMI} -r {device}"
    submit_result = await _post("/queue", {
        "cmd": cmd, "cwd": "", "timeout": 30,
    })
    job_id = submit_result["job_id"]
    result = await _wait_for_job(job_id)

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
