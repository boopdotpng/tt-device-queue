# tt-device-queue

Fair job queue that serializes access to a shared resource (a GPU, a dev board, a serial port, etc.) so multiple AI agents — or humans — don't collide. Scheduling is round-robin across agents (FIFO within each agent), so one agent with 50 queued jobs can't starve everyone else. The server also owns device health: resets are coalesced instead of stacking up, and an unrecoverable device fails everything with a clear reboot-required message.

## Why

AI coding agents cannot use `flock` correctly. They forget the lock, release it early, hold it across unrelated work, or simply ignore it when told to use it. After enough wasted debugging sessions watching Claude trample its own device state, we gave up on teaching it and built a queue server instead. If the agent can only run commands by submitting them to a queue, it is physically impossible to collide.

## Components

- **server.py** — HTTP server (localhost:5741) that runs the job queue. Commands execute one at a time via a single worker thread, scheduled round-robin across client ids. Output is saved to `./logs/<job_id>/output` and mirrored into `./logs/jobs.sqlite3`.
- **mcp_server.py** — MCP (Model Context Protocol) server that wraps the HTTP API as native tools for AI coding agents. Runs over stdio.

## Architecture

```
┌─────────────┐    stdio/MCP     ┌────────────────┐    HTTP     ┌────────────┐
│  AI Agent   │ ◄──────────────► │  mcp_server.py │ ──────────► │ server.py  │
│  (claude,   │                  │                │             │ :5741      │
│   codex,    │                  │  queue         │             │            │
│   opencode) │                  │  result        │             │  fair      │──► shared
│             │                  │  status        │             │  worker    │    resource
└─────────────┘                  │  reset         │             └────────────┘
                                 └────────────────┘
```

The MCP server enables an **async two-tool pattern**: the agent calls `queue` to enqueue a command (returns immediately), does other work (reads files, writes code, plans), then calls `result` when it actually needs the output. This avoids blocking the agent during device execution.

## MCP Tools

The MCP server is only for commands that touch Tenstorrent hardware. Agents
should use normal shell/tools for CPU-only or general development work such as
reading files, editing code, installing packages, building non-device projects,
starting ordinary local dev servers, or running tests that do not touch the
device.

| Tool | Blocks? | Description |
|---|---|---|
| `queue(cmd, cwd, repeat)` | No | Enqueue a command, get back a `job_id` immediately |
| `queue_python(script, cwd, repeat, python, args)` | No | Write a Python snippet to a script file, then enqueue that script |
| `job(job_id)` | No | Fetch structured per-job status, timestamps, repeat progress, and queue position |
| `logs(job_id, offset, limit)` | No | Read current or persisted job output by byte offset without blocking |
| `result(job_id)` | Yes | Wait for a job to finish, return full output |
| `status()` | No | Show running, queued, and recent jobs |
| `last_breakage()` | No | Show the last broken-device report, suspected job, output file, and reset log |
| `kill(job_id="")` | No | Stop the running job, sending Ctrl+C first and force-killing only if needed |
| `cancel(job_id)` | No | Cancel one of your queued (not yet running) jobs |
| `reset(job_id="")` | No | Report a broken device; the server coalesces resets and holds the queue while resetting |

`repeat` defaults to `1`. When set higher, the server runs the same command sequentially inside a single queued job, appends all iterations into the same output file, and still returns one `job_id` for the agent to track. It stops immediately on the first failing iteration and exposes repeat progress through `job` and `status`. Initial ETA scales with `repeat`, then refines after the first successful iteration by reusing that iteration's runtime as the per-repeat estimate.

Queued commands do not have a default timeout. Raw HTTP callers may set `timeout`; MCP callers cannot set one. If a command hits an explicit timeout, `result(job_id)` starts with `Status: TIMED OUT`, `/result` and `/job` return `timed_out: true` plus `timeout_message`, and the job log contains the timeout message.

The server automatically prepends `.` to `PYTHONPATH` for queued jobs, so agents do not need to add `PYTHONPATH=.`. Normal leading shell assignments such as `MATMUL_PROFILE=1 python3 examples/matmul_peak.py` work as expected.

Use `queue_python` instead of large `python -c` strings or heredocs. The MCP wrapper writes the snippet into `logs/mcp-scripts/` and queues a short command that runs the generated file.

Logs are persistent by default. The server stores compatibility output files in `./logs/<job_id>/output` and appends the same bytes to `./logs/jobs.sqlite3` as they are produced. Completed jobs remain available through `job`, `logs`, `result`, and `status` after the server restarts. The whole `./logs/` directory is ignored by git.

## Fair scheduling across agents

Each MCP server process generates a stable client id at startup (override with `TT_QUEUE_CLIENT_ID`; raw HTTP callers can pass `client_id` on `/queue`, otherwise they share the `anon` bucket). The scheduler keeps one FIFO subqueue per client and serves clients round-robin, so an agent that dumps 50 jobs into the queue waits its turn like everyone else, while each agent's own jobs still run in submission order. Queue positions and wait estimates reflect the simulated round-robin dispatch order.

## Device health and resets

`reset(job_id)` does not queue a reset command — it *reports* a broken device. The server coalesces reports using reset epochs: if the device was already reset since your failing job ran, you get `already_reset` (just resubmit); if a reset is pending or running, you get `joined`; otherwise one reset is `scheduled`. Twenty agents reporting the same breakage produce exactly one reset. Reset responses and `last_breakage()` include the reported job when provided, plus the server's suspected culprit: the reported job, otherwise the currently running non-reset job, otherwise the most recently completed non-reset job. That record includes the command, client, and output file.

The reset runs between jobs (the current job finishes first). While resetting, the queue is held. The first-level reset command is `~/tenstorrent/blackhole-py/reset.py -r` by default, and its exit code decides whether the device recovered. If that command keeps failing after retries, the server escalates to a **deep reset**: a PCI remove + rescan (`echo 1 > /sys/bus/pci/devices/<BDF>/remove`, then `echo 1 > /sys/bus/pci/rescan`) via a root-owned helper, followed by one more `reset.py -r`. This recovers devices that have fallen off the bus, where a first-level reset can't reach them. Only if that also fails is the device declared **dead**: every queued job is failed with a `DEVICE UNRECOVERABLE … host reboot is required` message in its output (so agents blocked on `result` see it), new submissions get HTTP 503, and `status()` shows a dead-device banner. After the host reboots, the systemd service restart brings the queue back healthy.

The deep reset needs root, but the service runs as your user — so it goes through `sudo -n /usr/local/sbin/tt-pci-deep-reset`, a fixed-path helper allowed by a single-line `/etc/sudoers.d/tt-device-queue` rule (no blanket sudo for the service). The helper also checks that sudo was spawned directly by the queue server's reset worker; direct agent-shell calls such as `sudo -n /usr/local/sbin/tt-pci-deep-reset` are refused before touching `/sys`. Install or refresh it with `sudo ./install-deep-reset.sh`. Until it's installed, `sudo -n` fails fast and behavior degrades to the old mark-dead path. Like the first-level reset, the deep reset only ever runs from the worker thread between jobs — never while a job is on the device.

Reset command and retry count are configurable via `TT_DEVICE_RESET_CMD` and `TT_DEVICE_RESET_RETRIES`; the escalation via `TT_DEVICE_PCI_BDF` (default `0000:01:00.0`) and `TT_DEVICE_DEEP_RESET_CMD`.

## Setup

```bash
git clone https://github.com/boopdotpng/tt-device-queue.git
cd tt-device-queue
./install.sh
```

The install script creates a venv, installs dependencies, starts a systemd user service, and removes any legacy CLI symlink from `~/.local/bin`. At the end it prints the commands to register the MCP server with your agent.

### Manual setup

```bash
# Install dependencies (or: python3 -m venv .venv && .venv/bin/pip install mcp)
uv venv .venv
uv pip install mcp

# Start the queue server
python server.py &

# Or install as a systemd service
cp tt-device-queue.service ~/.config/systemd/user/
systemctl --user enable --now tt-device-queue
```

## Registering the MCP server

The MCP server command is:
```
/path/to/tt-device-queue/.venv/bin/python3 /path/to/tt-device-queue/mcp_server.py
```

### Claude Code

```bash
claude mcp add -s user tt-device-queue -- /path/to/tt-device-queue/.venv/bin/python3 /path/to/tt-device-queue/mcp_server.py
```

### Codex

```bash
codex mcp add tt-device-queue -- /path/to/tt-device-queue/.venv/bin/python3 /path/to/tt-device-queue/mcp_server.py
```

### OpenCode

Run `opencode mcp add` and follow the interactive prompts. Use transport `stdio` and the command above.

### Project-scoped (any tool)

Drop a `.mcp.json` in your project root:
```json
{
  "mcpServers": {
    "tt-device-queue": {
      "command": "/path/to/tt-device-queue/.venv/bin/python3",
      "args": ["mcp_server.py"],
      "cwd": "/path/to/tt-device-queue"
    }
  }
}
```

## License

MIT — Copyright (c) 2026 Claude
