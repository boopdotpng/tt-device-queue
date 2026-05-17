# claude-collide

FIFO job queue that serializes access to a shared resource (a GPU, a dev board, a serial port, etc.) so multiple AI agents — or humans — don't collide.

## Why

AI coding agents cannot use `flock` correctly. They forget the lock, release it early, hold it across unrelated work, or simply ignore it when told to use it. After enough wasted debugging sessions watching Claude trample its own device state, we gave up on teaching it and built a queue server instead. If the agent can only run commands by submitting them to a FIFO, it is physically impossible to collide.

## Components

- **server.py** — HTTP server (localhost:5741) that runs a FIFO job queue. Commands execute one at a time via a single worker thread. Output is saved to `/tmp/tt-device-logs/<job_id>/output`.
- **mcp_server.py** — MCP (Model Context Protocol) server that wraps the HTTP API as native tools for AI coding agents. Runs over stdio.
- **claude-collide** — CLI client for submitting jobs and checking results from the shell.

## Architecture

```
┌─────────────┐    stdio/MCP     ┌────────────────┐    HTTP     ┌────────────┐
│  AI Agent   │ ◄──────────────► │  mcp_server.py │ ──────────► │ server.py  │
│  (claude,   │                  │                │             │ :5741      │
│   codex,    │                  │  submit        │             │            │
│   opencode) │                  │  result        │             │  FIFO      │──► shared
│             │                  │  run           │             │  worker    │    resource
│             │                  │  status        │             │            │
│             │                  │  tt_smi_status │             │            │
└─────────────┘                  │  reset         │             └────────────┘
                                 └────────────────┘
```

The MCP server enables an **async two-tool pattern**: the agent calls `submit` to enqueue a command (returns immediately), does other work (reads files, writes code, plans), then calls `result` when it actually needs the output. This avoids blocking the agent during device execution.

## MCP Tools

| Tool | Blocks? | Description |
|---|---|---|
| `submit(cmd, cwd, timeout, repeat)` | No | Enqueue a command, get back a `job_id` immediately |
| `open_forever(cmd, cwd, timeout)` | No | Enqueue an intentionally long-running job that keeps the queue occupied until stopped |
| `job(job_id)` | No | Fetch structured per-job status, timestamps, repeat progress, and queue position |
| `logs(job_id, offset, limit)` | No | Read the current output file for a job without blocking |
| `tt_smi_status()` | No | Print a one-shot `tt-smi --snapshot` telemetry view without consuming a queue slot |
| `result(job_id)` | Yes | Wait for a job to finish, return full output |
| `run(cmd, cwd, timeout, repeat)` | Yes | Submit + wait in one call (convenience) |
| `status()` | No | Show running, queued, and recent jobs |
| `kill(job_id="")` | No | Stop the running job, sending Ctrl+C first and force-killing only if needed |
| `reset()` | No | Queue a device reset command |

`repeat` defaults to `1`. When set higher, the server runs the same command sequentially inside a single queued job, appends all iterations into the same output file, and still returns one `job_id` for the agent to track. It stops immediately on the first failing iteration and exposes repeat progress through `job` and `status`. Initial ETA scales with `repeat`, then refines after the first successful iteration by reusing that iteration's runtime as the per-repeat estimate.

`open_forever` is for commands that are intentionally meant to stay alive for a while, like local UI/profile servers. These jobs still use the same FIFO queue and stdout file, but they keep the queue slot occupied until they exit or the agent calls `kill(job_id)`. Manual `kill` sends Ctrl+C first and only escalates to SIGKILL if the process ignores it; timeouts send SIGKILL immediately. The default timeout for `open_forever` jobs is 180 seconds.

## Setup

```bash
git clone https://github.com/boopdotpng/claude-collide.git
cd claude-collide
./install.sh
```

The install script creates a venv, installs dependencies, symlinks `claude-collide` into `~/.local/bin`, and starts a systemd user service. At the end it prints the commands to register the MCP server with your agent.

### Manual setup

```bash
# Install dependencies (or: python3 -m venv .venv && .venv/bin/pip install mcp)
uv venv .venv
uv pip install mcp

# Start the queue server
python server.py &

# Or install as a systemd service
cp claude-collide.service ~/.config/systemd/user/
systemctl --user enable --now claude-collide
```

## Registering the MCP server

The MCP server command is:
```
/path/to/claude-collide/.venv/bin/python3 /path/to/claude-collide/mcp_server.py
```

### Claude Code

```bash
claude mcp add -s user tt-device-queue -- /path/to/claude-collide/.venv/bin/python3 /path/to/claude-collide/mcp_server.py
```

### Codex

```bash
codex mcp add tt-device-queue -- /path/to/claude-collide/.venv/bin/python3 /path/to/claude-collide/mcp_server.py
```

### OpenCode

Run `opencode mcp add` and follow the interactive prompts. Use transport `stdio` and the command above.

### Project-scoped (any tool)

Drop a `.mcp.json` in your project root:
```json
{
  "mcpServers": {
    "tt-device-queue": {
      "command": "/path/to/claude-collide/.venv/bin/python3",
      "args": ["mcp_server.py"],
      "cwd": "/path/to/claude-collide",
      "timeout": 300
    }
  }
}
```

## CLI Usage

```bash
# Submit and block until done
claude-collide exec my-command --flag arg

# Submit and run it 10 times sequentially
claude-collide --repeat 10 exec my-command --flag arg

# Submit and get job_id back immediately
claude-collide queue my-command --flag arg

# Submit an intentionally long-running command
claude-collide open my-command --serve-ui --flag arg

# Inspect one job without blocking
claude-collide job <job_id>

# Stream the current output file in chunks
claude-collide logs <job_id> [offset] [limit]

# Print a tt-smi telemetry snapshot directly without queueing
claude-collide tt-smi-status

# Check result
claude-collide result <job_id>


# Stop the currently running job with Ctrl+C first
claude-collide kill

# Stop a specific running open job
claude-collide kill <job_id>

# View queue
claude-collide status

# Queue a device reset
claude-collide reset
```

## License

MIT — Copyright (c) 2026 Claude
