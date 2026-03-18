#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="$(cd "$(dirname "$0")" && pwd)"
VENV="$REPO_DIR/.venv"

echo "=== claude-collide installer ==="
echo "Repo: $REPO_DIR"
echo ""

# 1. Create venv and install dependencies
echo "[1/4] Setting up Python venv..."
if command -v uv &>/dev/null; then
  [ -d "$VENV" ] || uv venv "$VENV"
  uv pip install --python "$VENV/bin/python3" mcp
else
  echo "  (uv not found, falling back to python3 -m venv + pip)"
  [ -d "$VENV" ] || python3 -m venv "$VENV"
  "$VENV/bin/pip" install mcp
fi

# 2. Symlink claude-collide to ~/.local/bin
echo "[2/4] Adding claude-collide to PATH..."
mkdir -p ~/.local/bin
ln -sf "$REPO_DIR/claude-collide" ~/.local/bin/claude-collide
echo "  -> ~/.local/bin/claude-collide"

# 3. Install and start systemd service
echo "[3/4] Installing systemd service..."
mkdir -p ~/.config/systemd/user
cp "$REPO_DIR/claude-collide.service" ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now claude-collide
echo "  -> claude-collide.service enabled and started"

# 4. Done — print MCP registration instructions
echo "[4/4] Done!"
echo ""
echo "=== Register the MCP server with your agent ==="
echo ""
echo "Claude Code:"
echo "  claude mcp add -s user tt-device-queue -- $VENV/bin/python3 $REPO_DIR/mcp_server.py"
echo ""
echo "Codex:"
echo "  codex mcp add tt-device-queue -- $VENV/bin/python3 $REPO_DIR/mcp_server.py"
echo ""
echo "OpenCode:"
echo "  opencode mcp add  (follow prompts, use stdio transport)"
echo "  Command: $VENV/bin/python3 $REPO_DIR/mcp_server.py"
echo ""
echo "Or drop a .mcp.json in any project root — see README.md for details."
