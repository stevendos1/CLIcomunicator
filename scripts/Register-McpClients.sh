#!/usr/bin/env bash
set -euo pipefail

tool_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
server_exe="$tool_root/target/release/agent-hub-mcp"
server_name="agent-hub"

if ! command -v cargo >/dev/null 2>&1; then
  echo "Rust cargo was not found. Install Rust with rustup from https://rustup.rs/ and run this script again." >&2
  exit 1
fi

echo "Building Rust binary in release mode..."
(
  cd "$tool_root"
  cargo build --release
)

if [[ ! -x "$server_exe" ]]; then
  echo "Could not find compiled binary: $server_exe" >&2
  exit 1
fi

echo "Initializing agent hub state..."
"$server_exe" init >/dev/null

registered=()

if command -v codex >/dev/null 2>&1; then
  echo "Registering MCP in Codex..."
  codex mcp remove "$server_name" >/dev/null 2>&1 || true
  codex mcp add "$server_name" -- "$server_exe"
  registered+=("Codex")
else
  echo "Warning: codex CLI not found; skipping Codex registration."
fi

if command -v claude >/dev/null 2>&1; then
  echo "Registering MCP in Claude Code..."
  claude mcp remove -s user "$server_name" >/dev/null 2>&1 || true
  claude mcp remove -s local "$server_name" >/dev/null 2>&1 || true
  claude mcp add -s user "$server_name" -- "$server_exe"
  registered+=("Claude Code")
else
  echo "Warning: claude CLI not found; skipping Claude registration."
fi

if command -v gemini >/dev/null 2>&1 && command -v python3 >/dev/null 2>&1; then
  echo "Registering MCP in Gemini..."
  SERVER_NAME="$server_name" SERVER_EXE="$server_exe" python3 <<'PY'
import json, os, pathlib

server_name = os.environ["SERVER_NAME"]
server_exe = os.environ["SERVER_EXE"]
settings_path = pathlib.Path.home() / ".gemini" / "settings.json"
settings_path.parent.mkdir(parents=True, exist_ok=True)

if settings_path.exists():
    raw = settings_path.read_text(encoding="utf-8")
    settings = json.loads(raw) if raw.strip() else {}
else:
    settings = {}

settings.setdefault("mcpServers", {})
settings["mcpServers"][server_name] = {
    "command": server_exe,
    "args": [],
    "env": {}
}

settings_path.write_text(
    json.dumps(settings, ensure_ascii=False, indent=2) + "\n",
    encoding="utf-8",
)
PY
  registered+=("Gemini")
elif command -v gemini >/dev/null 2>&1; then
  echo "Warning: Gemini CLI was found but python3 was not; skipping Gemini registration."
else
  echo "Warning: Gemini CLI not found; skipping Gemini registration."
fi

if [[ ${#registered[@]} -eq 0 ]]; then
  echo "Warning: no supported client CLI was detected. The binary was built successfully, but nothing was registered."
else
  printf 'Done. Registered agent-hub for: %s\n' "$(IFS=', '; echo "${registered[*]}")"
fi
