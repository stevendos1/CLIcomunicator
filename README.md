# CLIcomunicator

`agent-hub-mcp` is a local multi-agent MCP server and job orchestrator for Claude Code, Codex/OpenAI, and Gemini CLI.

The project is now **Rust-only**, cross-platform, and designed to be easy to publish and reuse in any repository.

## What it does

- Registers a local MCP server named `agent-hub`
- Lets one AI client delegate work to other agents/models
- Supports single-agent jobs, supervisor/reviewer teams, queueing, and thread memory
- Runs locally on **Windows**, **macOS**, and **Linux**
- Stores runtime state in the user home directory instead of the repo

## Quick start

### Windows

Run **one file** from the repository root:

```powershell
.\setup.ps1
```

If you prefer double-clicking from Explorer:

```bat
setup.cmd
```

### macOS / Linux

```bash
chmod +x ./setup.sh
./setup.sh
```

## What the setup script does

1. Builds the Rust binary in release mode
2. Initializes local runtime state
3. Registers the MCP in the clients that are installed on the machine
4. Leaves the project ready to use; no long-running daemon is required

The MCP is launched **on demand** by Codex, Claude, or Gemini when they call it.

## Requirements

### Required

- **Rust 1.85+** (recommended via [rustup](https://rustup.rs/))

### Optional but useful

Install any client you want to use:

- **Codex CLI**
- **Claude Code CLI**
- **Gemini CLI**

Additional note:

- On **macOS/Linux**, automatic Gemini registration uses **Python 3**

## Runtime state

This project does **not** store user/job state inside the repository.

State is written under the current user home directory:

- Windows: `%USERPROFILE%\\.agent-hub\\`
- macOS / Linux: `~/.agent-hub/`

Main files:

- `hub.db` - SQLite queue/state
- `config.json` - local config/presets
- `jobs/` - per-job logs and outputs

## Repository structure

```text
agent-hub-mcp/
|- src/
|  `- main.rs
|- scripts/
|  |- Register-McpClients.ps1
|  `- Register-McpClients.sh
|- setup.ps1
|- setup.cmd
|- setup.sh
|- Cargo.toml
|- Cargo.lock
|- README.md
`- .gitignore
```

## How it works

1. You ask Claude/Codex/Gemini to do something
2. That client calls the `agent-hub` MCP
3. The MCP stores the job in a local SQLite queue
4. The worker launches the requested CLI/model
5. Results are persisted and can be reviewed by other agents

## Supported usage patterns

- Single delegated task
- Primary agent + reviewer agents
- Cross-provider supervision
- Thread memory for multi-step work
- Parallel agent validation

## Example CLI usage

### Submit a single job

```bash
./target/release/agent-hub-mcp submit \
  --agent codex-5.4 \
  --prompt "Review this repository" \
  --cwd "/path/to/repo" \
  --wait
```

### Run a supervised team

```bash
./target/release/agent-hub-mcp submit-supervised \
  --cwd "/path/to/repo" \
  --task "Implement feature X" \
  --primary-agent claude-sonnet \
  --reviewers-json '[{"agent":"codex-5.4"},{"agent":"gemini-3-pro-preview"}]' \
  --wait
```

### Use any model exposed by the CLI

```bash
./target/release/agent-hub-mcp submit \
  --provider codex \
  --model gpt-5.4 \
  --reasoning-effort xhigh \
  --prompt "Review this repository" \
  --wait
```

## MCP tools

- `agenthub_list_agents`
- `agenthub_list_models`
- `agenthub_submit_job`
- `agenthub_delegate_to_agent`
- `agenthub_submit_supervised_task`
- `agenthub_run_supervisor_team`
- `agenthub_get_job`
- `agenthub_list_jobs`
- `agenthub_wait_job`
- `agenthub_get_thread`
- `agenthub_add_thread_memory`
- `agenthub_dispatch`

## Discoverability: helping the AI realize it can open other agents

The server already improves this in three ways:

- clearer `initialize` instructions
- tool descriptions focused on delegation and supervision
- discoverable aliases such as `delegate_to_agent`, `run_supervisor_team`, and `list_models`

That makes it easier for the client model to detect requests like:

- "get a second opinion"
- "ask another model to review this"
- "compare two providers"
- "open a supervisor agent"
- "parallelize this task"

The most reliable phrasing is still natural and direct, for example:

- "Do this, then send it to another agent for review."
- "Use another model as a supervisor."
- "Create a team with one primary agent and two reviewers."

## Important behavior

- For **Codex/OpenAI**, the hub uses `xhigh` by default; if the selected model does not support it, the hub falls back to `high` unless the user explicitly asked for another level.
- The MCP stdio server supports both **NDJSON** and **Content-Length** framing.
- The queue recovers stale `running` jobs if the worker PID is dead or if a job never persisted `worker_pid`.
- On Windows, the hub bypasses npm `.cmd` wrappers for Codex and Gemini when possible to reduce visible terminal flashes.

## Rebuild / reinstall

Any time you pull changes, you can just run the same setup entrypoint again:

- Windows: `.\setup.ps1`
- macOS / Linux: `./setup.sh`

## Publishing notes

This repository is ready to upload:

- no hardcoded personal paths in the tracked source files
- generated build output is ignored
- runtime state lives outside the repo
- setup is reduced to a single command/file per platform
