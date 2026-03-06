use std::collections::{BTreeMap, HashSet};
use std::env;
use std::fs;
use std::io::{self, BufRead, BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use chrono::{DateTime, Utc};
use clap::{ArgAction, Parser, Subcommand};
use rusqlite::{Connection, OptionalExtension, Row, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sysinfo::{Pid, ProcessesToUpdate, System};
use uuid::Uuid;
use wait_timeout::ChildExt;

#[cfg(windows)]
use std::os::windows::process::CommandExt;

const POLL_SECONDS: u64 = 1;
const DEFAULT_MAX_DEPENDENCY_CHARS: i64 = 12000;
const CREATE_NO_WINDOW: u32 = 0x0800_0000;
const DETACHED_PROCESS: u32 = 0x0000_0008;
const CREATE_NEW_PROCESS_GROUP: u32 = 0x0000_0200;
const STALE_RUNNING_WITHOUT_PID_GRACE_SECONDS: i64 = 60;
const SUPPORTED_PROTOCOL_VERSIONS: &[&str] = &[
    "2025-11-25",
    "2025-06-18",
    "2025-03-26",
    "2024-11-05",
    "2024-10-07",
];
const DEFAULT_NEGOTIATED_PROTOCOL_VERSION: &str = "2025-03-26";
const SERVER_NAME: &str = "agent-hub";
const SERVER_VERSION: &str = "2.0.0";

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AgentPreset {
    provider: String,
    model: String,
    description: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct Defaults {
    reasoning_effort: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Concurrency {
    global: i64,
    providers: BTreeMap<String, i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Config {
    version: i64,
    defaults: Defaults,
    concurrency: Concurrency,
    agents: BTreeMap<String, AgentPreset>,
}

#[derive(Debug, Clone)]
struct AgentTarget {
    provider: String,
    model: Option<String>,
    agent_name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Job {
    id: String,
    thread_id: String,
    parent_job_id: Option<String>,
    title: Option<String>,
    provider: String,
    model: Option<String>,
    agent_name: Option<String>,
    reasoning_effort: Option<String>,
    role: Option<String>,
    prompt: String,
    shared_context: Option<String>,
    cwd: Option<String>,
    priority: i64,
    status: String,
    review_mode: bool,
    timeout_seconds: Option<i64>,
    max_dependency_chars: i64,
    depends_on: Vec<String>,
    created_at: String,
    updated_at: String,
    started_at: Option<String>,
    finished_at: Option<String>,
    worker_pid: Option<i64>,
    attempts: i64,
    error: Option<String>,
    output_text: Option<String>,
    output_json: Option<Value>,
    meta: Value,
    prompt_path: Option<String>,
    stdout_path: Option<String>,
    stderr_path: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ThreadRow {
    id: String,
    title: Option<String>,
    shared_context: Option<String>,
    created_at: String,
    updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ThreadMemory {
    id: i64,
    thread_id: String,
    source_job_id: Option<String>,
    kind: String,
    content: String,
    created_at: String,
}

#[derive(Debug, Clone)]
struct ProcessResult {
    stdout: String,
    stderr: String,
    returncode: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ReviewerInput {
    agent: Option<String>,
    provider: Option<String>,
    model: Option<String>,
    #[serde(rename = "reasoningEffort")]
    reasoning_effort_camel: Option<String>,
    reasoning_effort: Option<String>,
    role: Option<String>,
    prompt: Option<String>,
    max_dependency_chars: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SynthesisInput {
    agent: Option<String>,
    provider: Option<String>,
    model: Option<String>,
    #[serde(rename = "reasoningEffort")]
    reasoning_effort_camel: Option<String>,
    reasoning_effort: Option<String>,
    role: Option<String>,
    prompt: Option<String>,
    max_dependency_chars: Option<i64>,
}

#[derive(Parser, Debug)]
#[command(name = "agent-hub-mcp", version = SERVER_VERSION)]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand, Debug)]
enum Commands {
    Serve,
    Init,
    ListAgents,
    Submit {
        #[arg(long)]
        agent: Option<String>,
        #[arg(long)]
        provider: Option<String>,
        #[arg(long)]
        model: Option<String>,
        #[arg(long = "reasoning-effort")]
        reasoning_effort: Option<String>,
        #[arg(long = "thread-id")]
        thread_id: Option<String>,
        #[arg(long = "parent-job-id")]
        parent_job_id: Option<String>,
        #[arg(long = "depends-on")]
        depends_on: Vec<String>,
        #[arg(long)]
        title: Option<String>,
        #[arg(long)]
        cwd: Option<String>,
        #[arg(long)]
        prompt: String,
        #[arg(long)]
        role: Option<String>,
        #[arg(long = "shared-context")]
        shared_context: Option<String>,
        #[arg(long, default_value_t = 0)]
        priority: i64,
        #[arg(long, action = ArgAction::SetTrue)]
        wait: bool,
        #[arg(long = "timeout-seconds")]
        timeout_seconds: Option<i64>,
        #[arg(long = "review-mode", action = ArgAction::SetTrue)]
        review_mode: bool,
    },
    SubmitSupervised {
        #[arg(long)]
        title: Option<String>,
        #[arg(long)]
        cwd: Option<String>,
        #[arg(long)]
        task: String,
        #[arg(long = "primary-agent")]
        primary_agent: Option<String>,
        #[arg(long = "primary-provider")]
        primary_provider: Option<String>,
        #[arg(long = "primary-model")]
        primary_model: Option<String>,
        #[arg(long = "primary-reasoning-effort")]
        primary_reasoning_effort: Option<String>,
        #[arg(long = "reviewers-json")]
        reviewers_json: Option<String>,
        #[arg(long = "synthesis-json")]
        synthesis_json: Option<String>,
        #[arg(long = "shared-context")]
        shared_context: Option<String>,
        #[arg(long, default_value_t = 0)]
        priority: i64,
        #[arg(long, action = ArgAction::SetTrue)]
        wait: bool,
        #[arg(long = "timeout-seconds")]
        timeout_seconds: Option<i64>,
    },
    Dispatch,
    RunJob {
        #[arg(long = "job-id")]
        job_id: String,
    },
    GetJob {
        #[arg(long = "job-id")]
        job_id: String,
    },
    ListJobs {
        #[arg(long)]
        status: Option<String>,
        #[arg(long = "thread-id")]
        thread_id: Option<String>,
        #[arg(long, default_value_t = 50)]
        limit: i64,
    },
    WaitJob {
        #[arg(long = "job-ids")]
        job_ids: Vec<String>,
        #[arg(long = "timeout-seconds")]
        timeout_seconds: Option<i64>,
    },
    GetThread {
        #[arg(long = "thread-id")]
        thread_id: String,
    },
    AddMemory {
        #[arg(long = "thread-id")]
        thread_id: String,
        #[arg(long)]
        kind: Option<String>,
        #[arg(long)]
        content: String,
        #[arg(long = "source-job-id")]
        source_job_id: Option<String>,
    },
}

fn home_dir() -> Result<PathBuf> {
    if let Ok(home) = env::var("HOME") {
        return Ok(PathBuf::from(home));
    }
    if let Ok(profile) = env::var("USERPROFILE") {
        return Ok(PathBuf::from(profile));
    }
    bail!("No pude determinar el home del usuario")
}

fn state_dir() -> Result<PathBuf> {
    Ok(home_dir()?.join(".agent-hub"))
}

fn jobs_dir() -> Result<PathBuf> {
    Ok(state_dir()?.join("jobs"))
}

fn db_path() -> Result<PathBuf> {
    Ok(state_dir()?.join("hub.db"))
}

fn config_path() -> Result<PathBuf> {
    Ok(state_dir()?.join("config.json"))
}

fn current_executable_path() -> Result<PathBuf> {
    env::current_exe().context("No pude obtener la ruta del ejecutable actual")
}

fn utc_now() -> String {
    Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string()
}

fn new_id(prefix: &str) -> String {
    format!("{}_{}", prefix, Uuid::new_v4().simple())
}

fn json_pretty(value: &Value) -> String {
    serde_json::to_string_pretty(value).unwrap_or_else(|_| value.to_string())
}

fn emit_json(value: &Value) -> Result<()> {
    let mut stdout = io::stdout().lock();
    stdout.write_all(json_pretty(value).as_bytes())?;
    stdout.write_all(b"\n")?;
    stdout.flush()?;
    Ok(())
}

fn truncate_text(input: Option<&str>, limit: usize) -> String {
    let text = input.unwrap_or("").trim();
    if text.chars().count() <= limit {
        return text.to_string();
    }
    let truncated: String = text.chars().take(limit.saturating_sub(16)).collect();
    format!("{truncated}\n...[truncated]")
}

fn merge_json(base: &mut Value, overlay: Value) {
    match (base, overlay) {
        (Value::Object(base_map), Value::Object(overlay_map)) => {
            for (key, value) in overlay_map {
                merge_json(base_map.entry(key).or_insert(Value::Null), value);
            }
        }
        (base_slot, overlay_value) => {
            *base_slot = overlay_value;
        }
    }
}

fn default_config() -> Config {
    let mut providers = BTreeMap::new();
    providers.insert("claude".to_string(), 2);
    providers.insert("codex".to_string(), 2);
    providers.insert("gemini".to_string(), 2);

    let mut defaults = Defaults::default();
    defaults
        .reasoning_effort
        .insert("codex".to_string(), "xhigh".to_string());

    let mut agents = BTreeMap::new();
    let entries = [
        (
            "claude-sonnet",
            "claude",
            "sonnet",
            "Claude Sonnet alias del CLI",
        ),
        ("claude-opus", "claude", "opus", "Claude Opus alias del CLI"),
        (
            "claude-haiku",
            "claude",
            "haiku",
            "Claude Haiku alias del CLI",
        ),
        (
            "claude-opus-4-6",
            "claude",
            "claude-opus-4-6",
            "Claude Opus 4.6 explícito",
        ),
        (
            "claude-sonnet-4-6",
            "claude",
            "claude-sonnet-4-6",
            "Claude Sonnet 4.6 explícito",
        ),
        (
            "claude-sonnet-4-20250514",
            "claude",
            "claude-sonnet-4-20250514",
            "Claude Sonnet 4 snapshot 2025-05-14",
        ),
        (
            "claude-haiku-4-5",
            "claude",
            "claude-haiku-4-5-20251001",
            "Claude Haiku 4.5 snapshot 2025-10-01",
        ),
        (
            "codex-5.4",
            "codex",
            "gpt-5.4",
            "Codex con el modelo 5.4 configurado localmente",
        ),
        ("codex-gpt-5", "codex", "gpt-5", "GPT-5 en Codex CLI"),
        ("codex-gpt-5-1", "codex", "gpt-5.1", "GPT-5.1 en Codex CLI"),
        ("codex-gpt-5-2", "codex", "gpt-5.2", "GPT-5.2 en Codex CLI"),
        ("codex-gpt-5-codex", "codex", "gpt-5-codex", "GPT-5 Codex"),
        (
            "codex-gpt-5-1-codex",
            "codex",
            "gpt-5.1-codex",
            "GPT-5.1 Codex",
        ),
        (
            "codex-gpt-5-1-codex-max",
            "codex",
            "gpt-5.1-codex-max",
            "GPT-5.1 Codex Max",
        ),
        (
            "codex-gpt-5-2-codex",
            "codex",
            "gpt-5.2-codex",
            "GPT-5.2 Codex",
        ),
        (
            "gemini-flash",
            "gemini",
            "gemini-2.5-flash-lite",
            "Gemini Flash Lite",
        ),
        (
            "gemini-main",
            "gemini",
            "gemini-3-flash-preview",
            "Gemini principal por defecto",
        ),
        (
            "gemini-2-5-pro",
            "gemini",
            "gemini-2.5-pro",
            "Gemini 2.5 Pro",
        ),
        (
            "gemini-2-5-flash",
            "gemini",
            "gemini-2.5-flash",
            "Gemini 2.5 Flash",
        ),
        (
            "gemini-2-5-flash-lite",
            "gemini",
            "gemini-2.5-flash-lite",
            "Gemini 2.5 Flash Lite",
        ),
        (
            "gemini-3-flash-preview",
            "gemini",
            "gemini-3-flash-preview",
            "Gemini 3 Flash Preview",
        ),
        (
            "gemini-3-pro-preview",
            "gemini",
            "gemini-3-pro-preview",
            "Gemini 3 Pro Preview",
        ),
    ];

    for (name, provider, model, description) in entries {
        agents.insert(
            name.to_string(),
            AgentPreset {
                provider: provider.to_string(),
                model: model.to_string(),
                description: description.to_string(),
            },
        );
    }

    Config {
        version: 1,
        defaults,
        concurrency: Concurrency {
            global: 3,
            providers,
        },
        agents,
    }
}

fn ensure_dirs() -> Result<()> {
    fs::create_dir_all(state_dir()?)?;
    fs::create_dir_all(jobs_dir()?)?;
    Ok(())
}

fn save_config(config: &Config) -> Result<()> {
    ensure_dirs()?;
    let path = config_path()?;
    fs::write(path, serde_json::to_string_pretty(config)? + "\n")?;
    Ok(())
}

fn load_config() -> Result<Config> {
    ensure_dirs()?;
    let path = config_path()?;
    if !path.exists() {
        let cfg = default_config();
        save_config(&cfg)?;
        return Ok(cfg);
    }
    let text = fs::read_to_string(&path)?;
    let existing: Value = serde_json::from_str(&text).context("No pude parsear config.json")?;
    let mut merged = serde_json::to_value(default_config())?;
    merge_json(&mut merged, existing);
    let cfg: Config = serde_json::from_value(merged)?;
    Ok(cfg)
}

fn init_storage() -> Result<Value> {
    ensure_dirs()?;
    let cfg = load_config()?;
    save_config(&cfg)?;
    let conn = open_connection()?;
    init_db(&conn)?;
    Ok(json!({
        "ok": true,
        "state_dir": state_dir()?.display().to_string(),
        "db_path": db_path()?.display().to_string(),
        "config_path": config_path()?.display().to_string(),
        "jobs_dir": jobs_dir()?.display().to_string()
    }))
}

fn open_connection() -> Result<Connection> {
    ensure_dirs()?;
    let conn = Connection::open(db_path()?)?;
    conn.pragma_update(None, "journal_mode", "WAL")?;
    conn.pragma_update(None, "foreign_keys", "ON")?;
    conn.pragma_update(None, "busy_timeout", 5000i64)?;
    Ok(conn)
}

fn init_db(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS threads (
            id TEXT PRIMARY KEY,
            title TEXT,
            shared_context TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS thread_memories (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            thread_id TEXT NOT NULL,
            source_job_id TEXT,
            kind TEXT NOT NULL,
            content TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY(thread_id) REFERENCES threads(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS jobs (
            id TEXT PRIMARY KEY,
            thread_id TEXT NOT NULL,
            parent_job_id TEXT,
            title TEXT,
            provider TEXT NOT NULL,
            model TEXT,
            agent_name TEXT,
            reasoning_effort TEXT,
            role TEXT,
            prompt TEXT NOT NULL,
            shared_context TEXT,
            cwd TEXT,
            priority INTEGER NOT NULL DEFAULT 0,
            status TEXT NOT NULL,
            review_mode INTEGER NOT NULL DEFAULT 0,
            timeout_seconds INTEGER,
            max_dependency_chars INTEGER NOT NULL DEFAULT 12000,
            depends_on_json TEXT NOT NULL DEFAULT '[]',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            started_at TEXT,
            finished_at TEXT,
            worker_pid INTEGER,
            attempts INTEGER NOT NULL DEFAULT 0,
            error TEXT,
            output_text TEXT,
            output_json TEXT,
            meta_json TEXT,
            prompt_path TEXT,
            stdout_path TEXT,
            stderr_path TEXT,
            FOREIGN KEY(thread_id) REFERENCES threads(id) ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_jobs_status_created ON jobs(status, created_at);
        CREATE INDEX IF NOT EXISTS idx_jobs_thread_created ON jobs(thread_id, created_at);
        CREATE INDEX IF NOT EXISTS idx_memories_thread_created ON thread_memories(thread_id, created_at);
        "#,
    )?;

    let mut stmt = conn.prepare("PRAGMA table_info(jobs)")?;
    let columns: HashSet<String> = stmt
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<rusqlite::Result<Vec<_>>>()?
        .into_iter()
        .collect();
    if !columns.contains("reasoning_effort") {
        conn.execute("ALTER TABLE jobs ADD COLUMN reasoning_effort TEXT", [])?;
    }
    Ok(())
}

fn value_from_json_text(text: Option<String>, fallback: Value) -> Value {
    text.and_then(|raw| serde_json::from_str(&raw).ok())
        .unwrap_or(fallback)
}

fn row_to_job(row: &Row<'_>) -> rusqlite::Result<Job> {
    let provider: String = row.get("provider")?;
    let output_json_raw: Option<String> = row.get("output_json")?;
    let mut output_json = value_from_json_text(output_json_raw, Value::Null);
    if provider == "codex" {
        if let Some(events) = output_json.get("events").and_then(|v| v.as_array()) {
            let mut messages = Vec::new();
            for event in events {
                if let Some(item) = event.get("item").and_then(|v| v.as_object()) {
                    if item.get("type").and_then(|v| v.as_str()) == Some("agent_message") {
                        let text = item.get("text").and_then(|v| v.as_str()).unwrap_or("");
                        let truncated = truncate_text(Some(text), 400);
                        if !truncated.is_empty() {
                            messages.push(Value::String(truncated));
                        }
                    }
                }
            }
            let last_event_types: Vec<Value> = events
                .iter()
                .rev()
                .take(8)
                .rev()
                .filter_map(|event| {
                    event
                        .get("type")
                        .and_then(|v| v.as_str())
                        .map(|s| Value::String(s.to_string()))
                })
                .collect();
            output_json = json!({
                "thread_id": output_json.get("thread_id").cloned().unwrap_or(Value::Null),
                "event_count": events.len(),
                "last_event_types": last_event_types,
                "agent_messages": messages.into_iter().rev().take(3).collect::<Vec<_>>().into_iter().rev().collect::<Vec<_>>()
            });
        }
    } else if output_json == Value::Null {
        output_json = Value::Null;
    }

    let depends_on_text: String = row.get("depends_on_json")?;
    let depends_on: Vec<String> = serde_json::from_str(&depends_on_text).unwrap_or_default();
    let meta_json = value_from_json_text(row.get("meta_json")?, json!({}));

    Ok(Job {
        id: row.get("id")?,
        thread_id: row.get("thread_id")?,
        parent_job_id: row.get("parent_job_id")?,
        title: row.get("title")?,
        provider,
        model: row.get("model")?,
        agent_name: row.get("agent_name")?,
        reasoning_effort: row.get("reasoning_effort")?,
        role: row.get("role")?,
        prompt: row.get("prompt")?,
        shared_context: row.get("shared_context")?,
        cwd: row.get("cwd")?,
        priority: row.get("priority")?,
        status: row.get("status")?,
        review_mode: row.get::<_, i64>("review_mode")? != 0,
        timeout_seconds: row.get("timeout_seconds")?,
        max_dependency_chars: row.get("max_dependency_chars")?,
        depends_on,
        created_at: row.get("created_at")?,
        updated_at: row.get("updated_at")?,
        started_at: row.get("started_at")?,
        finished_at: row.get("finished_at")?,
        worker_pid: row.get("worker_pid")?,
        attempts: row.get("attempts")?,
        error: row.get("error")?,
        output_text: row.get("output_text")?,
        output_json: if output_json.is_null() {
            None
        } else {
            Some(output_json)
        },
        meta: meta_json,
        prompt_path: row.get("prompt_path")?,
        stdout_path: row.get("stdout_path")?,
        stderr_path: row.get("stderr_path")?,
    })
}

fn row_to_thread(row: &Row<'_>) -> rusqlite::Result<ThreadRow> {
    Ok(ThreadRow {
        id: row.get("id")?,
        title: row.get("title")?,
        shared_context: row.get("shared_context")?,
        created_at: row.get("created_at")?,
        updated_at: row.get("updated_at")?,
    })
}

fn row_to_memory(row: &Row<'_>) -> rusqlite::Result<ThreadMemory> {
    Ok(ThreadMemory {
        id: row.get("id")?,
        thread_id: row.get("thread_id")?,
        source_job_id: row.get("source_job_id")?,
        kind: row.get("kind")?,
        content: row.get("content")?,
        created_at: row.get("created_at")?,
    })
}

fn get_thread_row(conn: &Connection, thread_id: &str) -> Result<ThreadRow> {
    conn.query_row(
        "SELECT * FROM threads WHERE id = ?",
        [thread_id],
        row_to_thread,
    )
    .optional()?
    .ok_or_else(|| anyhow!("Thread no encontrado: {thread_id}"))
}

fn get_job(conn: &Connection, job_id: &str) -> Result<Job> {
    require_non_empty(job_id, "jobId")?;
    conn.query_row("SELECT * FROM jobs WHERE id = ?", [job_id], row_to_job)
        .optional()?
        .ok_or_else(|| anyhow!("Job no encontrado: {job_id}"))
}

fn ensure_thread(
    conn: &Connection,
    thread_id: Option<&str>,
    title: Option<&str>,
    shared_context: Option<&str>,
) -> Result<String> {
    let now = utc_now();
    let thread_id = thread_id
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .unwrap_or_else(|| new_id("thread"));
    let existing = conn
        .query_row(
            "SELECT * FROM threads WHERE id = ?",
            [thread_id.as_str()],
            row_to_thread,
        )
        .optional()?;
    if let Some(_) = existing {
        let mut updates = Vec::new();
        let mut params_vec: Vec<Value> = Vec::new();
        if let Some(title) = title {
            updates.push("title = ?");
            params_vec.push(Value::String(title.to_string()));
        }
        if let Some(shared_context) = shared_context {
            updates.push("shared_context = ?");
            params_vec.push(Value::String(shared_context.to_string()));
        }
        if !updates.is_empty() {
            updates.push("updated_at = ?");
            params_vec.push(Value::String(now));
            let statement = format!("UPDATE threads SET {} WHERE id = ?", updates.join(", "));
            let mut values: Vec<String> = params_vec
                .into_iter()
                .map(|v| v.as_str().unwrap_or_default().to_string())
                .collect();
            values.push(thread_id.clone());
            let params_refs: Vec<&dyn rusqlite::ToSql> =
                values.iter().map(|v| v as &dyn rusqlite::ToSql).collect();
            conn.execute(statement.as_str(), params_refs.as_slice())?;
        }
        return Ok(thread_id);
    }

    conn.execute(
        "INSERT INTO threads (id, title, shared_context, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
        params![thread_id, title, shared_context, now, utc_now()],
    )?;
    Ok(thread_id)
}

fn add_thread_memory(
    conn: &Connection,
    thread_id: &str,
    content: &str,
    kind: &str,
    source_job_id: Option<&str>,
) -> Result<ThreadMemory> {
    require_non_empty(thread_id, "threadId")?;
    require_non_empty(content, "content")?;
    let _ = get_thread_row(conn, thread_id)?;
    let now = utc_now();
    conn.execute(
        "INSERT INTO thread_memories (thread_id, source_job_id, kind, content, created_at) VALUES (?, ?, ?, ?, ?)",
        params![thread_id, source_job_id, kind, content, now],
    )?;
    let id = conn.last_insert_rowid();
    conn.query_row(
        "SELECT * FROM thread_memories WHERE id = ?",
        [id],
        row_to_memory,
    )
    .map_err(Into::into)
}

fn list_thread_memories(
    conn: &Connection,
    thread_id: &str,
    limit: i64,
) -> Result<Vec<ThreadMemory>> {
    let mut stmt =
        conn.prepare("SELECT * FROM thread_memories WHERE thread_id = ? ORDER BY id DESC LIMIT ?")?;
    let mut rows = stmt.query(params![thread_id, limit])?;
    let mut items = Vec::new();
    while let Some(row) = rows.next()? {
        items.push(row_to_memory(row)?);
    }
    items.reverse();
    Ok(items)
}

fn list_jobs(
    conn: &Connection,
    status: Option<&str>,
    thread_id: Option<&str>,
    limit: i64,
) -> Result<Vec<Job>> {
    let mut sql = "SELECT * FROM jobs".to_string();
    let mut conditions = Vec::new();
    let mut owned_params: Vec<String> = Vec::new();
    if let Some(status) = status {
        conditions.push("status = ?");
        owned_params.push(status.to_string());
    }
    if let Some(thread_id) = thread_id {
        conditions.push("thread_id = ?");
        owned_params.push(thread_id.to_string());
    }
    if !conditions.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&conditions.join(" AND "));
    }
    sql.push_str(" ORDER BY priority DESC, created_at ASC LIMIT ?");
    let limit_owned = limit.to_string();
    owned_params.push(limit_owned);
    let params_refs: Vec<&dyn rusqlite::ToSql> = owned_params
        .iter()
        .map(|v| v as &dyn rusqlite::ToSql)
        .collect();
    let mut stmt = conn.prepare(&sql)?;
    let mut rows = stmt.query(params_refs.as_slice())?;
    let mut jobs = Vec::new();
    while let Some(row) = rows.next()? {
        jobs.push(row_to_job(row)?);
    }
    Ok(jobs)
}

fn get_thread(conn: &Connection, thread_id: &str) -> Result<Value> {
    require_non_empty(thread_id, "threadId")?;
    let thread = get_thread_row(conn, thread_id)?;
    let jobs = list_jobs(conn, None, Some(thread_id), 500)?;
    let memories = list_thread_memories(conn, thread_id, 100)?;
    Ok(json!({
        "thread": thread,
        "jobs": jobs.into_iter().map(job_summary).collect::<Vec<_>>(),
        "memories": memories
    }))
}

fn resolve_target(
    config: &Config,
    agent: Option<&str>,
    provider: Option<&str>,
    model: Option<&str>,
) -> Result<AgentTarget> {
    let mut provider_owned = provider.map(|s| s.to_string());
    let mut model_owned = model.map(|s| s.to_string());
    if let Some(agent_name) = agent {
        let preset = config
            .agents
            .get(agent_name)
            .ok_or_else(|| anyhow!("Preset de agente desconocido: {agent_name}"))?;
        if provider_owned.is_none() {
            provider_owned = Some(preset.provider.clone());
        }
        if model_owned.is_none() {
            model_owned = Some(preset.model.clone());
        }
    }

    let provider_value = provider_owned
        .ok_or_else(|| anyhow!("Debes indicar `agent` o `provider` para crear el job."))?;
    let provider_lower = provider_value.to_lowercase();
    if !matches!(provider_lower.as_str(), "claude" | "codex" | "gemini") {
        bail!("Provider no soportado: {provider_lower}");
    }

    Ok(AgentTarget {
        provider: provider_lower,
        model: model_owned,
        agent_name: agent.map(|s| s.to_string()),
    })
}

fn normalize_depends_on(depends_on: &[String]) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut ordered = Vec::new();
    for item in depends_on {
        let trimmed = item.trim();
        if trimmed.is_empty() {
            continue;
        }
        if seen.insert(trimmed.to_string()) {
            ordered.push(trimmed.to_string());
        }
    }
    ordered
}

fn require_non_empty(value: &str, field: &str) -> Result<()> {
    if value.trim().is_empty() {
        bail!("`{field}` no puede estar vacío.");
    }
    Ok(())
}

fn create_job(
    conn: &Connection,
    agent: Option<&str>,
    provider: Option<&str>,
    model: Option<&str>,
    reasoning_effort: Option<&str>,
    thread_id: Option<&str>,
    parent_job_id: Option<&str>,
    depends_on: &[String],
    title: Option<&str>,
    cwd: Option<&str>,
    prompt: &str,
    role: Option<&str>,
    shared_context: Option<&str>,
    priority: i64,
    timeout_seconds: Option<i64>,
    review_mode: bool,
    max_dependency_chars: Option<i64>,
) -> Result<Job> {
    require_non_empty(prompt, "prompt")?;
    let config = load_config()?;
    let target = resolve_target(&config, agent, provider, model)?;
    let now = utc_now();
    let job_id = new_id("job");
    let depends = normalize_depends_on(depends_on);

    let cwd_owned = if let Some(cwd) = cwd {
        let path = PathBuf::from(cwd);
        if !path.exists() {
            bail!("El cwd no existe: {cwd}");
        }
        Some(path.display().to_string())
    } else {
        None
    };

    let thread_title = title
        .map(|s| s.to_string())
        .unwrap_or_else(|| truncate_text(Some(&prompt.replace(['\r', '\n'], " ")), 120));
    let thread_id_value =
        ensure_thread(conn, thread_id, Some(thread_title.as_str()), shared_context)?;

    if let Some(parent_job_id) = parent_job_id {
        let _ = get_job(conn, parent_job_id)?;
    }
    for dep in &depends {
        let _ = get_job(conn, dep)?;
    }

    conn.execute(
        "INSERT INTO jobs (
            id, thread_id, parent_job_id, title, provider, model, agent_name, reasoning_effort, role, prompt,
            shared_context, cwd, priority, status, review_mode, timeout_seconds, max_dependency_chars,
            depends_on_json, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?, ?, ?, ?, ?)",
        params![
            job_id,
            thread_id_value,
            parent_job_id,
            title,
            target.provider,
            target.model,
            target.agent_name,
            reasoning_effort,
            role,
            prompt,
            shared_context,
            cwd_owned,
            priority,
            if review_mode { 1 } else { 0 },
            timeout_seconds,
            max_dependency_chars.unwrap_or(DEFAULT_MAX_DEPENDENCY_CHARS),
            serde_json::to_string(&depends)?,
            now,
            utc_now()
        ],
    )?;

    get_job(conn, &job_id)
}

fn set_job_status(
    conn: &Connection,
    job_id: &str,
    status: &str,
    started_at: Option<&str>,
    finished_at: Option<&str>,
    worker_pid: Option<i64>,
    error: Option<&str>,
    output_text: Option<&str>,
    output_json: Option<&Value>,
    meta: Option<&Value>,
    prompt_path: Option<&str>,
    stdout_path: Option<&str>,
    stderr_path: Option<&str>,
    attempts_increment: bool,
) -> Result<Job> {
    let output_json_text = output_json
        .map(|v| serde_json::to_string_pretty(v))
        .transpose()?;
    let meta_text = meta.map(|v| serde_json::to_string_pretty(v)).transpose()?;
    let mut sql = String::from("UPDATE jobs SET status = ?, updated_at = ?");
    let mut values: Vec<Box<dyn rusqlite::ToSql>> =
        vec![Box::new(status.to_string()), Box::new(utc_now())];

    if let Some(started_at) = started_at {
        sql.push_str(", started_at = ?");
        values.push(Box::new(started_at.to_string()));
    }
    if let Some(finished_at) = finished_at {
        sql.push_str(", finished_at = ?");
        values.push(Box::new(finished_at.to_string()));
    }
    if let Some(worker_pid) = worker_pid {
        sql.push_str(", worker_pid = ?");
        values.push(Box::new(worker_pid));
    }
    if let Some(error) = error {
        sql.push_str(", error = ?");
        values.push(Box::new(error.to_string()));
    }
    if let Some(output_text) = output_text {
        sql.push_str(", output_text = ?");
        values.push(Box::new(output_text.to_string()));
    }
    if let Some(output_json_text) = output_json_text {
        sql.push_str(", output_json = ?");
        values.push(Box::new(output_json_text));
    }
    if let Some(meta_text) = meta_text {
        sql.push_str(", meta_json = ?");
        values.push(Box::new(meta_text));
    }
    if let Some(prompt_path) = prompt_path {
        sql.push_str(", prompt_path = ?");
        values.push(Box::new(prompt_path.to_string()));
    }
    if let Some(stdout_path) = stdout_path {
        sql.push_str(", stdout_path = ?");
        values.push(Box::new(stdout_path.to_string()));
    }
    if let Some(stderr_path) = stderr_path {
        sql.push_str(", stderr_path = ?");
        values.push(Box::new(stderr_path.to_string()));
    }
    if attempts_increment {
        sql.push_str(", attempts = attempts + 1");
    }
    sql.push_str(" WHERE id = ?");
    values.push(Box::new(job_id.to_string()));
    let params_refs: Vec<&dyn rusqlite::ToSql> = values
        .iter()
        .map(|v| v.as_ref() as &dyn rusqlite::ToSql)
        .collect();
    conn.execute(sql.as_str(), params_refs.as_slice())?;
    get_job(conn, job_id)
}

fn summarize_job_for_memory(job: &Job) -> String {
    let title = job.title.as_deref().unwrap_or("(sin título)");
    let model = job.model.as_deref().unwrap_or("default");
    let summary = job.output_text.as_deref().or(job.error.as_deref());
    let body = truncate_text(summary, 3500);
    format!(
        "[{}] {} {}/{} — {}\n{}",
        job.status, job.id, job.provider, model, title, body
    )
}

fn default_reasoning_effort(provider: &str) -> Result<Option<String>> {
    let cfg = load_config()?;
    Ok(cfg.defaults.reasoning_effort.get(provider).cloned())
}

fn job_summary(job: Job) -> Value {
    json!({
        "id": job.id,
        "thread_id": job.thread_id,
        "parent_job_id": job.parent_job_id,
        "title": job.title,
        "provider": job.provider,
        "model": job.model,
        "agent_name": job.agent_name,
        "reasoning_effort": job.reasoning_effort,
        "role": job.role,
        "status": job.status,
        "priority": job.priority,
        "review_mode": job.review_mode,
        "depends_on": job.depends_on,
        "created_at": job.created_at,
        "started_at": job.started_at,
        "finished_at": job.finished_at,
        "cwd": job.cwd,
        "output_excerpt": job.output_text.as_deref().map(|s| truncate_text(Some(s), 500)),
        "error_excerpt": job.error.as_deref().map(|s| truncate_text(Some(s), 500))
    })
}

fn get_dependency_jobs(conn: &Connection, depends_on: &[String]) -> Result<Vec<Job>> {
    depends_on
        .iter()
        .map(|job_id| get_job(conn, job_id))
        .collect()
}

fn build_job_prompt(conn: &Connection, job: &Job) -> Result<String> {
    let thread = get_thread_row(conn, &job.thread_id)?;
    let memories = list_thread_memories(conn, &job.thread_id, 12)?;
    let dep_jobs = get_dependency_jobs(conn, &job.depends_on)?;
    let mut pieces = vec![
        "You are participating in a local multi-agent workflow.".to_string(),
        "Produce the best possible result for your assigned task.".to_string(),
    ];

    if let Some(role) = &job.role {
        pieces.push(format!("Assigned role: {role}"));
    }
    if job.review_mode {
        pieces.push("This is a review/supervision task. Critique the prerequisite outputs, identify issues, missing tests, regressions, risks, and concrete improvements.".to_string());
    }
    if let Some(shared) = &thread.shared_context {
        pieces.push(format!("Shared thread context:\n{shared}"));
    }
    if let Some(job_context) = &job.shared_context {
        if thread.shared_context.as_ref() != Some(job_context) {
            pieces.push(format!("Additional job context:\n{job_context}"));
        }
    }
    if !memories.is_empty() {
        let mut lines = Vec::new();
        for memory in memories {
            let mut header = format!("- {} [{}]", memory.created_at, memory.kind);
            if let Some(source_job_id) = memory.source_job_id {
                header.push_str(&format!(" from {source_job_id}"));
            }
            lines.push(format!(
                "{}\n{}",
                header,
                truncate_text(Some(&memory.content), 600)
            ));
        }
        pieces.push(format!("Recent shared memory:\n{}", lines.join("\n")));
    }
    if !dep_jobs.is_empty() {
        let max_total = job.max_dependency_chars.max(DEFAULT_MAX_DEPENDENCY_CHARS);
        let per_dep = std::cmp::max(1200_i64, max_total / dep_jobs.len() as i64) as usize;
        let mut blocks = Vec::new();
        for dep in dep_jobs {
            let dep_text = dep
                .output_text
                .as_deref()
                .or(dep.error.as_deref())
                .unwrap_or("(dependency without output)");
            let dep_label = dep.title.clone().unwrap_or_else(|| dep.id.clone());
            blocks.push(format!(
                "<<DEPENDENCY {}>>\nDependency {} — {}\nProvider/model: {}/{}\nStatus: {}\nOriginal prompt:\n{}\nContent:\n{}\n<<END DEPENDENCY {}>>",
                dep.id,
                dep.id,
                dep_label,
                dep.provider,
                dep.model.clone().unwrap_or_else(|| "default".to_string()),
                dep.status,
                truncate_text(Some(&dep.prompt), 800),
                truncate_text(Some(dep_text), per_dep),
                dep.id
            ));
        }
        pieces.push(format!(
            "Outputs from prerequisite jobs are included below. Use them directly in your answer; do not ask the user to resend them unless a dependency block is literally empty.\n\n{}",
            blocks.join("\n\n")
        ));
    }
    if let Some(cwd) = &job.cwd {
        pieces.push(format!("Working directory for this job: {cwd}"));
    }
    pieces.push(format!("Task:\n{}", job.prompt.trim()));
    if job.review_mode {
        pieces.push("Return the review in this structure:\n1. Executive summary\n2. Findings\n3. Risks / blind spots\n4. Recommended follow-up".to_string());
    }
    Ok(pieces.join("\n\n"))
}

fn resolve_cli_binary(name: &str) -> Result<PathBuf> {
    let mut candidates = Vec::new();
    if let Ok(path) = which::which(name) {
        candidates.push(path);
    }
    if let Ok(path) = which::which(format!("{name}.cmd")) {
        candidates.push(path);
    }
    if let Ok(path) = which::which(format!("{name}.exe")) {
        candidates.push(path);
    }
    if let Ok(appdata) = env::var("APPDATA") {
        candidates.push(
            PathBuf::from(&appdata)
                .join("npm")
                .join(format!("{name}.cmd")),
        );
        candidates.push(
            PathBuf::from(&appdata)
                .join("npm")
                .join(format!("{name}.exe")),
        );
    }
    candidates.push(
        home_dir()?
            .join(".local")
            .join("bin")
            .join(format!("{name}.exe")),
    );
    for candidate in candidates {
        if candidate.exists() {
            return Ok(candidate);
        }
    }
    bail!("No pude encontrar el ejecutable para `{name}` en PATH.")
}

#[derive(Debug, Clone)]
struct LaunchTarget {
    program: PathBuf,
    args_prefix: Vec<String>,
}

fn resolve_node_binary() -> Result<PathBuf> {
    if let Ok(path) = which::which("node.exe") {
        return Ok(path);
    }
    if let Ok(path) = which::which("node") {
        return Ok(path);
    }
    if let Ok(program_files) = env::var("ProgramFiles") {
        let candidate = PathBuf::from(program_files).join("nodejs").join("node.exe");
        if candidate.exists() {
            return Ok(candidate);
        }
    }
    bail!("No pude encontrar `node.exe` para lanzar el CLI sin wrappers .cmd.")
}

#[cfg(windows)]
fn resolve_windows_npm_js_target(
    module_segments: &[&str],
    script_segments: &[&str],
    args_prefix: &[&str],
) -> Result<LaunchTarget> {
    let appdata = env::var("APPDATA").context("APPDATA no está definido")?;
    let mut script = PathBuf::from(appdata).join("npm").join("node_modules");
    for segment in module_segments {
        script.push(segment);
    }
    for segment in script_segments {
        script.push(segment);
    }
    if !script.exists() {
        bail!("No encontré el script npm esperado: {}", script.display());
    }
    let mut resolved_args = args_prefix
        .iter()
        .map(|value| value.to_string())
        .collect::<Vec<_>>();
    resolved_args.push(script.display().to_string());
    Ok(LaunchTarget {
        program: resolve_node_binary()?,
        args_prefix: resolved_args,
    })
}

fn resolve_cli_target(name: &str) -> Result<LaunchTarget> {
    #[cfg(windows)]
    {
        match name {
            "codex" => {
                if let Ok(target) =
                    resolve_windows_npm_js_target(&["@openai", "codex"], &["bin", "codex.js"], &[])
                {
                    return Ok(target);
                }
            }
            "gemini" => {
                if let Ok(target) = resolve_windows_npm_js_target(
                    &["@google", "gemini-cli"],
                    &["dist", "index.js"],
                    &["--no-warnings=DEP0040"],
                ) {
                    return Ok(target);
                }
            }
            _ => {}
        }
    }

    Ok(LaunchTarget {
        program: resolve_cli_binary(name)?,
        args_prefix: Vec::new(),
    })
}

fn run_process(
    program: &Path,
    args: &[String],
    prompt: &str,
    cwd: Option<&str>,
    timeout_seconds: Option<i64>,
) -> Result<ProcessResult> {
    let mut command = Command::new(program);
    command.args(args);
    if let Some(cwd) = cwd {
        command.current_dir(cwd);
    }
    command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    command
        .env("CI", "1")
        .env("NO_COLOR", "1")
        .env("TERM", "dumb");
    #[cfg(windows)]
    {
        command.creation_flags(CREATE_NO_WINDOW | CREATE_NEW_PROCESS_GROUP);
    }
    let mut child = command
        .spawn()
        .with_context(|| format!("No pude lanzar {}", program.display()))?;
    if let Some(mut stdin) = child.stdin.take() {
        stdin.write_all(prompt.as_bytes())?;
    }
    let timeout = timeout_seconds.map(|seconds| Duration::from_secs(seconds as u64));
    let status = match timeout {
        Some(timeout) => {
            if let Some(status) = child.wait_timeout(timeout)? {
                status
            } else {
                let _ = child.kill();
                let _ = child.wait();
                bail!("El job excedió el timeout de {timeout_seconds:?} segundos.")
            }
        }
        None => child.wait()?,
    };
    let mut stdout = String::new();
    let mut stderr = String::new();
    if let Some(mut out) = child.stdout.take() {
        out.read_to_string(&mut stdout)?;
    }
    if let Some(mut err) = child.stderr.take() {
        err.read_to_string(&mut stderr)?;
    }
    Ok(ProcessResult {
        stdout,
        stderr,
        returncode: status.code().unwrap_or(-1),
    })
}

fn parse_first_json_object(text: &str) -> Result<Value> {
    let trimmed = text.trim_start();
    let mut deserializer = serde_json::Deserializer::from_str(trimmed);
    let value = Value::deserialize(&mut deserializer)?;
    Ok(value)
}

fn parse_claude_output(stdout: &str) -> Result<(String, Value, Value)> {
    let payload: Value = serde_json::from_str(stdout.trim())?;
    let output_text = payload
        .get("result")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .trim()
        .to_string();
    let meta = json!({
        "session_id": payload.get("session_id"),
        "stop_reason": payload.get("stop_reason"),
        "usage": payload.get("usage"),
        "model_usage": payload.get("modelUsage")
    });
    Ok((output_text, payload, meta))
}

fn parse_codex_output(stdout: &str) -> Result<(String, Value, Value)> {
    let mut events = Vec::new();
    let mut messages = Vec::new();
    let mut thread_id = None;
    let mut usage = Value::Null;
    let mut error_message = None;
    for line in stdout.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let event: Value = serde_json::from_str(trimmed)?;
        if event.get("type").and_then(|v| v.as_str()) == Some("thread.started") {
            thread_id = event
                .get("thread_id")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
        }
        if event.get("type").and_then(|v| v.as_str()) == Some("turn.completed") {
            usage = event.get("usage").cloned().unwrap_or(Value::Null);
        }
        if event.get("type").and_then(|v| v.as_str()) == Some("error") {
            error_message = event
                .get("message")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
        }
        if event.get("type").and_then(|v| v.as_str()) == Some("turn.failed") {
            error_message = event
                .get("error")
                .and_then(|v| v.get("message"))
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
                .or(error_message);
        }
        if let Some(item) = event.get("item").and_then(|v| v.as_object()) {
            if item.get("type").and_then(|v| v.as_str()) == Some("agent_message") {
                let text = item
                    .get("text")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .trim();
                if !text.is_empty() {
                    messages.push(text.to_string());
                }
            }
        }
        events.push(event);
    }
    if let Some(error_message) = error_message {
        bail!("{error_message}");
    }
    let output_text = messages.join("\n\n");
    let output_json = json!({
        "thread_id": thread_id,
        "event_count": events.len(),
        "last_event_types": events.iter().rev().take(8).rev().filter_map(|e| e.get("type").and_then(|v| v.as_str()).map(|s| s.to_string())).collect::<Vec<_>>(),
        "agent_messages": messages.iter().rev().take(3).cloned().collect::<Vec<_>>().into_iter().rev().collect::<Vec<_>>()
    });
    let meta = json!({
        "thread_id": output_json.get("thread_id"),
        "usage": usage
    });
    Ok((output_text, output_json, meta))
}

fn parse_gemini_output(stdout: &str) -> Result<(String, Value, Value)> {
    let payload = parse_first_json_object(stdout)?;
    let output_text = payload
        .get("response")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .trim()
        .to_string();
    let meta = json!({
        "session_id": payload.get("session_id"),
        "stats": payload.get("stats")
    });
    Ok((output_text, payload, meta))
}

fn execute_provider_job(
    job: &Job,
    prompt: &str,
) -> Result<(String, Value, Value, String, String, i32)> {
    let launch = resolve_cli_target(&job.provider)?;
    let cwd = job.cwd.as_deref();
    let timeout_seconds = job.timeout_seconds;

    match job.provider.as_str() {
        "claude" => {
            let mut args = launch.args_prefix.clone();
            args.extend([
                "-p".to_string(),
                "--output-format".to_string(),
                "json".to_string(),
                "--permission-mode".to_string(),
                "bypassPermissions".to_string(),
                "--no-session-persistence".to_string(),
            ]);
            if let Some(model) = &job.model {
                args.push("--model".to_string());
                args.push(model.clone());
            }
            let result = run_process(&launch.program, &args, prompt, cwd, timeout_seconds)?;
            if result.returncode != 0 {
                bail!(
                    "{}",
                    if !result.stderr.trim().is_empty() {
                        result.stderr.trim().to_string()
                    } else {
                        result.stdout.trim().to_string()
                    }
                );
            }
            let (output_text, output_json, meta) = parse_claude_output(&result.stdout)?;
            Ok((
                output_text,
                output_json,
                meta,
                result.stdout,
                result.stderr,
                result.returncode,
            ))
        }
        "codex" => {
            let requested_effort = job
                .reasoning_effort
                .clone()
                .or(default_reasoning_effort("codex")?);
            let run_codex_once =
                |effort: Option<&str>| -> Result<(String, Value, Value, String, String, i32)> {
                    let mut args = launch.args_prefix.clone();
                    args.extend([
                        "exec".to_string(),
                        "--json".to_string(),
                        "--skip-git-repo-check".to_string(),
                        "--dangerously-bypass-approvals-and-sandbox".to_string(),
                        "--ephemeral".to_string(),
                    ]);
                    if let Some(effort) = effort {
                        args.push("-c".to_string());
                        args.push(format!("model_reasoning_effort=\"{effort}\""));
                    }
                    if let Some(model) = &job.model {
                        args.push("--model".to_string());
                        args.push(model.clone());
                    }
                    if let Some(cwd) = cwd {
                        args.push("-C".to_string());
                        args.push(cwd.to_string());
                    }
                    args.push("-".to_string());
                    let result = run_process(&launch.program, &args, prompt, cwd, timeout_seconds)?;
                    if result.returncode != 0 {
                        bail!(
                            "{}",
                            if !result.stderr.trim().is_empty() {
                                result.stderr.trim().to_string()
                            } else {
                                result.stdout.trim().to_string()
                            }
                        );
                    }
                    let (output_text, output_json, mut meta) = parse_codex_output(&result.stdout)?;
                    if let Some(meta_obj) = meta.as_object_mut() {
                        meta_obj.insert(
                            "reasoning_effort_used".to_string(),
                            effort
                                .map(|s| Value::String(s.to_string()))
                                .unwrap_or(Value::Null),
                        );
                    }
                    Ok((
                        output_text,
                        output_json,
                        meta,
                        result.stdout,
                        result.stderr,
                        result.returncode,
                    ))
                };

            match run_codex_once(requested_effort.as_deref()) {
                Ok(result) => Ok(result),
                Err(err) => {
                    let message = err.to_string();
                    if job.reasoning_effort.is_none()
                        && requested_effort.as_deref() == Some("xhigh")
                        && message.contains("reasoning.effort")
                        && message.contains("Unsupported value")
                    {
                        let (output_text, output_json, mut meta, stdout, stderr, returncode) =
                            run_codex_once(Some("high"))?;
                        if let Some(meta_obj) = meta.as_object_mut() {
                            meta_obj.insert(
                                "reasoning_effort_fallback_from".to_string(),
                                Value::String("xhigh".to_string()),
                            );
                        }
                        Ok((output_text, output_json, meta, stdout, stderr, returncode))
                    } else {
                        Err(err)
                    }
                }
            }
        }
        "gemini" => {
            let mut args = launch.args_prefix.clone();
            args.extend([
                "-p".to_string(),
                " ".to_string(),
                "-o".to_string(),
                "json".to_string(),
                "--approval-mode".to_string(),
                "yolo".to_string(),
            ]);
            if let Some(model) = &job.model {
                args.push("-m".to_string());
                args.push(model.clone());
            }
            if let Some(cwd) = cwd {
                args.push("--include-directories".to_string());
                args.push(cwd.to_string());
            }
            let result = run_process(&launch.program, &args, prompt, cwd, timeout_seconds)?;
            if result.returncode != 0 {
                bail!(
                    "{}",
                    if !result.stderr.trim().is_empty() {
                        result.stderr.trim().to_string()
                    } else {
                        result.stdout.trim().to_string()
                    }
                );
            }
            let (output_text, output_json, meta) = parse_gemini_output(&result.stdout)?;
            Ok((
                output_text,
                output_json,
                meta,
                result.stdout,
                result.stderr,
                result.returncode,
            ))
        }
        _ => bail!("Provider no soportado: {}", job.provider),
    }
}

fn dependency_state(conn: &Connection, job: &Job) -> Result<(bool, Vec<Job>, Vec<Job>)> {
    let deps = get_dependency_jobs(conn, &job.depends_on)?;
    let mut waiting = Vec::new();
    let mut failed = Vec::new();
    for dep in deps {
        if dep.status == "completed" {
            continue;
        }
        if matches!(dep.status.as_str(), "failed" | "cancelled") {
            failed.push(dep);
        } else {
            waiting.push(dep);
        }
    }
    Ok((waiting.is_empty() && failed.is_empty(), waiting, failed))
}

fn running_counts(conn: &Connection) -> Result<BTreeMap<String, i64>> {
    let mut counts = BTreeMap::new();
    counts.insert("global".to_string(), 0);
    counts.insert("claude".to_string(), 0);
    counts.insert("codex".to_string(), 0);
    counts.insert("gemini".to_string(), 0);

    let mut stmt = conn.prepare(
        "SELECT provider, COUNT(*) AS count FROM jobs WHERE status = 'running' GROUP BY provider",
    )?;
    let mut rows = stmt.query([])?;
    while let Some(row) = rows.next()? {
        let provider: String = row.get("provider")?;
        let count: i64 = row.get("count")?;
        counts.insert(provider.clone(), count);
        *counts.entry("global".to_string()).or_insert(0) += count;
    }
    Ok(counts)
}

fn claim_job(conn: &Connection, job_id: &str) -> Result<bool> {
    let now = utc_now();
    let affected = conn.execute(
        "UPDATE jobs SET status = 'running', started_at = COALESCE(started_at, ?), updated_at = ?, attempts = attempts + 1 WHERE id = ? AND status = 'pending'",
        params![now, utc_now(), job_id],
    )?;
    Ok(affected == 1)
}

fn fail_job_due_to_dependencies(conn: &Connection, job: &Job, failed_deps: &[Job]) -> Result<Job> {
    let message = format!(
        "Dependencias fallidas: {}",
        failed_deps
            .iter()
            .map(|dep| format!("{}({})", dep.id, dep.status))
            .collect::<Vec<_>>()
            .join(", ")
    );
    let updated = set_job_status(
        conn,
        &job.id,
        "failed",
        None,
        Some(&utc_now()),
        None,
        Some(&message),
        None,
        None,
        None,
        None,
        None,
        None,
        false,
    )?;
    let _ = add_thread_memory(
        conn,
        &job.thread_id,
        &message,
        "dependency_failure",
        Some(&job.id),
    );
    Ok(updated)
}

fn job_directory(job_id: &str) -> Result<PathBuf> {
    let path = jobs_dir()?.join(job_id);
    fs::create_dir_all(&path)?;
    Ok(path)
}

fn is_process_alive(system: &System, pid: i64) -> bool {
    if pid <= 0 {
        return false;
    }
    system.process(Pid::from_u32(pid as u32)).is_some()
}

fn is_timestamp_older_than(timestamp: Option<&str>, seconds: i64) -> bool {
    let Some(timestamp) = timestamp else {
        return false;
    };
    let Ok(parsed) = DateTime::parse_from_rfc3339(timestamp) else {
        return false;
    };
    let age = Utc::now().signed_duration_since(parsed.with_timezone(&Utc));
    age.num_seconds() >= seconds
}

fn recover_stale_running_jobs(conn: &Connection) -> Result<Vec<String>> {
    let mut system = System::new_all();
    system.refresh_processes(ProcessesToUpdate::All, true);

    let running_jobs = list_jobs(conn, Some("running"), None, 1000)?;
    let mut recovered = Vec::new();

    for job in running_jobs {
        let message = match job.worker_pid {
            Some(worker_pid) if is_process_alive(&system, worker_pid) => continue,
            Some(worker_pid) => format!(
                "Worker PID {} ya no está activo. Marco el job como stale/failed para liberar la cola.",
                worker_pid
            ),
            None if is_timestamp_older_than(
                job.started_at.as_deref().or(Some(job.updated_at.as_str())),
                STALE_RUNNING_WITHOUT_PID_GRACE_SECONDS,
            ) =>
            {
                format!(
                    "Job en `running` sin worker_pid por más de {}s. Marco el job como stale/failed para liberar la cola.",
                    STALE_RUNNING_WITHOUT_PID_GRACE_SECONDS
                )
            }
            None => continue,
        };
        let updated = set_job_status(
            conn,
            &job.id,
            "failed",
            None,
            Some(&utc_now()),
            None,
            Some(&message),
            None,
            None,
            None,
            None,
            None,
            None,
            false,
        )?;
        let _ = add_thread_memory(
            conn,
            &updated.thread_id,
            &message,
            "worker_stale",
            Some(&updated.id),
        );
        recovered.push(updated.id);
    }

    Ok(recovered)
}

fn spawn_worker(job_id: &str) -> Result<u32> {
    let exe = current_executable_path()?;
    let mut command = Command::new(exe);
    command.arg("run-job").arg("--job-id").arg(job_id);
    command
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    #[cfg(windows)]
    {
        command.creation_flags(CREATE_NO_WINDOW | DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP);
    }
    let child = command.spawn()?;
    Ok(child.id())
}

fn dispatch_once() -> Result<Value> {
    let config = load_config()?;
    let conn = open_connection()?;
    init_db(&conn)?;
    let recovered_stale = recover_stale_running_jobs(&conn)?;
    let mut started = Vec::new();
    let mut skipped_waiting = 0;
    let mut failed_from_dependencies = 0;

    enum DispatchSelection {
        Claimed { job_id: String },
        FailedDependency,
        None,
    }

    loop {
        let mut waiting_seen_this_pass = 0;
        let selection = {
            let tx = conn.unchecked_transaction()?;
            let counts = running_counts(&tx)?;
            if counts.get("global").copied().unwrap_or(0) >= config.concurrency.global {
                tx.commit()?;
                DispatchSelection::None
            } else {
                let pending_jobs = list_jobs(&tx, Some("pending"), None, 500)?;
                let mut selection = DispatchSelection::None;

                for job in pending_jobs {
                    let provider_limit = config
                        .concurrency
                        .providers
                        .get(&job.provider)
                        .copied()
                        .unwrap_or(config.concurrency.global);
                    if counts.get(&job.provider).copied().unwrap_or(0) >= provider_limit {
                        continue;
                    }

                    let (ready, waiting, failed) = dependency_state(&tx, &job)?;
                    if !failed.is_empty() {
                        let _ = fail_job_due_to_dependencies(&tx, &job, &failed)?;
                        selection = DispatchSelection::FailedDependency;
                        break;
                    }
                    if !ready {
                        let _ = waiting;
                        waiting_seen_this_pass += 1;
                        continue;
                    }
                    if !claim_job(&tx, &job.id)? {
                        continue;
                    }
                    selection = DispatchSelection::Claimed {
                        job_id: job.id.clone(),
                    };
                    break;
                }

                tx.commit()?;
                selection
            }
        };

        skipped_waiting += waiting_seen_this_pass;

        match selection {
            DispatchSelection::Claimed { job_id } => match spawn_worker(&job_id) {
                Ok(pid) => {
                    let _ = set_job_status(
                        &conn,
                        &job_id,
                        "running",
                        None,
                        None,
                        Some(pid as i64),
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        false,
                    )?;
                    started.push(job_id);
                }
                Err(err) => {
                    let _ = set_job_status(
                        &conn,
                        &job_id,
                        "failed",
                        None,
                        Some(&utc_now()),
                        None,
                        Some(&format!("No pude lanzar el worker: {err}")),
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        false,
                    )?;
                }
            },
            DispatchSelection::FailedDependency => {
                failed_from_dependencies += 1;
            }
            DispatchSelection::None => break,
        }
    }

    Ok(json!({
        "ok": true,
        "started_jobs": started,
        "skipped_waiting": skipped_waiting,
        "failed_from_dependencies": failed_from_dependencies,
        "recovered_stale_jobs": recovered_stale
    }))
}

fn wait_for_jobs(job_ids: &[String], timeout_seconds: Option<i64>) -> Result<Value> {
    if job_ids.is_empty() {
        bail!("`jobIds` debe contener al menos un id.");
    }
    let deadline =
        timeout_seconds.map(|seconds| Instant::now() + Duration::from_secs(seconds as u64));
    loop {
        let _ = dispatch_once()?;
        let conn = open_connection()?;
        init_db(&conn)?;
        let jobs: Vec<Job> = job_ids
            .iter()
            .map(|job_id| get_job(&conn, job_id))
            .collect::<Result<Vec<_>>>()?;
        let done = jobs
            .iter()
            .all(|job| matches!(job.status.as_str(), "completed" | "failed" | "cancelled"));
        if done {
            return Ok(json!({"ok": true, "timed_out": false, "jobs": jobs}));
        }
        if let Some(deadline) = deadline {
            if Instant::now() >= deadline {
                return Ok(json!({"ok": true, "timed_out": true, "jobs": jobs}));
            }
        }
        thread::sleep(Duration::from_secs(POLL_SECONDS));
    }
}

fn run_job(job_id: &str) -> Result<Value> {
    let conn = open_connection()?;
    init_db(&conn)?;
    let mut job = get_job(&conn, job_id)?;
    if job.status == "pending" {
        let _ = claim_job(&conn, job_id)?;
        job = get_job(&conn, job_id)?;
    }
    if !matches!(job.status.as_str(), "running" | "pending") {
        return Ok(json!({"ok": true, "job": job, "note": "Job ya estaba en estado terminal."}));
    }

    let prompt = build_job_prompt(&conn, &job)?;
    let work_dir = job_directory(job_id)?;
    let prompt_path = work_dir.join("prompt.txt");
    let stdout_path = work_dir.join("stdout.txt");
    let stderr_path = work_dir.join("stderr.txt");
    fs::write(&prompt_path, &prompt)?;
    let _ = set_job_status(
        &conn,
        job_id,
        "running",
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        Some(prompt_path.to_string_lossy().as_ref()),
        None,
        None,
        false,
    )?;

    match execute_provider_job(&job, &prompt) {
        Ok((output_text, output_json, mut meta, stdout, stderr, _)) => {
            let stdout_write = fs::write(&stdout_path, stdout);
            let stderr_write = fs::write(&stderr_path, stderr);
            let stdout_path_value = stdout_write
                .as_ref()
                .ok()
                .map(|_| stdout_path.to_string_lossy().to_string());
            let stderr_path_value = stderr_write
                .as_ref()
                .ok()
                .map(|_| stderr_path.to_string_lossy().to_string());
            let persistence_errors = [("stdout.txt", stdout_write), ("stderr.txt", stderr_write)]
                .into_iter()
                .filter_map(|(name, result)| {
                    result
                        .err()
                        .map(|err| format!("No pude escribir {name}: {err}"))
                })
                .collect::<Vec<_>>();

            if !persistence_errors.is_empty() {
                let persistence_error = persistence_errors.join("; ");
                if let Some(obj) = meta.as_object_mut() {
                    obj.insert(
                        "persistence_error".to_string(),
                        Value::String(persistence_error),
                    );
                } else {
                    meta = json!({
                        "provider_meta": meta,
                        "persistence_error": persistence_error
                    });
                }
            }

            let conn = open_connection()?;
            init_db(&conn)?;
            let updated = set_job_status(
                &conn,
                job_id,
                "completed",
                None,
                Some(&utc_now()),
                None,
                Some(""),
                Some(&output_text),
                Some(&output_json),
                Some(&meta),
                None,
                stdout_path_value.as_deref(),
                stderr_path_value.as_deref(),
                false,
            )?;
            let _ = add_thread_memory(
                &conn,
                &updated.thread_id,
                &summarize_job_for_memory(&updated),
                "job_result",
                Some(job_id),
            );
        }
        Err(err) => {
            let message = err.to_string();
            let _ = fs::write(&stdout_path, "");
            let _ = fs::write(&stderr_path, &message);
            let conn = open_connection()?;
            init_db(&conn)?;
            let updated = set_job_status(
                &conn,
                job_id,
                "failed",
                None,
                Some(&utc_now()),
                None,
                Some(&message),
                None,
                None,
                None,
                None,
                Some(stdout_path.to_string_lossy().as_ref()),
                Some(stderr_path.to_string_lossy().as_ref()),
                false,
            )?;
            let _ = add_thread_memory(
                &conn,
                &updated.thread_id,
                &summarize_job_for_memory(&updated),
                "job_error",
                Some(job_id),
            );
        }
    }
    let _ = dispatch_once()?;
    let conn = open_connection()?;
    init_db(&conn)?;
    Ok(json!({"ok": true, "job": get_job(&conn, job_id)?}))
}

fn cmd_list_agents() -> Result<Value> {
    let config = load_config()?;
    Ok(json!({
        "ok": true,
        "dynamic_provider_model_selection": true,
        "providers": ["claude", "codex", "gemini"],
        "presets": config.agents,
        "concurrency": config.concurrency,
        "defaults": config.defaults
    }))
}

fn cmd_submit(
    agent: Option<String>,
    provider: Option<String>,
    model: Option<String>,
    reasoning_effort: Option<String>,
    thread_id: Option<String>,
    parent_job_id: Option<String>,
    depends_on: Vec<String>,
    title: Option<String>,
    cwd: Option<String>,
    prompt: String,
    role: Option<String>,
    shared_context: Option<String>,
    priority: i64,
    wait: bool,
    timeout_seconds: Option<i64>,
    review_mode: bool,
) -> Result<Value> {
    let conn = open_connection()?;
    init_db(&conn)?;
    let default_agent = if provider.is_none() && agent.is_none() {
        Some("claude-sonnet".to_string())
    } else {
        agent
    };
    let job = create_job(
        &conn,
        default_agent.as_deref(),
        provider.as_deref(),
        model.as_deref(),
        reasoning_effort.as_deref(),
        thread_id.as_deref(),
        parent_job_id.as_deref(),
        &depends_on,
        title.as_deref(),
        cwd.as_deref(),
        &prompt,
        role.as_deref(),
        shared_context.as_deref(),
        priority,
        timeout_seconds,
        review_mode,
        None,
    )?;
    let dispatch = dispatch_once()?;
    if wait {
        let waited = wait_for_jobs(&[job.id.clone()], timeout_seconds)?;
        return Ok(json!({
            "ok": true,
            "job": waited["jobs"][0].clone(),
            "dispatch": dispatch,
            "timed_out": waited["timed_out"].clone()
        }));
    }
    Ok(json!({"ok": true, "job": job, "dispatch": dispatch}))
}

fn cmd_submit_supervised(
    title: Option<String>,
    cwd: Option<String>,
    task: String,
    primary_agent: Option<String>,
    primary_provider: Option<String>,
    primary_model: Option<String>,
    primary_reasoning_effort: Option<String>,
    reviewers_json: Option<String>,
    synthesis_json: Option<String>,
    shared_context: Option<String>,
    priority: i64,
    wait: bool,
    timeout_seconds: Option<i64>,
) -> Result<Value> {
    let reviewers: Vec<ReviewerInput> = reviewers_json
        .as_deref()
        .map(|raw| serde_json::from_str(raw))
        .transpose()?
        .unwrap_or_default();
    let synthesis: Option<SynthesisInput> = synthesis_json
        .as_deref()
        .map(|raw| serde_json::from_str(raw))
        .transpose()?;

    let conn = open_connection()?;
    init_db(&conn)?;
    let effective_primary_agent = if primary_agent.is_none() && primary_provider.is_none() {
        Some("claude-sonnet".to_string())
    } else {
        primary_agent
    };
    let primary = create_job(
        &conn,
        effective_primary_agent.as_deref(),
        primary_provider.as_deref(),
        primary_model.as_deref(),
        primary_reasoning_effort.as_deref(),
        None,
        None,
        &[],
        title.as_deref().or(Some("Primary task")),
        cwd.as_deref(),
        &task,
        Some("primary"),
        shared_context.as_deref(),
        priority,
        timeout_seconds,
        false,
        None,
    )?;
    let mut created_jobs = vec![primary.id.clone()];
    let mut reviewer_ids = Vec::new();
    for (index, reviewer) in reviewers.iter().enumerate() {
        let reviewer_title = reviewer
            .role
            .clone()
            .unwrap_or_else(|| format!("Reviewer {}", index + 1));
        let reviewer_role = reviewer
            .role
            .clone()
            .unwrap_or_else(|| "reviewer".to_string());
        let reviewer_job = create_job(
            &conn,
            reviewer.agent.as_deref(),
            reviewer.provider.as_deref(),
            reviewer.model.as_deref(),
            reviewer.reasoning_effort.as_deref().or(reviewer.reasoning_effort_camel.as_deref()),
            Some(&primary.thread_id),
            Some(&primary.id),
            std::slice::from_ref(&primary.id),
            Some(reviewer_title.as_str()),
            cwd.as_deref(),
            reviewer
                .prompt
                .as_deref()
                .unwrap_or("Review the primary output carefully. Focus on defects, risks, missing tests, edge cases, and concrete corrections."),
            Some(reviewer_role.as_str()),
            shared_context.as_deref(),
            priority,
            timeout_seconds,
            true,
            reviewer.max_dependency_chars,
        )?;
        reviewer_ids.push(reviewer_job.id.clone());
        created_jobs.push(reviewer_job.id);
    }

    let synthesis_job_id = if let Some(synthesis) = synthesis {
        let mut deps = vec![primary.id.clone()];
        deps.extend(reviewer_ids.clone());
        let synthesis_title = synthesis
            .role
            .clone()
            .unwrap_or_else(|| "Synthesis".to_string());
        let synthesis_role = synthesis
            .role
            .clone()
            .unwrap_or_else(|| "synthesizer".to_string());
        let synthesis_job = create_job(
            &conn,
            synthesis.agent.as_deref(),
            synthesis.provider.as_deref(),
            synthesis.model.as_deref(),
            synthesis.reasoning_effort.as_deref().or(synthesis.reasoning_effort_camel.as_deref()),
            Some(&primary.thread_id),
            Some(&primary.id),
            &deps,
            Some(synthesis_title.as_str()),
            cwd.as_deref(),
            synthesis
                .prompt
                .as_deref()
                .unwrap_or("Synthesize the primary output and all reviews into one final actionable recommendation. Reconcile disagreements explicitly."),
            Some(synthesis_role.as_str()),
            shared_context.as_deref(),
            priority,
            timeout_seconds,
            false,
            synthesis.max_dependency_chars,
        )?;
        created_jobs.push(synthesis_job.id.clone());
        Some(synthesis_job.id)
    } else {
        None
    };

    let dispatch = dispatch_once()?;
    if wait {
        let waited = wait_for_jobs(&created_jobs, timeout_seconds)?;
        return Ok(json!({
            "ok": true,
            "thread_id": primary.thread_id,
            "primary_job_id": primary.id,
            "reviewer_job_ids": reviewer_ids,
            "synthesis_job_id": synthesis_job_id,
            "jobs": waited["jobs"].clone(),
            "dispatch": dispatch,
            "timed_out": waited["timed_out"].clone()
        }));
    }
    Ok(json!({
        "ok": true,
        "thread_id": primary.thread_id,
        "primary_job_id": primary.id,
        "reviewer_job_ids": reviewer_ids,
        "synthesis_job_id": synthesis_job_id,
        "dispatch": dispatch
    }))
}

fn cmd_get_job(job_id: &str) -> Result<Value> {
    let conn = open_connection()?;
    init_db(&conn)?;
    let job = get_job(&conn, job_id)?;
    let dependencies = get_dependency_jobs(&conn, &job.depends_on)?;
    let mut stmt =
        conn.prepare("SELECT * FROM jobs WHERE parent_job_id = ? ORDER BY created_at ASC")?;
    let children = stmt
        .query_map([job_id], row_to_job)?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(json!({"ok": true, "job": job, "dependencies": dependencies, "children": children}))
}

fn cmd_list_jobs(status: Option<String>, thread_id: Option<String>, limit: i64) -> Result<Value> {
    let conn = open_connection()?;
    init_db(&conn)?;
    let jobs = list_jobs(&conn, status.as_deref(), thread_id.as_deref(), limit)?
        .into_iter()
        .map(job_summary)
        .collect::<Vec<_>>();
    Ok(json!({"ok": true, "jobs": jobs}))
}

fn cmd_add_memory(
    thread_id: String,
    kind: Option<String>,
    content: String,
    source_job_id: Option<String>,
) -> Result<Value> {
    let conn = open_connection()?;
    init_db(&conn)?;
    let memory = add_thread_memory(
        &conn,
        &thread_id,
        &content,
        kind.as_deref().unwrap_or("note"),
        source_job_id.as_deref(),
    )?;
    Ok(json!({"ok": true, "memory": memory}))
}

fn tool_defs() -> Vec<Value> {
    vec![
        json!({"name":"agenthub_list_agents","description":"Lista los presets de agentes disponibles. ?sala cuando el usuario pida qu? agentes/modelos puede abrir el hub.","inputSchema":{"type":"object","properties":{}}}),
        json!({"name":"agenthub_list_models","description":"Alias de list_agents. ?sala cuando el usuario pida modelos disponibles, reviewers posibles o qu? proveedor conviene usar.","inputSchema":{"type":"object","properties":{}}}),
        json!({"name":"agenthub_submit_job","description":"Lanza un agente extra en segundo plano. ?sala cuando el usuario pida abrir otro agente/modelo, delegar una subtarea, pedir una segunda opini?n o hacer una revisi?n con otro proveedor.","inputSchema":{"type":"object","properties":{
            "agent":{"type":"string"},"provider":{"type":"string"},"model":{"type":"string"},"reasoningEffort":{"type":"string"},
            "threadId":{"type":"string"},"parentJobId":{"type":"string"},"dependsOn":{"type":"array","items":{"type":"string"}},
            "title":{"type":"string"},"cwd":{"type":"string"},"prompt":{"type":"string"},"role":{"type":"string"},
            "sharedContext":{"type":"string"},"priority":{"type":"integer"},"wait":{"type":"boolean"},
            "timeoutSeconds":{"type":"integer"},"reviewMode":{"type":"boolean"}},"required":["prompt"]}}),
        json!({"name":"agenthub_delegate_to_agent","description":"Alias de submit_job. ?sala cuando el usuario diga 'abre otro agente', 'p?saselo a otro modelo', 'que otro agente lo revise' o pida una segunda opini?n.","inputSchema":{"type":"object","properties":{
            "agent":{"type":"string"},"provider":{"type":"string"},"model":{"type":"string"},"reasoningEffort":{"type":"string"},
            "threadId":{"type":"string"},"parentJobId":{"type":"string"},"dependsOn":{"type":"array","items":{"type":"string"}},
            "title":{"type":"string"},"cwd":{"type":"string"},"prompt":{"type":"string"},"role":{"type":"string"},
            "sharedContext":{"type":"string"},"priority":{"type":"integer"},"wait":{"type":"boolean"},
            "timeoutSeconds":{"type":"integer"},"reviewMode":{"type":"boolean"}},"required":["prompt"]}}),
        json!({"name":"agenthub_submit_supervised_task","description":"Crea un flujo multiagente: agente principal + revisores en paralelo + s?ntesis opcional. ?sala cuando el usuario quiera supervisor/reviewer agents, comparar modelos, o hacer trabajo paralelo con validaci?n cruzada.","inputSchema":{"type":"object","properties":{
            "title":{"type":"string"},"cwd":{"type":"string"},"task":{"type":"string"},"primaryAgent":{"type":"string"},
            "primaryProvider":{"type":"string"},"primaryModel":{"type":"string"},"primaryReasoningEffort":{"type":"string"},
            "reviewers":{"type":"array","items":{"type":"object"}},"synthesis":{"type":"object"},"sharedContext":{"type":"string"},
            "priority":{"type":"integer"},"wait":{"type":"boolean"},"timeoutSeconds":{"type":"integer"}},"required":["task"]}}),
        json!({"name":"agenthub_run_supervisor_team","description":"Alias de submit_supervised_task. ?sala cuando el usuario diga 'hazlo y luego que otros agentes lo supervisen/revisen', o quiera un agente principal + reviewers + s?ntesis.","inputSchema":{"type":"object","properties":{
            "title":{"type":"string"},"cwd":{"type":"string"},"task":{"type":"string"},"primaryAgent":{"type":"string"},
            "primaryProvider":{"type":"string"},"primaryModel":{"type":"string"},"primaryReasoningEffort":{"type":"string"},
            "reviewers":{"type":"array","items":{"type":"object"}},"synthesis":{"type":"object"},"sharedContext":{"type":"string"},
            "priority":{"type":"integer"},"wait":{"type":"boolean"},"timeoutSeconds":{"type":"integer"}},"required":["task"]}}),
        json!({"name":"agenthub_get_job","description":"Devuelve el estado detallado y resultados de un job.","inputSchema":{"type":"object","properties":{"jobId":{"type":"string"}},"required":["jobId"]}}),
        json!({"name":"agenthub_list_jobs","description":"Lista jobs de la cola.","inputSchema":{"type":"object","properties":{"status":{"type":"string"},"threadId":{"type":"string"},"limit":{"type":"integer"}}}}),
        json!({"name":"agenthub_wait_job","description":"Espera hasta que uno o m?s jobs terminen.","inputSchema":{"type":"object","properties":{"jobIds":{"type":"array","items":{"type":"string"}},"timeoutSeconds":{"type":"integer"}},"required":["jobIds"]}}),
        json!({"name":"agenthub_get_thread","description":"Devuelve un thread con jobs y memorias.","inputSchema":{"type":"object","properties":{"threadId":{"type":"string"}},"required":["threadId"]}}),
        json!({"name":"agenthub_add_thread_memory","description":"A?ade memoria compartida a un thread.","inputSchema":{"type":"object","properties":{"threadId":{"type":"string"},"kind":{"type":"string"},"content":{"type":"string"},"sourceJobId":{"type":"string"}},"required":["threadId","content"]}}),
        json!({"name":"agenthub_dispatch","description":"Fuerza una pasada del despachador.","inputSchema":{"type":"object","properties":{}}}),
    ]
}

fn tool_text_result(value: Value, is_error: bool) -> Value {
    json!({
        "content": [{"type": "text", "text": json_pretty(&value)}],
        "isError": is_error
    })
}

fn handle_tool_call(name: &str, arguments: Value) -> Value {
    let result = match name {
        "agenthub_list_agents" | "agenthub_list_models" => cmd_list_agents(),
        "agenthub_submit_job" | "agenthub_delegate_to_agent" => cmd_submit(
            arguments
                .get("agent")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            arguments
                .get("provider")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            arguments
                .get("model")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            arguments
                .get("reasoningEffort")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            arguments
                .get("threadId")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            arguments
                .get("parentJobId")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            arguments
                .get("dependsOn")
                .and_then(|v| v.as_array())
                .map(|items| {
                    items
                        .iter()
                        .filter_map(|v| v.as_str().map(|s| s.to_string()))
                        .collect()
                })
                .unwrap_or_default(),
            arguments
                .get("title")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            arguments
                .get("cwd")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            arguments
                .get("prompt")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string(),
            arguments
                .get("role")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            arguments
                .get("sharedContext")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            arguments
                .get("priority")
                .and_then(|v| v.as_i64())
                .unwrap_or(0),
            arguments
                .get("wait")
                .and_then(|v| v.as_bool())
                .unwrap_or(false),
            arguments.get("timeoutSeconds").and_then(|v| v.as_i64()),
            arguments
                .get("reviewMode")
                .and_then(|v| v.as_bool())
                .unwrap_or(false),
        ),
        "agenthub_submit_supervised_task" | "agenthub_run_supervisor_team" => {
            cmd_submit_supervised(
                arguments
                    .get("title")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string()),
                arguments
                    .get("cwd")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string()),
                arguments
                    .get("task")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default()
                    .to_string(),
                arguments
                    .get("primaryAgent")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string()),
                arguments
                    .get("primaryProvider")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string()),
                arguments
                    .get("primaryModel")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string()),
                arguments
                    .get("primaryReasoningEffort")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string()),
                arguments
                    .get("reviewers")
                    .map(|v| serde_json::to_string(v).unwrap()),
                arguments
                    .get("synthesis")
                    .map(|v| serde_json::to_string(v).unwrap()),
                arguments
                    .get("sharedContext")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string()),
                arguments
                    .get("priority")
                    .and_then(|v| v.as_i64())
                    .unwrap_or(0),
                arguments
                    .get("wait")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false),
                arguments.get("timeoutSeconds").and_then(|v| v.as_i64()),
            )
        }
        "agenthub_get_job" => cmd_get_job(
            arguments
                .get("jobId")
                .and_then(|v| v.as_str())
                .unwrap_or_default(),
        ),
        "agenthub_list_jobs" => cmd_list_jobs(
            arguments
                .get("status")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            arguments
                .get("threadId")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            arguments
                .get("limit")
                .and_then(|v| v.as_i64())
                .unwrap_or(50),
        ),
        "agenthub_wait_job" => {
            let job_ids = arguments
                .get("jobIds")
                .and_then(|v| v.as_array())
                .map(|items| {
                    items
                        .iter()
                        .filter_map(|v| v.as_str().map(|s| s.to_string()))
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            wait_for_jobs(
                &job_ids,
                arguments.get("timeoutSeconds").and_then(|v| v.as_i64()),
            )
        }
        "agenthub_get_thread" => {
            let conn = open_connection().and_then(|conn| {
                init_db(&conn)?;
                get_thread(
                    &conn,
                    arguments
                        .get("threadId")
                        .and_then(|v| v.as_str())
                        .unwrap_or_default(),
                )
            });
            conn
        }
        "agenthub_add_thread_memory" => cmd_add_memory(
            arguments
                .get("threadId")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string(),
            arguments
                .get("kind")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            arguments
                .get("content")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string(),
            arguments
                .get("sourceJobId")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
        ),
        "agenthub_dispatch" => dispatch_once(),
        _ => Err(anyhow!("Tool desconocida: {name}")),
    };

    match result {
        Ok(value) => tool_text_result(value, false),
        Err(err) => tool_text_result(json!({"ok": false, "error": err.to_string()}), true),
    }
}

#[derive(Clone, Copy)]
enum McpFraming {
    Ndjson,
    ContentLength,
}

struct McpReadError {
    framing: McpFraming,
    code: i64,
    message: String,
}

enum McpIncoming {
    Message(Value, McpFraming),
    RecoverableError(McpReadError),
}

fn write_mcp_message(message: &Value, framing: McpFraming) -> Result<()> {
    let payload = message.to_string();
    let mut stdout = io::stdout().lock();
    match framing {
        McpFraming::Ndjson => {
            writeln!(stdout, "{payload}")?;
        }
        McpFraming::ContentLength => {
            write!(
                stdout,
                "Content-Length: {}\r\n\r\n{}",
                payload.as_bytes().len(),
                payload
            )?;
        }
    }
    stdout.flush()?;
    Ok(())
}

fn send_jsonrpc_response(id: Value, result: Value, framing: McpFraming) -> Result<()> {
    let response = json!({"jsonrpc": "2.0", "id": id, "result": result});
    write_mcp_message(&response, framing)
}

fn send_jsonrpc_error(id: Value, code: i64, message: &str, framing: McpFraming) -> Result<()> {
    let response = json!({"jsonrpc":"2.0","id":id,"error":{"code":code,"message":message}});
    write_mcp_message(&response, framing)
}

fn read_mcp_message(reader: &mut BufReader<io::StdinLock<'_>>) -> Result<Option<McpIncoming>> {
    loop {
        let mut line = String::new();
        let bytes = reader.read_line(&mut line)?;
        if bytes == 0 {
            return Ok(None);
        }
        let trimmed = line.trim_end_matches(['\r', '\n']);
        if trimmed.is_empty() {
            continue;
        }
        if trimmed.starts_with('{') || trimmed.starts_with('[') {
            return Ok(Some(match serde_json::from_str(trimmed) {
                Ok(message) => McpIncoming::Message(message, McpFraming::Ndjson),
                Err(err) => McpIncoming::RecoverableError(McpReadError {
                    framing: McpFraming::Ndjson,
                    code: -32700,
                    message: format!("Parse error: {err}"),
                }),
            }));
        }

        let mut content_length: Option<usize> = None;
        if let Some((name, value)) = trimmed.split_once(':') {
            if name.eq_ignore_ascii_case("Content-Length") {
                match value.trim().parse() {
                    Ok(parsed) => content_length = Some(parsed),
                    Err(err) => {
                        return Ok(Some(McpIncoming::RecoverableError(McpReadError {
                            framing: McpFraming::ContentLength,
                            code: -32600,
                            message: format!("Invalid Content-Length: {err}"),
                        })));
                    }
                }
            }
        } else {
            return Ok(Some(McpIncoming::RecoverableError(McpReadError {
                framing: McpFraming::Ndjson,
                code: -32600,
                message: "Invalid request framing.".to_string(),
            })));
        }

        loop {
            line.clear();
            let header_bytes = reader.read_line(&mut line)?;
            if header_bytes == 0 {
                return Ok(Some(McpIncoming::RecoverableError(McpReadError {
                    framing: McpFraming::ContentLength,
                    code: -32600,
                    message: "EOF while reading MCP headers.".to_string(),
                })));
            }
            let header = line.trim_end_matches(['\r', '\n']);
            if header.is_empty() {
                break;
            }
            if let Some((name, value)) = header.split_once(':') {
                if name.eq_ignore_ascii_case("Content-Length") {
                    match value.trim().parse() {
                        Ok(parsed) => content_length = Some(parsed),
                        Err(err) => {
                            return Ok(Some(McpIncoming::RecoverableError(McpReadError {
                                framing: McpFraming::ContentLength,
                                code: -32600,
                                message: format!("Invalid Content-Length: {err}"),
                            })));
                        }
                    }
                }
            }
        }

        let Some(content_length) = content_length else {
            return Ok(Some(McpIncoming::RecoverableError(McpReadError {
                framing: McpFraming::ContentLength,
                code: -32600,
                message: "Falta Content-Length.".to_string(),
            })));
        };
        let mut buf = vec![0u8; content_length];
        reader.read_exact(&mut buf)?;
        return Ok(Some(match serde_json::from_slice(&buf) {
            Ok(message) => McpIncoming::Message(message, McpFraming::ContentLength),
            Err(err) => McpIncoming::RecoverableError(McpReadError {
                framing: McpFraming::ContentLength,
                code: -32700,
                message: format!("Parse error: {err}"),
            }),
        }));
    }
}

fn run_mcp_server() -> Result<()> {
    let stdin = io::stdin();
    let mut reader = BufReader::new(stdin.lock());
    loop {
        let incoming = match read_mcp_message(&mut reader) {
            Ok(Some(incoming)) => incoming,
            Ok(None) => break,
            Err(err) => {
                let _ = send_jsonrpc_error(
                    Value::Null,
                    -32603,
                    &format!("Internal error reading MCP input: {err}"),
                    McpFraming::Ndjson,
                );
                break;
            }
        };
        let (message, framing) = match incoming {
            McpIncoming::Message(message, framing) => (message, framing),
            McpIncoming::RecoverableError(error) => {
                let _ = send_jsonrpc_error(Value::Null, error.code, &error.message, error.framing);
                continue;
            }
        };
        let id = message.get("id").cloned();
        let jsonrpc = message
            .get("jsonrpc")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if jsonrpc != "2.0" {
            send_jsonrpc_error(
                id.unwrap_or(Value::Null),
                -32600,
                "Invalid Request: `jsonrpc` must be `2.0`.",
                framing,
            )?;
            continue;
        }
        let method = message.get("method").and_then(|v| v.as_str()).unwrap_or("");
        if method.is_empty() {
            send_jsonrpc_error(
                id.unwrap_or(Value::Null),
                -32600,
                "Invalid Request: missing `method`.",
                framing,
            )?;
            continue;
        }
        match method {
            "initialize" => {
                let requested = message
                    .pointer("/params/protocolVersion")
                    .and_then(|v| v.as_str())
                    .unwrap_or(DEFAULT_NEGOTIATED_PROTOCOL_VERSION);
                let negotiated = if SUPPORTED_PROTOCOL_VERSIONS.contains(&requested) {
                    requested
                } else {
                    DEFAULT_NEGOTIATED_PROTOCOL_VERSION
                };
                if let Some(id) = id {
                    send_jsonrpc_response(
                        id,
                        json!({
                            "protocolVersion": negotiated,
                            "capabilities": {"tools": {"listChanged": false}},
                            "serverInfo": {"name": SERVER_NAME, "version": SERVER_VERSION},
                            "instructions": "Local multi-agent delegation hub for Claude, Codex and Gemini. Use this MCP whenever the user asks to open another agent/model, get a second opinion, delegate a subtask, compare providers, parallelize work, or have other agents review/supervise the result. Use `agenthub_delegate_to_agent` / `agenthub_submit_job` for one extra agent and `agenthub_run_supervisor_team` / `agenthub_submit_supervised_task` for a primary agent plus reviewers."
                        }),
                        framing,
                    )?;
                }
            }
            "notifications/initialized" => {}
            "ping" => {
                if let Some(id) = id {
                    send_jsonrpc_response(id, json!({}), framing)?;
                }
            }
            "tools/list" => {
                if let Some(id) = id {
                    send_jsonrpc_response(id, json!({"tools": tool_defs()}), framing)?;
                }
            }
            "tools/call" => {
                let params = message.get("params").cloned().unwrap_or_else(|| json!({}));
                let name = params.get("name").and_then(|v| v.as_str()).unwrap_or("");
                let arguments = params
                    .get("arguments")
                    .cloned()
                    .unwrap_or_else(|| json!({}));
                if let Some(id) = id {
                    send_jsonrpc_response(id, handle_tool_call(name, arguments), framing)?;
                }
            }
            _ => {
                if let Some(id) = id {
                    send_jsonrpc_error(
                        id,
                        -32601,
                        &format!("Method not found: {method}"),
                        framing,
                    )?;
                }
            }
        }
    }
    Ok(())
}

fn execute_cli(command: Commands) -> Result<Value> {
    match command {
        Commands::Serve => {
            run_mcp_server()?;
            Ok(json!({"ok": true}))
        }
        Commands::Init => init_storage(),
        Commands::ListAgents => cmd_list_agents(),
        Commands::Submit {
            agent,
            provider,
            model,
            reasoning_effort,
            thread_id,
            parent_job_id,
            depends_on,
            title,
            cwd,
            prompt,
            role,
            shared_context,
            priority,
            wait,
            timeout_seconds,
            review_mode,
        } => cmd_submit(
            agent,
            provider,
            model,
            reasoning_effort,
            thread_id,
            parent_job_id,
            depends_on,
            title,
            cwd,
            prompt,
            role,
            shared_context,
            priority,
            wait,
            timeout_seconds,
            review_mode,
        ),
        Commands::SubmitSupervised {
            title,
            cwd,
            task,
            primary_agent,
            primary_provider,
            primary_model,
            primary_reasoning_effort,
            reviewers_json,
            synthesis_json,
            shared_context,
            priority,
            wait,
            timeout_seconds,
        } => cmd_submit_supervised(
            title,
            cwd,
            task,
            primary_agent,
            primary_provider,
            primary_model,
            primary_reasoning_effort,
            reviewers_json,
            synthesis_json,
            shared_context,
            priority,
            wait,
            timeout_seconds,
        ),
        Commands::Dispatch => dispatch_once(),
        Commands::RunJob { job_id } => run_job(&job_id),
        Commands::GetJob { job_id } => cmd_get_job(&job_id),
        Commands::ListJobs {
            status,
            thread_id,
            limit,
        } => cmd_list_jobs(status, thread_id, limit),
        Commands::WaitJob {
            job_ids,
            timeout_seconds,
        } => wait_for_jobs(&job_ids, timeout_seconds),
        Commands::GetThread { thread_id } => {
            let conn = open_connection()?;
            init_db(&conn)?;
            let mut value = get_thread(&conn, &thread_id)?;
            if let Some(obj) = value.as_object_mut() {
                obj.insert("ok".to_string(), Value::Bool(true));
            }
            Ok(value)
        }
        Commands::AddMemory {
            thread_id,
            kind,
            content,
            source_job_id,
        } => cmd_add_memory(thread_id, kind, content, source_job_id),
    }
}

fn main() -> Result<()> {
    if env::args().len() == 1 {
        return run_mcp_server();
    }
    let cli = Cli::parse();
    if let Some(command) = cli.command {
        let result = execute_cli(command)?;
        if !matches!(result.get("ok").and_then(|v| v.as_bool()), Some(true)) {
            emit_json(&result)?;
            return Ok(());
        }
        emit_json(&result)?;
        return Ok(());
    }
    run_mcp_server()
}
