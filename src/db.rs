use std::path::Path;

use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};
use rusqlite::{Connection, OptionalExtension, params};

use crate::models::{
    AgentRole, AgentSessionState, AgentSummary, CodexSessionStatus, DecisionSummary, SessionMode,
    SessionSummary, StreamEventRecord, TaskState, TaskSummary,
};

pub struct Database {
    conn: Connection,
}

impl Database {
    pub fn open(path: &Path) -> Result<Self> {
        let conn = Connection::open(path)
            .with_context(|| format!("failed to open sqlite database {}", path.display()))?;
        conn.execute_batch("PRAGMA foreign_keys = ON; PRAGMA journal_mode = WAL;")
            .context("failed to configure sqlite pragmas")?;

        let db = Self { conn };
        db.init_schema()?;
        Ok(db)
    }

    fn init_schema(&self) -> Result<()> {
        self.conn.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS agents (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL UNIQUE,
                role TEXT NOT NULL,
                repo_name TEXT,
                cwd TEXT NOT NULL,
                thread_id TEXT,
                current_session_id TEXT,
                state TEXT NOT NULL,
                current_task_id TEXT,
                last_output_at TEXT,
                last_heartbeat_at TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS sessions (
                id TEXT PRIMARY KEY,
                agent_id TEXT NOT NULL,
                session_mode TEXT NOT NULL,
                pid INTEGER,
                thread_id TEXT,
                status TEXT NOT NULL,
                started_at TEXT NOT NULL,
                ended_at TEXT,
                last_output_at TEXT
            );

            CREATE TABLE IF NOT EXISTS tasks (
                id TEXT PRIMARY KEY,
                from_agent TEXT NOT NULL,
                to_agent TEXT NOT NULL,
                title TEXT NOT NULL,
                summary TEXT NOT NULL,
                auto_resolve_by TEXT,
                auto_resolve_summary TEXT,
                state TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                closed_at TEXT
            );

            CREATE TABLE IF NOT EXISTS task_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_id TEXT NOT NULL,
                event_type TEXT NOT NULL,
                from_state TEXT,
                to_state TEXT,
                payload_json TEXT,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS decisions (
                id TEXT PRIMARY KEY,
                task_id TEXT NOT NULL,
                issued_by_agent_id TEXT NOT NULL,
                target_agent_id TEXT NOT NULL,
                summary TEXT NOT NULL,
                payload_json TEXT,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL,
                acknowledged_at TEXT
            );

            CREATE TABLE IF NOT EXISTS stream_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT,
                agent_name TEXT NOT NULL,
                stream_type TEXT NOT NULL,
                content TEXT NOT NULL,
                created_at TEXT NOT NULL
            );
            "#,
        )?;
        self.ensure_column("agents", "thread_id", "TEXT")?;
        self.ensure_column("agents", "current_session_id", "TEXT")?;
        self.ensure_column("tasks", "auto_resolve_by", "TEXT")?;
        self.ensure_column("tasks", "auto_resolve_summary", "TEXT")?;
        self.ensure_column("sessions", "session_mode", "TEXT NOT NULL DEFAULT 'round'")?;
        self.ensure_column("sessions", "thread_id", "TEXT")?;
        Ok(())
    }

    fn ensure_column(&self, table: &str, column: &str, definition: &str) -> Result<()> {
        let mut stmt = self
            .conn
            .prepare(&format!("PRAGMA table_info({table})"))
            .with_context(|| format!("failed to inspect table {table}"))?;

        let rows = stmt.query_map([], |row| row.get::<_, String>(1))?;
        let mut exists = false;
        for row in rows {
            if row? == column {
                exists = true;
                break;
            }
        }

        if !exists {
            self.conn
                .execute(
                    &format!("ALTER TABLE {table} ADD COLUMN {column} {definition}"),
                    [],
                )
                .with_context(|| format!("failed to add column {column} to table {table}"))?;
        }

        Ok(())
    }

    pub fn upsert_agent(&self, agent: &AgentSummary) -> Result<()> {
        let now = Utc::now().to_rfc3339();
        self.conn.execute(
            r#"
            INSERT INTO agents (
                id, name, role, repo_name, cwd, thread_id, current_session_id, state, current_task_id, last_output_at, last_heartbeat_at, created_at, updated_at
            ) VALUES (
                ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13
            )
            ON CONFLICT(name) DO UPDATE SET
                role = excluded.role,
                repo_name = excluded.repo_name,
                cwd = excluded.cwd,
                thread_id = excluded.thread_id,
                current_session_id = excluded.current_session_id,
                state = excluded.state,
                current_task_id = excluded.current_task_id,
                last_output_at = excluded.last_output_at,
                last_heartbeat_at = excluded.last_heartbeat_at,
                updated_at = excluded.updated_at
            "#,
            params![
                agent.name,
                agent.name,
                serialize_role(&agent.role),
                agent.repo_name,
                agent.cwd,
                agent.thread_id,
                agent.current_session_id,
                serialize_agent_state(&agent.state),
                agent.current_task_id,
                agent.last_output_at.map(|dt| dt.to_rfc3339()),
                agent.last_heartbeat_at.map(|dt| dt.to_rfc3339()),
                now,
                now,
            ],
        )?;
        Ok(())
    }

    pub fn insert_task(&self, task: &TaskSummary) -> Result<()> {
        self.conn.execute(
            r#"
            INSERT INTO tasks (
                id, from_agent, to_agent, title, summary, auto_resolve_by, auto_resolve_summary, state, created_at, updated_at, closed_at
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
            "#,
            params![
                task.task_id,
                task.from_agent,
                task.to_agent,
                task.title,
                task.summary,
                task.auto_resolve_by,
                task.auto_resolve_summary,
                serialize_task_state(&task.state),
                task.created_at.to_rfc3339(),
                task.updated_at.to_rfc3339(),
                task.closed_at.map(|dt| dt.to_rfc3339()),
            ],
        )?;
        Ok(())
    }

    pub fn update_task(&self, task: &TaskSummary) -> Result<()> {
        self.conn.execute(
            r#"
            UPDATE tasks
            SET state = ?2, updated_at = ?3, closed_at = ?4
            WHERE id = ?1
            "#,
            params![
                task.task_id,
                serialize_task_state(&task.state),
                task.updated_at.to_rfc3339(),
                task.closed_at.map(|dt| dt.to_rfc3339()),
            ],
        )?;
        Ok(())
    }

    pub fn insert_task_event(
        &self,
        task_id: &str,
        event_type: &str,
        from_state: Option<TaskState>,
        to_state: Option<TaskState>,
        payload_json: &str,
    ) -> Result<()> {
        self.conn.execute(
            r#"
            INSERT INTO task_events (task_id, event_type, from_state, to_state, payload_json, created_at)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6)
            "#,
            params![
                task_id,
                event_type,
                from_state.as_ref().map(serialize_task_state),
                to_state.as_ref().map(serialize_task_state),
                payload_json,
                Utc::now().to_rfc3339(),
            ],
        )?;
        Ok(())
    }

    pub fn insert_decision(&self, decision: &DecisionSummary, payload_json: &str) -> Result<()> {
        self.conn.execute(
            r#"
            INSERT INTO decisions (
                id, task_id, issued_by_agent_id, target_agent_id, summary, payload_json, status, created_at, acknowledged_at
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
            "#,
            params![
                decision.decision_id,
                decision.task_id,
                decision.issued_by,
                decision.target_agent,
                decision.summary,
                payload_json,
                decision.status,
                decision.created_at.to_rfc3339(),
                decision.acknowledged_at.map(|dt| dt.to_rfc3339()),
            ],
        )?;
        Ok(())
    }

    pub fn upsert_session(&self, session: &SessionSummary) -> Result<()> {
        self.conn.execute(
            r#"
            INSERT INTO sessions (
                id, agent_id, session_mode, pid, thread_id, status, started_at, ended_at, last_output_at
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
            ON CONFLICT(id) DO UPDATE SET
                agent_id = excluded.agent_id,
                session_mode = excluded.session_mode,
                pid = excluded.pid,
                thread_id = excluded.thread_id,
                status = excluded.status,
                started_at = excluded.started_at,
                ended_at = excluded.ended_at,
                last_output_at = excluded.last_output_at
            "#,
            params![
                session.session_id,
                session.agent_name,
                serialize_session_mode(&session.session_mode),
                session.pid.map(i64::from),
                session.thread_id,
                serialize_codex_session_status(&session.status),
                session.started_at.to_rfc3339(),
                session.ended_at.map(|dt| dt.to_rfc3339()),
                session.last_output_at.map(|dt| dt.to_rfc3339()),
            ],
        )?;
        Ok(())
    }

    pub fn update_decision_acknowledged(&self, decision: &DecisionSummary) -> Result<()> {
        let changed = self.conn.execute(
            r#"
            UPDATE decisions
            SET status = ?2, acknowledged_at = ?3
            WHERE id = ?1
            "#,
            params![
                decision.decision_id,
                decision.status,
                decision.acknowledged_at.map(|dt| dt.to_rfc3339()),
            ],
        )?;

        if changed == 0 {
            bail!("decision not found: {}", decision.decision_id);
        }

        Ok(())
    }

    pub fn append_stream_event(
        &self,
        session_id: Option<&str>,
        agent_name: &str,
        stream_type: &str,
        content: &str,
    ) -> Result<()> {
        self.conn.execute(
            r#"
            INSERT INTO stream_events (session_id, agent_name, stream_type, content, created_at)
            VALUES (?1, ?2, ?3, ?4, ?5)
            "#,
            params![
                session_id,
                agent_name,
                stream_type,
                content,
                Utc::now().to_rfc3339()
            ],
        )?;
        Ok(())
    }

    pub fn load_agents(&self) -> Result<Vec<AgentSummary>> {
        let mut stmt = self.conn.prepare(
            r#"
            SELECT name, role, repo_name, cwd, thread_id, current_session_id, state, current_task_id, last_output_at, last_heartbeat_at
            FROM agents
            ORDER BY name ASC
            "#,
        )?;

        let rows = stmt.query_map([], |row| {
            Ok(AgentSummary {
                name: row.get(0)?,
                role: parse_role(row.get::<_, String>(1)?),
                repo_name: row.get(2)?,
                cwd: row.get(3)?,
                thread_id: row.get(4)?,
                current_session_id: row.get(5)?,
                state: parse_agent_state(row.get::<_, String>(6)?),
                current_task_id: row.get(7)?,
                last_output_at: parse_datetime(row.get(8)?),
                last_heartbeat_at: parse_datetime(row.get(9)?),
            })
        })?;

        let mut agents = Vec::new();
        for row in rows {
            agents.push(row?);
        }
        Ok(agents)
    }

    pub fn load_recent_sessions(&self, limit: usize) -> Result<Vec<SessionSummary>> {
        let mut stmt = self.conn.prepare(
            r#"
            SELECT id, agent_id, session_mode, pid, thread_id, status, started_at, ended_at, last_output_at
            FROM sessions
            ORDER BY started_at DESC
            LIMIT ?1
            "#,
        )?;

        let rows = stmt.query_map(params![limit as i64], |row| {
            let pid: Option<i64> = row.get(3)?;
            Ok(SessionSummary {
                session_id: row.get(0)?,
                agent_name: row.get(1)?,
                session_mode: parse_session_mode(row.get::<_, String>(2)?),
                pid: pid.and_then(|value| u32::try_from(value).ok()),
                thread_id: row.get(4)?,
                status: parse_codex_session_status(row.get::<_, String>(5)?),
                started_at: parse_required_datetime(row.get(6)?)?,
                ended_at: parse_datetime(row.get(7)?),
                last_output_at: parse_datetime(row.get(8)?),
            })
        })?;

        let mut sessions = Vec::new();
        for row in rows {
            sessions.push(row?);
        }
        sessions.reverse();
        Ok(sessions)
    }

    pub fn load_running_sessions(&self) -> Result<Vec<SessionSummary>> {
        let mut stmt = self.conn.prepare(
            r#"
            SELECT id, agent_id, session_mode, pid, thread_id, status, started_at, ended_at, last_output_at
            FROM sessions
            WHERE status = 'running'
            ORDER BY started_at ASC
            "#,
        )?;

        let rows = stmt.query_map([], |row| {
            let pid: Option<i64> = row.get(3)?;
            Ok(SessionSummary {
                session_id: row.get(0)?,
                agent_name: row.get(1)?,
                session_mode: parse_session_mode(row.get::<_, String>(2)?),
                pid: pid.and_then(|value| u32::try_from(value).ok()),
                thread_id: row.get(4)?,
                status: parse_codex_session_status(row.get::<_, String>(5)?),
                started_at: parse_required_datetime(row.get(6)?)?,
                ended_at: parse_datetime(row.get(7)?),
                last_output_at: parse_datetime(row.get(8)?),
            })
        })?;

        let mut sessions = Vec::new();
        for row in rows {
            sessions.push(row?);
        }
        Ok(sessions)
    }

    pub fn load_tasks(&self) -> Result<Vec<TaskSummary>> {
        let mut stmt = self.conn.prepare(
            r#"
            SELECT id, from_agent, to_agent, title, summary, auto_resolve_by, auto_resolve_summary, state, created_at, updated_at, closed_at
            FROM tasks
            ORDER BY created_at ASC
            "#,
        )?;

        let rows = stmt.query_map([], |row| {
            Ok(TaskSummary {
                task_id: row.get(0)?,
                from_agent: row.get(1)?,
                to_agent: row.get(2)?,
                title: row.get(3)?,
                summary: row.get(4)?,
                auto_resolve_by: row.get(5)?,
                auto_resolve_summary: row.get(6)?,
                state: parse_task_state(row.get::<_, String>(7)?),
                created_at: parse_required_datetime(row.get(8)?)?,
                updated_at: parse_required_datetime(row.get(9)?)?,
                closed_at: parse_datetime(row.get(10)?),
            })
        })?;

        let mut tasks = Vec::new();
        for row in rows {
            tasks.push(row?);
        }
        Ok(tasks)
    }

    pub fn load_decisions(&self) -> Result<Vec<DecisionSummary>> {
        let mut stmt = self.conn.prepare(
            r#"
            SELECT id, task_id, issued_by_agent_id, target_agent_id, summary, status, created_at, acknowledged_at
            FROM decisions
            ORDER BY created_at ASC
            "#,
        )?;

        let rows = stmt.query_map([], |row| {
            Ok(DecisionSummary {
                decision_id: row.get(0)?,
                task_id: row.get(1)?,
                issued_by: row.get(2)?,
                target_agent: row.get(3)?,
                summary: row.get(4)?,
                status: row.get(5)?,
                created_at: parse_required_datetime(row.get(6)?)?,
                acknowledged_at: parse_datetime(row.get(7)?),
            })
        })?;

        let mut decisions = Vec::new();
        for row in rows {
            decisions.push(row?);
        }
        Ok(decisions)
    }

    pub fn load_recent_stream_events(&self, limit: usize) -> Result<Vec<StreamEventRecord>> {
        let mut stmt = self.conn.prepare(
            r#"
            SELECT session_id, agent_name, stream_type, content, created_at
            FROM stream_events
            ORDER BY id DESC
            LIMIT ?1
            "#,
        )?;

        let rows = stmt.query_map(params![limit as i64], |row| {
            Ok(StreamEventRecord {
                session_id: row.get(0)?,
                agent: row.get(1)?,
                stream: row.get(2)?,
                content: row.get(3)?,
                at: parse_required_datetime(row.get(4)?)?,
            })
        })?;

        let mut events = Vec::new();
        for row in rows {
            events.push(row?);
        }
        events.reverse();
        Ok(events)
    }

    pub fn agent_current_task(&self, agent_name: &str) -> Result<Option<String>> {
        let task = self
            .conn
            .query_row(
                "SELECT current_task_id FROM agents WHERE name = ?1",
                params![agent_name],
                |row| row.get::<_, Option<String>>(0),
            )
            .optional()?;

        Ok(task.flatten())
    }
}

fn serialize_role(role: &AgentRole) -> &'static str {
    match role {
        AgentRole::Main => "main",
        AgentRole::Child => "child",
    }
}

fn parse_role(role: String) -> AgentRole {
    match role.as_str() {
        "main" => AgentRole::Main,
        _ => AgentRole::Child,
    }
}

fn serialize_agent_state(state: &AgentSessionState) -> &'static str {
    match state {
        AgentSessionState::Idle => "idle",
        AgentSessionState::Busy => "busy",
        AgentSessionState::Blocked => "blocked",
        AgentSessionState::Offline => "offline",
    }
}

fn parse_agent_state(state: String) -> AgentSessionState {
    match state.as_str() {
        "busy" => AgentSessionState::Busy,
        "blocked" => AgentSessionState::Blocked,
        "offline" => AgentSessionState::Offline,
        _ => AgentSessionState::Idle,
    }
}

fn serialize_session_mode(mode: &SessionMode) -> &'static str {
    match mode {
        SessionMode::Round => "round",
        SessionMode::Pty => "pty",
    }
}

fn parse_session_mode(mode: String) -> SessionMode {
    match mode.as_str() {
        "pty" => SessionMode::Pty,
        _ => SessionMode::Round,
    }
}

fn serialize_codex_session_status(status: &CodexSessionStatus) -> &'static str {
    match status {
        CodexSessionStatus::Running => "running",
        CodexSessionStatus::Succeeded => "succeeded",
        CodexSessionStatus::Failed => "failed",
    }
}

fn parse_codex_session_status(status: String) -> CodexSessionStatus {
    match status.as_str() {
        "succeeded" => CodexSessionStatus::Succeeded,
        "failed" => CodexSessionStatus::Failed,
        _ => CodexSessionStatus::Running,
    }
}

fn serialize_task_state(state: &TaskState) -> &'static str {
    match state {
        TaskState::Pending => "pending",
        TaskState::Accepted => "accepted",
        TaskState::Running => "running",
        TaskState::Completed => "completed",
        TaskState::Reported => "reported",
        TaskState::Analyzed => "analyzed",
        TaskState::DecisionSent => "decision_sent",
        TaskState::Closed => "closed",
        TaskState::BlockedWaitingDecision => "blocked_waiting_decision",
        TaskState::Cancelled => "cancelled",
        TaskState::Failed => "failed",
    }
}

fn parse_task_state(state: String) -> TaskState {
    match state.as_str() {
        "accepted" => TaskState::Accepted,
        "running" => TaskState::Running,
        "completed" => TaskState::Completed,
        "reported" => TaskState::Reported,
        "analyzed" => TaskState::Analyzed,
        "decision_sent" => TaskState::DecisionSent,
        "closed" => TaskState::Closed,
        "blocked_waiting_decision" => TaskState::BlockedWaitingDecision,
        "cancelled" => TaskState::Cancelled,
        "failed" => TaskState::Failed,
        _ => TaskState::Pending,
    }
}

fn parse_datetime(value: Option<String>) -> Option<DateTime<Utc>> {
    value
        .and_then(|raw| DateTime::parse_from_rfc3339(&raw).ok())
        .map(|dt| dt.with_timezone(&Utc))
}

fn parse_required_datetime(value: String) -> rusqlite::Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(&value)
        .map(|dt| dt.with_timezone(&Utc))
        .map_err(|err| {
            rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(err))
        })
}
