use std::path::Path;

use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};
use rusqlite::{Connection, OptionalExtension, params};

use crate::models::{
    AgentBootstrapState, AgentRole, AgentSessionState, AgentSummary, AppServerOwner,
    BridgeConnectionState, BridgeMode, CodexSessionStatus, DecisionSummary, RuntimeEventRecord,
    SessionMode, SessionSummary, StreamEventRecord, TaskState, TaskSummary, VisiblePaneKind,
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
                prompt_path TEXT,
                thread_id TEXT,
                app_server_url TEXT,
                app_server_owner TEXT,
                app_server_registered_at TEXT,
                visible_pane_pid INTEGER,
                visible_pane_kind TEXT,
                visible_pane_registered_at TEXT,
                current_session_id TEXT,
                state TEXT NOT NULL,
                bootstrap_state TEXT NOT NULL DEFAULT 'awaiting_init',
                bootstrap_summary TEXT,
                bootstrap_completed_at TEXT,
                current_task_id TEXT,
                last_output_at TEXT,
                last_heartbeat_at TEXT,
                bridge_state TEXT NOT NULL DEFAULT 'disconnected',
                bridge_mode TEXT,
                bridge_session_id TEXT,
                bridge_connected_at TEXT,
                bridge_last_seen_at TEXT,
                bridge_last_delivery_id INTEGER NOT NULL DEFAULT 0,
                bridge_last_ack_delivery_id INTEGER NOT NULL DEFAULT 0,
                bridge_pending_delivery_count INTEGER NOT NULL DEFAULT 0,
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
                effort TEXT,
                read_scope_json TEXT NOT NULL DEFAULT '[]',
                write_scope_json TEXT NOT NULL DEFAULT '[]',
                acceptance_json TEXT NOT NULL DEFAULT '[]',
                auto_resolve_by TEXT,
                auto_resolve_summary TEXT,
                round_count INTEGER NOT NULL DEFAULT 0,
                latest_child_status TEXT,
                latest_child_summary TEXT,
                latest_child_blocking TEXT,
                latest_child_topic TEXT,
                latest_child_details TEXT,
                latest_decision_id TEXT,
                latest_decision_summary TEXT,
                latest_decision_status TEXT,
                latest_decision_issued_by TEXT,
                latest_decision_at TEXT,
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

            CREATE TABLE IF NOT EXISTS runtime_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scope TEXT NOT NULL,
                scope_id TEXT NOT NULL,
                agent_name TEXT,
                task_id TEXT,
                session_id TEXT,
                actor_name TEXT,
                event_type TEXT NOT NULL,
                summary TEXT NOT NULL,
                reason TEXT,
                payload_json TEXT,
                created_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_runtime_events_agent_name ON runtime_events(agent_name, id DESC);
            CREATE INDEX IF NOT EXISTS idx_runtime_events_actor_name ON runtime_events(actor_name, id DESC);
            CREATE INDEX IF NOT EXISTS idx_runtime_events_task_id ON runtime_events(task_id, id DESC);
            CREATE INDEX IF NOT EXISTS idx_runtime_events_session_id ON runtime_events(session_id, id DESC);
            CREATE INDEX IF NOT EXISTS idx_runtime_events_scope ON runtime_events(scope, scope_id, id DESC);

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
        self.ensure_column("agents", "prompt_path", "TEXT")?;
        self.ensure_column("agents", "thread_id", "TEXT")?;
        self.ensure_column("agents", "app_server_url", "TEXT")?;
        self.ensure_column("agents", "app_server_owner", "TEXT")?;
        self.ensure_column("agents", "app_server_registered_at", "TEXT")?;
        self.ensure_column("agents", "visible_pane_pid", "INTEGER")?;
        self.ensure_column("agents", "visible_pane_kind", "TEXT")?;
        self.ensure_column("agents", "visible_pane_registered_at", "TEXT")?;
        self.ensure_column("agents", "current_session_id", "TEXT")?;
        self.ensure_column(
            "agents",
            "bootstrap_state",
            "TEXT NOT NULL DEFAULT 'awaiting_init'",
        )?;
        self.ensure_column("agents", "bootstrap_summary", "TEXT")?;
        self.ensure_column("agents", "bootstrap_completed_at", "TEXT")?;
        self.ensure_column(
            "agents",
            "bridge_state",
            "TEXT NOT NULL DEFAULT 'disconnected'",
        )?;
        self.ensure_column("agents", "bridge_mode", "TEXT")?;
        self.ensure_column("agents", "bridge_session_id", "TEXT")?;
        self.ensure_column("agents", "bridge_connected_at", "TEXT")?;
        self.ensure_column("agents", "bridge_last_seen_at", "TEXT")?;
        self.ensure_column(
            "agents",
            "bridge_last_delivery_id",
            "INTEGER NOT NULL DEFAULT 0",
        )?;
        self.ensure_column(
            "agents",
            "bridge_last_ack_delivery_id",
            "INTEGER NOT NULL DEFAULT 0",
        )?;
        self.ensure_column(
            "agents",
            "bridge_pending_delivery_count",
            "INTEGER NOT NULL DEFAULT 0",
        )?;
        self.ensure_column("tasks", "auto_resolve_by", "TEXT")?;
        self.ensure_column("tasks", "auto_resolve_summary", "TEXT")?;
        self.ensure_column("tasks", "effort", "TEXT")?;
        self.ensure_column("tasks", "read_scope_json", "TEXT NOT NULL DEFAULT '[]'")?;
        self.ensure_column("tasks", "write_scope_json", "TEXT NOT NULL DEFAULT '[]'")?;
        self.ensure_column("tasks", "acceptance_json", "TEXT NOT NULL DEFAULT '[]'")?;
        self.ensure_column("tasks", "round_count", "INTEGER NOT NULL DEFAULT 0")?;
        self.ensure_column("tasks", "latest_child_status", "TEXT")?;
        self.ensure_column("tasks", "latest_child_summary", "TEXT")?;
        self.ensure_column("tasks", "latest_child_blocking", "TEXT")?;
        self.ensure_column("tasks", "latest_child_topic", "TEXT")?;
        self.ensure_column("tasks", "latest_child_details", "TEXT")?;
        self.ensure_column("tasks", "latest_decision_id", "TEXT")?;
        self.ensure_column("tasks", "latest_decision_summary", "TEXT")?;
        self.ensure_column("tasks", "latest_decision_status", "TEXT")?;
        self.ensure_column("tasks", "latest_decision_issued_by", "TEXT")?;
        self.ensure_column("tasks", "latest_decision_at", "TEXT")?;
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
                id, name, role, repo_name, cwd, prompt_path, thread_id, app_server_url, app_server_owner, app_server_registered_at, visible_pane_pid, visible_pane_kind, visible_pane_registered_at, current_session_id, state, bootstrap_state, bootstrap_summary, bootstrap_completed_at, current_task_id, last_output_at, last_heartbeat_at, bridge_state, bridge_mode, bridge_session_id, bridge_connected_at, bridge_last_seen_at, bridge_last_delivery_id, bridge_last_ack_delivery_id, bridge_pending_delivery_count, created_at, updated_at
            ) VALUES (
                ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25, ?26, ?27, ?28, ?29, ?30, ?31
            )
            ON CONFLICT(name) DO UPDATE SET
                role = excluded.role,
                repo_name = excluded.repo_name,
                cwd = excluded.cwd,
                prompt_path = excluded.prompt_path,
                thread_id = excluded.thread_id,
                app_server_url = excluded.app_server_url,
                app_server_owner = excluded.app_server_owner,
                app_server_registered_at = excluded.app_server_registered_at,
                visible_pane_pid = excluded.visible_pane_pid,
                visible_pane_kind = excluded.visible_pane_kind,
                visible_pane_registered_at = excluded.visible_pane_registered_at,
                current_session_id = excluded.current_session_id,
                state = excluded.state,
                bootstrap_state = excluded.bootstrap_state,
                bootstrap_summary = excluded.bootstrap_summary,
                bootstrap_completed_at = excluded.bootstrap_completed_at,
                current_task_id = excluded.current_task_id,
                last_output_at = excluded.last_output_at,
                last_heartbeat_at = excluded.last_heartbeat_at,
                bridge_state = excluded.bridge_state,
                bridge_mode = excluded.bridge_mode,
                bridge_session_id = excluded.bridge_session_id,
                bridge_connected_at = excluded.bridge_connected_at,
                bridge_last_seen_at = excluded.bridge_last_seen_at,
                bridge_last_delivery_id = excluded.bridge_last_delivery_id,
                bridge_last_ack_delivery_id = excluded.bridge_last_ack_delivery_id,
                bridge_pending_delivery_count = excluded.bridge_pending_delivery_count,
                updated_at = excluded.updated_at
            "#,
            params![
                agent.name,
                agent.name,
                serialize_role(&agent.role),
                agent.repo_name,
                agent.cwd,
                agent.prompt_path,
                agent.thread_id,
                agent.app_server_url,
                agent.app_server_owner.as_ref().map(serialize_app_server_owner),
                agent.app_server_registered_at.map(|dt| dt.to_rfc3339()),
                agent.visible_pane_pid.map(i64::from),
                agent.visible_pane_kind.as_ref().map(serialize_visible_pane_kind),
                agent.visible_pane_registered_at.map(|dt| dt.to_rfc3339()),
                agent.current_session_id,
                serialize_agent_state(&agent.state),
                serialize_bootstrap_state(&agent.bootstrap_state),
                agent.bootstrap_summary,
                agent.bootstrap_completed_at.map(|dt| dt.to_rfc3339()),
                agent.current_task_id,
                agent.last_output_at.map(|dt| dt.to_rfc3339()),
                agent.last_heartbeat_at.map(|dt| dt.to_rfc3339()),
                serialize_bridge_state(&agent.bridge_state),
                agent.bridge_mode.as_ref().map(serialize_bridge_mode),
                agent.bridge_session_id,
                agent.bridge_connected_at.map(|dt| dt.to_rfc3339()),
                agent.bridge_last_seen_at.map(|dt| dt.to_rfc3339()),
                i64::try_from(agent.bridge_last_delivery_id).unwrap_or(i64::MAX),
                i64::try_from(agent.bridge_last_ack_delivery_id).unwrap_or(i64::MAX),
                i64::from(agent.bridge_pending_delivery_count),
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
                id, from_agent, to_agent, title, summary, effort, read_scope_json, write_scope_json, acceptance_json, auto_resolve_by, auto_resolve_summary, round_count, latest_child_status, latest_child_summary, latest_child_blocking, latest_child_topic, latest_child_details, latest_decision_id, latest_decision_summary, latest_decision_status, latest_decision_issued_by, latest_decision_at, state, created_at, updated_at, closed_at
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25, ?26)
            "#,
            params![
                task.task_id,
                task.from_agent,
                task.to_agent,
                task.title,
                task.summary,
                task.effort,
                serialize_string_list(&task.read_scope),
                serialize_string_list(&task.write_scope),
                serialize_string_list(&task.acceptance),
                task.auto_resolve_by,
                task.auto_resolve_summary,
                i64::from(task.round_count),
                task.latest_child_status,
                task.latest_child_summary,
                task.latest_child_blocking,
                task.latest_child_topic,
                task.latest_child_details,
                task.latest_decision_id,
                task.latest_decision_summary,
                task.latest_decision_status,
                task.latest_decision_issued_by,
                task.latest_decision_at.map(|dt| dt.to_rfc3339()),
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
            SET effort = ?2, read_scope_json = ?3, write_scope_json = ?4, acceptance_json = ?5, round_count = ?6, latest_child_status = ?7, latest_child_summary = ?8, latest_child_blocking = ?9, latest_child_topic = ?10, latest_child_details = ?11, latest_decision_id = ?12, latest_decision_summary = ?13, latest_decision_status = ?14, latest_decision_issued_by = ?15, latest_decision_at = ?16, state = ?17, updated_at = ?18, closed_at = ?19
            WHERE id = ?1
            "#,
            params![
                task.task_id,
                task.effort,
                serialize_string_list(&task.read_scope),
                serialize_string_list(&task.write_scope),
                serialize_string_list(&task.acceptance),
                i64::from(task.round_count),
                task.latest_child_status,
                task.latest_child_summary,
                task.latest_child_blocking,
                task.latest_child_topic,
                task.latest_child_details,
                task.latest_decision_id,
                task.latest_decision_summary,
                task.latest_decision_status,
                task.latest_decision_issued_by,
                task.latest_decision_at.map(|dt| dt.to_rfc3339()),
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

    pub fn insert_runtime_event(
        &self,
        scope: &str,
        scope_id: &str,
        agent_name: Option<&str>,
        task_id: Option<&str>,
        session_id: Option<&str>,
        actor_name: Option<&str>,
        event_type: &str,
        summary: &str,
        reason: Option<&str>,
        payload_json: Option<&str>,
    ) -> Result<RuntimeEventRecord> {
        let created_at = Utc::now();
        self.conn.execute(
            r#"
            INSERT INTO runtime_events (
                scope, scope_id, agent_name, task_id, session_id, actor_name, event_type, summary, reason, payload_json, created_at
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
            "#,
            params![
                scope,
                scope_id,
                agent_name,
                task_id,
                session_id,
                actor_name,
                event_type,
                summary,
                reason,
                payload_json,
                created_at.to_rfc3339(),
            ],
        )?;
        Ok(RuntimeEventRecord {
            id: self.conn.last_insert_rowid(),
            scope: scope.to_string(),
            scope_id: scope_id.to_string(),
            agent_name: agent_name.map(str::to_string),
            task_id: task_id.map(str::to_string),
            session_id: session_id.map(str::to_string),
            actor_name: actor_name.map(str::to_string),
            event_type: event_type.to_string(),
            summary: summary.to_string(),
            reason: reason.map(str::to_string),
            payload_json: payload_json.map(str::to_string),
            created_at,
        })
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
            SELECT name, role, repo_name, cwd, prompt_path, thread_id, app_server_url, app_server_owner, app_server_registered_at, visible_pane_pid, visible_pane_kind, visible_pane_registered_at, current_session_id, state, bootstrap_state, bootstrap_summary, bootstrap_completed_at, current_task_id, last_output_at, last_heartbeat_at, bridge_state, bridge_mode, bridge_session_id, bridge_connected_at, bridge_last_seen_at, bridge_last_delivery_id, bridge_last_ack_delivery_id, bridge_pending_delivery_count
            FROM agents
            ORDER BY name ASC
            "#,
        )?;

        let rows = stmt.query_map([], |row| {
            let visible_pane_pid: Option<i64> = row.get(9)?;
            let bridge_last_delivery_id: i64 = row.get(25)?;
            let bridge_last_ack_delivery_id: i64 = row.get(26)?;
            let bridge_pending_delivery_count: i64 = row.get(27)?;
            Ok(AgentSummary {
                name: row.get(0)?,
                role: parse_role(row.get::<_, String>(1)?),
                repo_name: row.get(2)?,
                cwd: row.get(3)?,
                prompt_path: row.get(4)?,
                thread_id: row.get(5)?,
                app_server_url: row.get(6)?,
                app_server_owner: row.get::<_, Option<String>>(7)?.map(parse_app_server_owner),
                app_server_registered_at: parse_datetime(row.get(8)?),
                visible_pane_pid: visible_pane_pid.and_then(|value| u32::try_from(value).ok()),
                visible_pane_kind: row.get::<_, Option<String>>(10)?.map(parse_visible_pane_kind),
                visible_pane_registered_at: parse_datetime(row.get(11)?),
                current_session_id: row.get(12)?,
                state: parse_agent_state(row.get::<_, String>(13)?),
                bootstrap_state: parse_bootstrap_state(row.get::<_, String>(14)?),
                bootstrap_summary: row.get(15)?,
                bootstrap_completed_at: parse_datetime(row.get(16)?),
                current_task_id: row.get(17)?,
                last_output_at: parse_datetime(row.get(18)?),
                last_heartbeat_at: parse_datetime(row.get(19)?),
                bridge_state: parse_bridge_state(row.get::<_, String>(20)?),
                bridge_mode: row.get::<_, Option<String>>(21)?.map(parse_bridge_mode),
                bridge_session_id: row.get(22)?,
                bridge_connected_at: parse_datetime(row.get(23)?),
                bridge_last_seen_at: parse_datetime(row.get(24)?),
                bridge_last_delivery_id: bridge_last_delivery_id.max(0) as u64,
                bridge_last_ack_delivery_id: bridge_last_ack_delivery_id.max(0) as u64,
                bridge_pending_delivery_count: bridge_pending_delivery_count.max(0) as u32,
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
            SELECT id, from_agent, to_agent, title, summary, effort, read_scope_json, write_scope_json, acceptance_json, auto_resolve_by, auto_resolve_summary, round_count, latest_child_status, latest_child_summary, latest_child_blocking, latest_child_topic, latest_child_details, latest_decision_id, latest_decision_summary, latest_decision_status, latest_decision_issued_by, latest_decision_at, state, created_at, updated_at, closed_at
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
                effort: row.get(5)?,
                read_scope: deserialize_string_list(row.get(6)?),
                write_scope: deserialize_string_list(row.get(7)?),
                acceptance: deserialize_string_list(row.get(8)?),
                auto_resolve_by: row.get(9)?,
                auto_resolve_summary: row.get(10)?,
                round_count: row.get::<_, i64>(11)?.max(0) as u32,
                latest_child_status: row.get(12)?,
                latest_child_summary: row.get(13)?,
                latest_child_blocking: row.get(14)?,
                latest_child_topic: row.get(15)?,
                latest_child_details: row.get(16)?,
                latest_decision_id: row.get(17)?,
                latest_decision_summary: row.get(18)?,
                latest_decision_status: row.get(19)?,
                latest_decision_issued_by: row.get(20)?,
                latest_decision_at: parse_datetime(row.get(21)?),
                state: parse_task_state(row.get::<_, String>(22)?),
                created_at: parse_required_datetime(row.get(23)?)?,
                updated_at: parse_required_datetime(row.get(24)?)?,
                closed_at: parse_datetime(row.get(25)?),
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

    pub fn load_runtime_events_for_agent(
        &self,
        agent_name: &str,
        limit: usize,
    ) -> Result<Vec<RuntimeEventRecord>> {
        let mut stmt = self.conn.prepare(
            r#"
            SELECT id, scope, scope_id, agent_name, task_id, session_id, actor_name, event_type, summary, reason, payload_json, created_at
            FROM runtime_events
            WHERE (scope = 'agent' AND scope_id = ?1)
               OR agent_name = ?1
               OR actor_name = ?1
            ORDER BY id DESC
            LIMIT ?2
            "#,
        )?;
        let rows = stmt.query_map(params![agent_name, limit as i64], |row| {
            Ok(RuntimeEventRecord {
                id: row.get(0)?,
                scope: row.get(1)?,
                scope_id: row.get(2)?,
                agent_name: row.get(3)?,
                task_id: row.get(4)?,
                session_id: row.get(5)?,
                actor_name: row.get(6)?,
                event_type: row.get(7)?,
                summary: row.get(8)?,
                reason: row.get(9)?,
                payload_json: row.get(10)?,
                created_at: parse_required_datetime(row.get(11)?)?,
            })
        })?;

        let mut events = Vec::new();
        for row in rows {
            events.push(row?);
        }
        events.reverse();
        Ok(events)
    }

    pub fn load_runtime_events_for_task(
        &self,
        task_id: &str,
        limit: usize,
    ) -> Result<Vec<RuntimeEventRecord>> {
        let mut stmt = self.conn.prepare(
            r#"
            SELECT id, scope, scope_id, agent_name, task_id, session_id, actor_name, event_type, summary, reason, payload_json, created_at
            FROM runtime_events
            WHERE (scope = 'task' AND scope_id = ?1)
               OR task_id = ?1
            ORDER BY id DESC
            LIMIT ?2
            "#,
        )?;
        let rows = stmt.query_map(params![task_id, limit as i64], |row| {
            Ok(RuntimeEventRecord {
                id: row.get(0)?,
                scope: row.get(1)?,
                scope_id: row.get(2)?,
                agent_name: row.get(3)?,
                task_id: row.get(4)?,
                session_id: row.get(5)?,
                actor_name: row.get(6)?,
                event_type: row.get(7)?,
                summary: row.get(8)?,
                reason: row.get(9)?,
                payload_json: row.get(10)?,
                created_at: parse_required_datetime(row.get(11)?)?,
            })
        })?;

        let mut events = Vec::new();
        for row in rows {
            events.push(row?);
        }
        events.reverse();
        Ok(events)
    }

    pub fn load_runtime_events_for_session(
        &self,
        session_id: &str,
        limit: usize,
    ) -> Result<Vec<RuntimeEventRecord>> {
        let mut stmt = self.conn.prepare(
            r#"
            SELECT id, scope, scope_id, agent_name, task_id, session_id, actor_name, event_type, summary, reason, payload_json, created_at
            FROM runtime_events
            WHERE (scope = 'session' AND scope_id = ?1)
               OR session_id = ?1
            ORDER BY id DESC
            LIMIT ?2
            "#,
        )?;
        let rows = stmt.query_map(params![session_id, limit as i64], |row| {
            Ok(RuntimeEventRecord {
                id: row.get(0)?,
                scope: row.get(1)?,
                scope_id: row.get(2)?,
                agent_name: row.get(3)?,
                task_id: row.get(4)?,
                session_id: row.get(5)?,
                actor_name: row.get(6)?,
                event_type: row.get(7)?,
                summary: row.get(8)?,
                reason: row.get(9)?,
                payload_json: row.get(10)?,
                created_at: parse_required_datetime(row.get(11)?)?,
            })
        })?;

        let mut events = Vec::new();
        for row in rows {
            events.push(row?);
        }
        events.reverse();
        Ok(events)
    }

    pub fn load_recent_runtime_events(&self, limit: usize) -> Result<Vec<RuntimeEventRecord>> {
        let mut stmt = self.conn.prepare(
            r#"
            SELECT id, scope, scope_id, agent_name, task_id, session_id, actor_name, event_type, summary, reason, payload_json, created_at
            FROM runtime_events
            ORDER BY id DESC
            LIMIT ?1
            "#,
        )?;
        let rows = stmt.query_map(params![limit as i64], |row| {
            Ok(RuntimeEventRecord {
                id: row.get(0)?,
                scope: row.get(1)?,
                scope_id: row.get(2)?,
                agent_name: row.get(3)?,
                task_id: row.get(4)?,
                session_id: row.get(5)?,
                actor_name: row.get(6)?,
                event_type: row.get(7)?,
                summary: row.get(8)?,
                reason: row.get(9)?,
                payload_json: row.get(10)?,
                created_at: parse_required_datetime(row.get(11)?)?,
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

    pub fn delete_agents_by_names(&self, agent_names: &[String]) -> Result<usize> {
        let mut removed = 0;
        for agent_name in agent_names {
            removed += self
                .conn
                .execute("DELETE FROM agents WHERE name = ?1", params![agent_name])?;
        }
        Ok(removed)
    }

    pub fn delete_tasks_by_ids(&self, task_ids: &[String]) -> Result<usize> {
        let mut removed = 0;
        for task_id in task_ids {
            removed += self
                .conn
                .execute("DELETE FROM tasks WHERE id = ?1", params![task_id])?;
        }
        Ok(removed)
    }

    pub fn delete_task_events_by_task_ids(&self, task_ids: &[String]) -> Result<usize> {
        let mut removed = 0;
        for task_id in task_ids {
            removed += self.conn.execute(
                "DELETE FROM task_events WHERE task_id = ?1",
                params![task_id],
            )?;
        }
        Ok(removed)
    }

    pub fn delete_runtime_events_by_agent_names(&self, agent_names: &[String]) -> Result<usize> {
        let mut removed = 0;
        for agent_name in agent_names {
            removed += self.conn.execute(
                "DELETE FROM runtime_events WHERE agent_name = ?1 OR actor_name = ?1 OR (scope = 'agent' AND scope_id = ?1)",
                params![agent_name],
            )?;
        }
        Ok(removed)
    }

    pub fn delete_runtime_events_by_task_ids(&self, task_ids: &[String]) -> Result<usize> {
        let mut removed = 0;
        for task_id in task_ids {
            removed += self.conn.execute(
                "DELETE FROM runtime_events WHERE task_id = ?1 OR (scope = 'task' AND scope_id = ?1)",
                params![task_id],
            )?;
        }
        Ok(removed)
    }

    pub fn delete_decisions_by_task_ids(&self, task_ids: &[String]) -> Result<usize> {
        let mut removed = 0;
        for task_id in task_ids {
            removed += self
                .conn
                .execute("DELETE FROM decisions WHERE task_id = ?1", params![task_id])?;
        }
        Ok(removed)
    }

    pub fn delete_sessions_by_agent_names(&self, agent_names: &[String]) -> Result<usize> {
        let mut removed = 0;
        for agent_name in agent_names {
            removed += self.conn.execute(
                "DELETE FROM sessions WHERE agent_id = ?1",
                params![agent_name],
            )?;
        }
        Ok(removed)
    }

    pub fn delete_stream_events_by_agent_names(&self, agent_names: &[String]) -> Result<usize> {
        let mut removed = 0;
        for agent_name in agent_names {
            removed += self.conn.execute(
                "DELETE FROM stream_events WHERE agent_name = ?1",
                params![agent_name],
            )?;
        }
        Ok(removed)
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

fn serialize_bootstrap_state(state: &AgentBootstrapState) -> &'static str {
    match state {
        AgentBootstrapState::AwaitingInit => "awaiting_init",
        AgentBootstrapState::Ready => "ready",
    }
}

fn parse_bootstrap_state(state: String) -> AgentBootstrapState {
    match state.as_str() {
        "ready" => AgentBootstrapState::Ready,
        _ => AgentBootstrapState::AwaitingInit,
    }
}

fn serialize_bridge_state(state: &BridgeConnectionState) -> &'static str {
    match state {
        BridgeConnectionState::Disconnected => "disconnected",
        BridgeConnectionState::Connected => "connected",
    }
}

fn parse_bridge_state(state: String) -> BridgeConnectionState {
    match state.as_str() {
        "connected" => BridgeConnectionState::Connected,
        _ => BridgeConnectionState::Disconnected,
    }
}

fn serialize_bridge_mode(mode: &BridgeMode) -> &'static str {
    match mode {
        BridgeMode::Passive => "passive",
        BridgeMode::Autorun => "autorun",
    }
}

fn parse_bridge_mode(mode: String) -> BridgeMode {
    match mode.as_str() {
        "autorun" => BridgeMode::Autorun,
        _ => BridgeMode::Passive,
    }
}

fn serialize_app_server_owner(owner: &AppServerOwner) -> &'static str {
    match owner {
        AppServerOwner::Bridge => "bridge",
        AppServerOwner::Daemon => "daemon",
    }
}

fn parse_app_server_owner(owner: String) -> AppServerOwner {
    match owner.as_str() {
        "daemon" => AppServerOwner::Daemon,
        _ => AppServerOwner::Bridge,
    }
}

fn serialize_visible_pane_kind(kind: &VisiblePaneKind) -> &'static str {
    match kind {
        VisiblePaneKind::Shell => "shell",
        VisiblePaneKind::View => "view",
    }
}

fn parse_visible_pane_kind(kind: String) -> VisiblePaneKind {
    match kind.as_str() {
        "view" => VisiblePaneKind::View,
        _ => VisiblePaneKind::Shell,
    }
}

fn serialize_session_mode(mode: &SessionMode) -> &'static str {
    match mode {
        SessionMode::Round => "round",
        SessionMode::Pty => "pty",
        SessionMode::AppServer => "app_server",
    }
}

fn parse_session_mode(mode: String) -> SessionMode {
    match mode.as_str() {
        "pty" => SessionMode::Pty,
        "app_server" => SessionMode::AppServer,
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

fn serialize_string_list(values: &[String]) -> String {
    serde_json::to_string(values).unwrap_or_else(|_| "[]".to_string())
}

fn deserialize_string_list(value: Option<String>) -> Vec<String> {
    value
        .and_then(|raw| serde_json::from_str::<Vec<String>>(&raw).ok())
        .unwrap_or_default()
}

fn parse_required_datetime(value: String) -> rusqlite::Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(&value)
        .map(|dt| dt.with_timezone(&Utc))
        .map_err(|err| {
            rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(err))
        })
}
