"""SQLite schema + connection helpers for the collection audit console."""
import json
import os
import sqlite3
import time

SCHEMA = """
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS sources (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  kind TEXT NOT NULL,               -- ftp | api | manual
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS batches (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  source_id INTEGER NOT NULL REFERENCES sources(id),
  label TEXT NOT NULL,              -- e.g. B-20261119-01
  status TEXT NOT NULL DEFAULT 'received',  -- received | parsing | parsed | failed
  current_version INTEGER NOT NULL DEFAULT 1,
  created_at TEXT NOT NULL
);

-- A batch version is one immutable conclusion set. Corrections only ever ADD
-- a new version; old versions (and their records/entities) are kept forever.
CREATE TABLE IF NOT EXISTS batch_versions (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  batch_id INTEGER NOT NULL REFERENCES batches(id),
  version INTEGER NOT NULL,
  state TEXT NOT NULL DEFAULT 'active',   -- active | superseded
  origin TEXT NOT NULL DEFAULT 'ingest',  -- ingest | correction
  correction_id INTEGER,
  note TEXT DEFAULT '',
  created_at TEXT NOT NULL,
  UNIQUE(batch_id, version)
);

CREATE TABLE IF NOT EXISTS raw_files (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  batch_version_id INTEGER NOT NULL REFERENCES batch_versions(id),
  filename TEXT NOT NULL,
  checksum TEXT NOT NULL,
  size_bytes INTEGER NOT NULL DEFAULT 0,
  payload TEXT NOT NULL DEFAULT '{}',     -- simulated file content (JSON)
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS parse_records (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  batch_version_id INTEGER NOT NULL REFERENCES batch_versions(id),
  raw_file_id INTEGER NOT NULL REFERENCES raw_files(id),
  record_key TEXT NOT NULL,               -- business key inside the file
  status TEXT NOT NULL,                   -- ok | warn | error
  payload TEXT NOT NULL DEFAULT '{}',
  error_msg TEXT DEFAULT '',
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS entities (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  batch_version_id INTEGER NOT NULL REFERENCES batch_versions(id),
  parse_record_id INTEGER REFERENCES parse_records(id),
  entity_type TEXT NOT NULL,              -- order | customer | sku | source_rollup
  entity_key TEXT NOT NULL,
  payload TEXT NOT NULL DEFAULT '{}',
  state TEXT NOT NULL DEFAULT 'active',   -- active | superseded  (superseded = 原结论保留)
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS tasks (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  task_type TEXT NOT NULL,                -- parse | extract | aggregate | correction
  batch_version_id INTEGER REFERENCES batch_versions(id),
  correction_id INTEGER,
  status TEXT NOT NULL DEFAULT 'queued',  -- queued | running | done | failed | cancelled | superseded
  depends_on TEXT NOT NULL DEFAULT '[]',  -- JSON array of task ids
  attempt INTEGER NOT NULL DEFAULT 1,
  supersedes INTEGER,                     -- task id this one replaces (re-queue chain)
  cancel_requested INTEGER NOT NULL DEFAULT 0,
  result TEXT NOT NULL DEFAULT '{}',      -- JSON: counts, duration, impact...
  created_at TEXT NOT NULL,
  started_at TEXT,
  finished_at TEXT
);

CREATE TABLE IF NOT EXISTS corrections (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  batch_id INTEGER NOT NULL REFERENCES batches(id),
  base_version INTEGER NOT NULL,          -- version the correction was authored against
  status TEXT NOT NULL DEFAULT 'pending', -- pending | running | done | conflicted | cancelled
  reason TEXT NOT NULL DEFAULT '',
  conflict_reason TEXT DEFAULT '',
  result_version INTEGER,
  impact TEXT NOT NULL DEFAULT '{}',      -- JSON: affected records/entities, requeued tasks
  created_by TEXT NOT NULL DEFAULT 'auditor',
  created_at TEXT NOT NULL,
  finished_at TEXT
);

CREATE TABLE IF NOT EXISTS correction_items (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  correction_id INTEGER NOT NULL REFERENCES corrections(id),
  action TEXT NOT NULL,                   -- fix | drop | add
  record_key TEXT NOT NULL,
  patch TEXT NOT NULL DEFAULT '{}'        -- fix/add: new payload(+status); drop: ignored
);

CREATE TABLE IF NOT EXISTS events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ts TEXT NOT NULL,
  kind TEXT NOT NULL,                     -- task | correction | batch | system
  ref_type TEXT NOT NULL DEFAULT '',
  ref_id INTEGER,
  message TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_bv_batch ON batch_versions(batch_id);
CREATE INDEX IF NOT EXISTS idx_rf_bv ON raw_files(batch_version_id);
CREATE INDEX IF NOT EXISTS idx_pr_bv ON parse_records(batch_version_id);
CREATE INDEX IF NOT EXISTS idx_pr_file ON parse_records(raw_file_id);
CREATE INDEX IF NOT EXISTS idx_ent_bv ON entities(batch_version_id);
CREATE INDEX IF NOT EXISTS idx_tasks_bv ON tasks(batch_version_id);
CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status);
"""


def now():
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime()) + f".{int(time.time()*1000)%1000:03d}"


def connect(db_path):
    conn = sqlite3.connect(db_path, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


def init_db(db_path):
    fresh = not os.path.exists(db_path)
    conn = connect(db_path)
    try:
        conn.executescript(SCHEMA)
        conn.commit()
    finally:
        conn.close()
    return fresh


def reset_db(db_path):
    for suffix in ("", "-wal", "-shm"):
        try:
            os.remove(db_path + suffix)
        except FileNotFoundError:
            pass
    init_db(db_path)


def log_event(conn, kind, message, ref_type="", ref_id=None):
    conn.execute(
        "INSERT INTO events(ts, kind, ref_type, ref_id, message) VALUES(?,?,?,?,?)",
        (now(), kind, ref_type, ref_id, message),
    )


def dumps(obj):
    return json.dumps(obj, ensure_ascii=False)


def loads(text, default=None):
    try:
        return json.loads(text)
    except Exception:
        return default if default is not None else {}
