#!/usr/bin/env python3
import json
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
DB_PATH = Path(os.environ.get("MACRO_DB_PATH", str(DATA_DIR / "macro_monitor.db")))


def now_iso():
  return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def get_conn():
  DB_PATH.parent.mkdir(parents=True, exist_ok=True)
  conn = sqlite3.connect(DB_PATH)
  conn.row_factory = sqlite3.Row
  return conn


def init_db():
  conn = get_conn()
  conn.executescript(
    """
    CREATE TABLE IF NOT EXISTS model_snapshots (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      as_of TEXT,
      report_date TEXT,
      generated_at TEXT,
      payload_json TEXT NOT NULL,
      created_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS sheet_rows (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      sheet_name TEXT NOT NULL,
      row_index INTEGER NOT NULL,
      row_json TEXT NOT NULL,
      as_of TEXT,
      created_at TEXT NOT NULL
    );
    CREATE UNIQUE INDEX IF NOT EXISTS idx_sheet_rows_unique ON sheet_rows(sheet_name, row_index, as_of);

    CREATE TABLE IF NOT EXISTS daily_reports (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      report_date TEXT UNIQUE NOT NULL,
      score REAL,
      status TEXT,
      summary TEXT,
      report_text TEXT,
      report_path TEXT,
      payload_json TEXT,
      generated_at TEXT,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS online_checks (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      checked_at TEXT NOT NULL,
      summary_json TEXT,
      rows_json TEXT
    );

    CREATE TABLE IF NOT EXISTS subscribers (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      email TEXT UNIQUE NOT NULL,
      status TEXT NOT NULL DEFAULT 'active',
      source TEXT,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS email_dispatch_logs (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      report_date TEXT,
      generated_at TEXT,
      sent INTEGER DEFAULT 0,
      failed INTEGER DEFAULT 0,
      recipients INTEGER DEFAULT 0,
      payload_json TEXT
    );

    CREATE TABLE IF NOT EXISTS email_event_logs (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      email TEXT NOT NULL,
      event_type TEXT NOT NULL,
      payload_json TEXT,
      created_at TEXT NOT NULL
    );
    CREATE UNIQUE INDEX IF NOT EXISTS idx_email_event_unique ON email_event_logs(email, event_type);

    CREATE TABLE IF NOT EXISTS email_delivery_logs (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      email TEXT NOT NULL,
      report_date TEXT,
      email_type TEXT NOT NULL,
      status TEXT NOT NULL,
      detail TEXT,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    );
    CREATE UNIQUE INDEX IF NOT EXISTS idx_email_delivery_unique ON email_delivery_logs(email, report_date, email_type);

    CREATE TABLE IF NOT EXISTS monitor_page_events (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      path TEXT,
      referrer TEXT,
      user_agent TEXT,
      ip TEXT,
      visited_at TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_monitor_page_events_time ON monitor_page_events(visited_at);
    CREATE INDEX IF NOT EXISTS idx_monitor_page_events_path ON monitor_page_events(path);

    CREATE TABLE IF NOT EXISTS monitor_token_usage (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      source TEXT,
      model TEXT,
      input_tokens INTEGER DEFAULT 0,
      output_tokens INTEGER DEFAULT 0,
      total_tokens INTEGER DEFAULT 0,
      meta_json TEXT,
      logged_at TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_monitor_token_usage_time ON monitor_token_usage(logged_at);

    CREATE TABLE IF NOT EXISTS api_credentials (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      service TEXT UNIQUE NOT NULL,
      api_key TEXT NOT NULL,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    );
    """
  )
  conn.commit()
  conn.close()


def replace_sheet_rows(sheet_name: str, rows, as_of: str):
  conn = get_conn()
  ts = now_iso()
  conn.execute("DELETE FROM sheet_rows WHERE sheet_name = ? AND as_of = ?", (sheet_name, as_of))
  for idx, row in enumerate(rows or [], start=1):
    conn.execute(
      "INSERT INTO sheet_rows (sheet_name, row_index, row_json, as_of, created_at) VALUES (?, ?, ?, ?, ?)",
      (sheet_name, idx, json.dumps(row, ensure_ascii=False), as_of, ts),
    )
  conn.commit()
  conn.close()


def save_model_snapshot(payload: dict):
  conn = get_conn()
  ts = now_iso()
  as_of = str(payload.get("asOf") or "")
  report_date = str(payload.get("reportDate") or as_of)
  generated_at = str(payload.get("generatedAt") or ts)
  conn.execute(
    "INSERT INTO model_snapshots (as_of, report_date, generated_at, payload_json, created_at) VALUES (?, ?, ?, ?, ?)",
    (as_of, report_date, generated_at, json.dumps(payload, ensure_ascii=False), ts),
  )
  conn.commit()
  conn.close()


def get_latest_model_snapshot():
  conn = get_conn()
  rows = conn.execute("SELECT payload_json FROM model_snapshots ORDER BY id DESC LIMIT 50").fetchall()
  conn.close()
  for row in rows:
    try:
      payload = json.loads(row["payload_json"])
    except Exception:
      continue
    if isinstance(payload, dict) and payload:
      return payload
  return None


def upsert_daily_report(report_date: str, text: str, meta: dict, report_path: str = "", payload=None):
  conn = get_conn()
  ts = now_iso()
  score = meta.get("score")
  status = meta.get("status")
  summary = meta.get("summary", "")
  payload_json = json.dumps(payload, ensure_ascii=False) if payload is not None else None
  conn.execute(
    """
    INSERT INTO daily_reports (report_date, score, status, summary, report_text, report_path, payload_json, generated_at, created_at, updated_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(report_date) DO UPDATE SET
      score=excluded.score,
      status=excluded.status,
      summary=excluded.summary,
      report_text=excluded.report_text,
      report_path=excluded.report_path,
      payload_json=excluded.payload_json,
      generated_at=excluded.generated_at,
      updated_at=excluded.updated_at
    """,
    (report_date, score, status, summary, text, report_path, payload_json, ts, ts, ts),
  )
  conn.commit()
  conn.close()


def list_daily_reports(limit: int = 200):
  conn = get_conn()
  rows = conn.execute(
    "SELECT report_date, score, status, summary, report_text, report_path, payload_json, generated_at FROM daily_reports ORDER BY report_date DESC LIMIT ?",
    (limit,),
  ).fetchall()
  conn.close()
  out = []
  for r in rows:
    payload = json.loads(r["payload_json"]) if r["payload_json"] else None
    out.append(
      {
        "date": r["report_date"],
        "meta": {"score": r["score"], "status": r["status"], "summary": r["summary"]},
        "text": r["report_text"],
        "path": r["report_path"],
        "reportPayload": payload,
        "generatedAt": r["generated_at"],
      }
    )
  return out


def get_daily_report(report_date: str):
  conn = get_conn()
  r = conn.execute(
    "SELECT report_date, score, status, summary, report_text, report_path, payload_json, generated_at FROM daily_reports WHERE report_date = ?",
    (report_date,),
  ).fetchone()
  conn.close()
  if not r:
    return None
  payload = json.loads(r["payload_json"]) if r["payload_json"] else None
  return {
    "date": r["report_date"],
    "meta": {"score": r["score"], "status": r["status"], "summary": r["summary"]},
    "text": r["report_text"],
    "path": r["report_path"],
    "reportPayload": payload,
    "generatedAt": r["generated_at"],
  }


def save_online_check(checked_at: str, summary: dict, rows):
  conn = get_conn()
  conn.execute(
    "INSERT INTO online_checks (checked_at, summary_json, rows_json) VALUES (?, ?, ?)",
    (checked_at, json.dumps(summary, ensure_ascii=False), json.dumps(rows, ensure_ascii=False)),
  )
  conn.commit()
  conn.close()


def add_subscriber(email: str, source: str = "web"):
  conn = get_conn()
  ts = now_iso()
  conn.execute(
    """
    INSERT INTO subscribers (email, status, source, created_at, updated_at)
    VALUES (?, 'active', ?, ?, ?)
    ON CONFLICT(email) DO UPDATE SET
      status='active',
      source=excluded.source,
      updated_at=excluded.updated_at
    """,
    (email.lower().strip(), source, ts, ts),
  )
  conn.commit()
  conn.close()


def list_active_subscribers():
  conn = get_conn()
  rows = conn.execute("SELECT email, created_at, updated_at FROM subscribers WHERE status='active' ORDER BY created_at ASC").fetchall()
  conn.close()
  return [dict(r) for r in rows]


def list_active_subscribers_with_status(report_date: str = ""):
  conn = get_conn()
  rows = conn.execute(
    "SELECT email, created_at, updated_at FROM subscribers WHERE status='active' ORDER BY created_at ASC"
  ).fetchall()
  out = []
  for r in rows:
    email = str(r["email"] or "").strip().lower()
    welcome_row = conn.execute(
      "SELECT created_at FROM email_event_logs WHERE email = ? AND event_type = 'welcome_sent' LIMIT 1",
      (email,),
    ).fetchone()
    latest_daily = conn.execute(
      """
      SELECT report_date, email_type, status, detail, updated_at
      FROM email_delivery_logs
      WHERE email = ? AND email_type = 'daily_report'
      ORDER BY report_date DESC, id DESC
      LIMIT 1
      """,
      (email,),
    ).fetchone()
    today_row = None
    if report_date:
      today_row = conn.execute(
        """
        SELECT report_date, email_type, status, detail, updated_at
        FROM email_delivery_logs
        WHERE email = ? AND report_date = ?
        ORDER BY id DESC
        LIMIT 1
        """,
        (email, report_date),
      ).fetchone()
    out.append(
      {
        "email": email,
        "created_at": r["created_at"],
        "updated_at": r["updated_at"],
        "welcome_email_sent": bool(welcome_row),
        "welcome_sent_at": welcome_row["created_at"] if welcome_row else "",
        "daily_report_sent_today": bool(today_row and today_row["email_type"] == "daily_report" and today_row["status"] == "sent"),
        "today_email_type": today_row["email_type"] if today_row else "",
        "today_email_status": today_row["status"] if today_row else "",
        "today_email_detail": today_row["detail"] if today_row else "",
        "today_email_updated_at": today_row["updated_at"] if today_row else "",
        "latest_daily_report_date": latest_daily["report_date"] if latest_daily else "",
        "latest_daily_report_status": latest_daily["status"] if latest_daily else "",
        "latest_daily_report_updated_at": latest_daily["updated_at"] if latest_daily else "",
      }
    )
  conn.close()
  return out


def save_email_dispatch_log(payload: dict):
  conn = get_conn()
  conn.execute(
    "INSERT INTO email_dispatch_logs (report_date, generated_at, sent, failed, recipients, payload_json) VALUES (?, ?, ?, ?, ?, ?)",
    (
      payload.get("date"),
      payload.get("generatedAt"),
      int(payload.get("sent", 0)),
      int(payload.get("failed", 0)),
      int(payload.get("recipients", 0)),
      json.dumps(payload, ensure_ascii=False),
    ),
  )
  conn.commit()
  conn.close()


def has_email_event(email: str, event_type: str):
  conn = get_conn()
  row = conn.execute(
    "SELECT 1 AS ok FROM email_event_logs WHERE email = ? AND event_type = ? LIMIT 1",
    (str(email or "").strip().lower(), str(event_type or "").strip().lower()),
  ).fetchone()
  conn.close()
  return bool(row)


def save_email_event(email: str, event_type: str, payload=None):
  conn = get_conn()
  ts = now_iso()
  conn.execute(
    """
    INSERT INTO email_event_logs (email, event_type, payload_json, created_at)
    VALUES (?, ?, ?, ?)
    ON CONFLICT(email, event_type) DO NOTHING
    """,
    (
      str(email or "").strip().lower(),
      str(event_type or "").strip().lower(),
      json.dumps(payload or {}, ensure_ascii=False),
      ts,
    ),
  )
  conn.commit()
  conn.close()


def upsert_email_delivery(email: str, report_date: str, email_type: str, status: str, detail: str = ""):
  conn = get_conn()
  ts = now_iso()
  conn.execute(
    """
    INSERT INTO email_delivery_logs (email, report_date, email_type, status, detail, created_at, updated_at)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(email, report_date, email_type) DO UPDATE SET
      status=excluded.status,
      detail=excluded.detail,
      updated_at=excluded.updated_at
    """,
    (
      str(email or "").strip().lower(),
      str(report_date or "").strip(),
      str(email_type or "").strip().lower(),
      str(status or "").strip().lower(),
      str(detail or "")[:500],
      ts,
      ts,
    ),
  )
  conn.commit()
  conn.close()


def log_page_event(path: str, referrer: str = "", user_agent: str = "", ip: str = "", visited_at: str = ""):
  conn = get_conn()
  ts = visited_at or now_iso()
  conn.execute(
    "INSERT INTO monitor_page_events (path, referrer, user_agent, ip, visited_at) VALUES (?, ?, ?, ?, ?)",
    (path, referrer, user_agent, ip, ts),
  )
  conn.commit()
  conn.close()


def log_token_usage(source: str, model: str = "", input_tokens: int = 0, output_tokens: int = 0, total_tokens: int = 0, meta=None, logged_at: str = ""):
  conn = get_conn()
  ts = logged_at or now_iso()
  payload = json.dumps(meta or {}, ensure_ascii=False)
  conn.execute(
    """
    INSERT INTO monitor_token_usage (source, model, input_tokens, output_tokens, total_tokens, meta_json, logged_at)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    """,
    (source, model, int(input_tokens or 0), int(output_tokens or 0), int(total_tokens or 0), payload, ts),
  )
  conn.commit()
  conn.close()


def delete_token_usage_by_source(source: str, start_iso: str = "", end_iso: str = ""):
  conn = get_conn()
  if start_iso and end_iso:
    conn.execute(
      "DELETE FROM monitor_token_usage WHERE source = ? AND logged_at >= ? AND logged_at < ?",
      (source, start_iso, end_iso),
    )
  elif start_iso:
    conn.execute(
      "DELETE FROM monitor_token_usage WHERE source = ? AND logged_at >= ?",
      (source, start_iso),
    )
  else:
    conn.execute("DELETE FROM monitor_token_usage WHERE source = ?", (source,))
  conn.commit()
  conn.close()


def upsert_api_key(service: str, api_key: str):
  conn = get_conn()
  ts = now_iso()
  conn.execute(
    """
    INSERT INTO api_credentials (service, api_key, created_at, updated_at)
    VALUES (?, ?, ?, ?)
    ON CONFLICT(service) DO UPDATE SET
      api_key=excluded.api_key,
      updated_at=excluded.updated_at
    """,
    (str(service or "").strip().lower(), str(api_key or "").strip(), ts, ts),
  )
  conn.commit()
  conn.close()


def get_api_key(service: str):
  conn = get_conn()
  row = conn.execute(
    "SELECT api_key FROM api_credentials WHERE service = ? LIMIT 1",
    (str(service or "").strip().lower(),),
  ).fetchone()
  conn.close()
  if not row:
    return ""
  return str(row["api_key"] or "").strip()


def get_page_visit_daily(days: int = 30):
  conn = get_conn()
  rows = conn.execute(
    """
    SELECT substr(visited_at,1,10) AS day, COUNT(*) AS visits
    FROM monitor_page_events
    WHERE visited_at >= datetime('now', ?)
    GROUP BY day
    ORDER BY day ASC
    """,
    (f"-{max(1,int(days))} day",),
  ).fetchall()
  conn.close()
  return [dict(r) for r in rows]


def get_page_visit_by_path(days: int = 30, limit: int = 50):
  conn = get_conn()
  rows = conn.execute(
    """
    SELECT path, COUNT(*) AS visits
    FROM monitor_page_events
    WHERE visited_at >= datetime('now', ?)
    GROUP BY path
    ORDER BY visits DESC
    LIMIT ?
    """,
    (f"-{max(1,int(days))} day", max(1, int(limit))),
  ).fetchall()
  conn.close()
  return [dict(r) for r in rows]


def get_token_usage_daily(days: int = 30):
  conn = get_conn()
  rows = conn.execute(
    """
    SELECT substr(logged_at,1,10) AS day,
           SUM(input_tokens) AS input_tokens,
           SUM(output_tokens) AS output_tokens,
           SUM(total_tokens) AS total_tokens
    FROM monitor_token_usage
    WHERE logged_at >= datetime('now', ?)
    GROUP BY day
    ORDER BY day ASC
    """,
    (f"-{max(1,int(days))} day",),
  ).fetchall()
  conn.close()
  return [dict(r) for r in rows]
