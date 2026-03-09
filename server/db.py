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
      ai_short_summary TEXT,
      ai_detailed_interpretation TEXT,
      ai_short_summary_zh TEXT,
      ai_short_summary_en TEXT,
      ai_detailed_interpretation_zh TEXT,
      ai_detailed_interpretation_en TEXT,
      ai_model TEXT,
      ai_status TEXT,
      ai_generated_at TEXT,
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

    CREATE TABLE IF NOT EXISTS daily_report_ai_insights (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      report_date TEXT UNIQUE NOT NULL,
      short_summary TEXT,
      detailed_text TEXT,
      insight_json TEXT,
      status TEXT,
      model TEXT,
      prompt_version TEXT,
      generated_at TEXT,
      error TEXT,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_daily_report_ai_insights_date ON daily_report_ai_insights(report_date);

    CREATE TABLE IF NOT EXISTS openrouter_fetch_runs (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      view TEXT NOT NULL,
      category TEXT NOT NULL,
      source_url TEXT,
      parse_mode TEXT,
      fetched_at TEXT,
      created_at TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_openrouter_fetch_runs_vc ON openrouter_fetch_runs(view, category, created_at DESC);

    CREATE TABLE IF NOT EXISTS openrouter_top_models (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      fetch_run_id INTEGER NOT NULL,
      rank_num INTEGER,
      name TEXT,
      creator TEXT,
      tokens TEXT,
      share TEXT,
      created_at TEXT NOT NULL,
      FOREIGN KEY(fetch_run_id) REFERENCES openrouter_fetch_runs(id) ON DELETE CASCADE
    );
    CREATE INDEX IF NOT EXISTS idx_openrouter_top_models_run ON openrouter_top_models(fetch_run_id, rank_num);

    CREATE TABLE IF NOT EXISTS openrouter_top_apps (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      fetch_run_id INTEGER NOT NULL,
      rank_num INTEGER,
      name TEXT,
      creator TEXT,
      tokens TEXT,
      share TEXT,
      created_at TEXT NOT NULL,
      FOREIGN KEY(fetch_run_id) REFERENCES openrouter_fetch_runs(id) ON DELETE CASCADE
    );
    CREATE INDEX IF NOT EXISTS idx_openrouter_top_apps_run ON openrouter_top_apps(fetch_run_id, rank_num);

    CREATE TABLE IF NOT EXISTS openrouter_top_providers (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      fetch_run_id INTEGER NOT NULL,
      rank_num INTEGER,
      name TEXT,
      creator TEXT,
      tokens TEXT,
      share TEXT,
      created_at TEXT NOT NULL,
      FOREIGN KEY(fetch_run_id) REFERENCES openrouter_fetch_runs(id) ON DELETE CASCADE
    );
    CREATE INDEX IF NOT EXISTS idx_openrouter_top_providers_run ON openrouter_top_providers(fetch_run_id, rank_num);

    CREATE TABLE IF NOT EXISTS openrouter_top_prompts (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      fetch_run_id INTEGER NOT NULL,
      rank_num INTEGER,
      name TEXT,
      creator TEXT,
      tokens TEXT,
      share TEXT,
      created_at TEXT NOT NULL,
      FOREIGN KEY(fetch_run_id) REFERENCES openrouter_fetch_runs(id) ON DELETE CASCADE
    );
    CREATE INDEX IF NOT EXISTS idx_openrouter_top_prompts_run ON openrouter_top_prompts(fetch_run_id, rank_num);
    """
  )
  for ddl in [
    "ALTER TABLE daily_reports ADD COLUMN ai_short_summary TEXT",
    "ALTER TABLE daily_reports ADD COLUMN ai_detailed_interpretation TEXT",
    "ALTER TABLE daily_reports ADD COLUMN ai_short_summary_zh TEXT",
    "ALTER TABLE daily_reports ADD COLUMN ai_short_summary_en TEXT",
    "ALTER TABLE daily_reports ADD COLUMN ai_detailed_interpretation_zh TEXT",
    "ALTER TABLE daily_reports ADD COLUMN ai_detailed_interpretation_en TEXT",
    "ALTER TABLE daily_reports ADD COLUMN ai_model TEXT",
    "ALTER TABLE daily_reports ADD COLUMN ai_status TEXT",
    "ALTER TABLE daily_reports ADD COLUMN ai_generated_at TEXT",
  ]:
    try:
      conn.execute(ddl)
    except sqlite3.OperationalError:
      pass
  conn.commit()
  conn.close()


def upsert_openrouter_rankings_snapshot(payload: dict):
  conn = get_conn()
  ts = now_iso()
  view = str((payload or {}).get("view") or "week").strip().lower() or "week"
  category = str((payload or {}).get("category") or "all").strip().lower() or "all"
  source_url = str((payload or {}).get("sourceUrl") or "")
  parse_mode = str((payload or {}).get("parseMode") or "")
  fetched_at = str((payload or {}).get("fetchedAt") or ts)

  cur = conn.execute(
    """
    INSERT INTO openrouter_fetch_runs (view, category, source_url, parse_mode, fetched_at, created_at)
    VALUES (?, ?, ?, ?, ?, ?)
    """,
    (view, category, source_url, parse_mode, fetched_at, ts),
  )
  run_id = int(cur.lastrowid or 0)

  def _insert_rows(table_name: str, rows):
    for r in rows or []:
      conn.execute(
        f"""
        INSERT INTO {table_name}
        (fetch_run_id, rank_num, name, creator, tokens, share, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
          run_id,
          int(r.get("rank") or 0),
          str(r.get("name") or ""),
          str(r.get("creator") or ""),
          str(r.get("tokens") or ""),
          str(r.get("share") or ""),
          ts,
        ),
      )

  _insert_rows("openrouter_top_models", (payload or {}).get("models") or [])
  _insert_rows("openrouter_top_apps", (payload or {}).get("apps") or [])
  _insert_rows("openrouter_top_providers", (payload or {}).get("providers") or [])
  _insert_rows("openrouter_top_prompts", (payload or {}).get("prompts") or [])

  conn.commit()
  conn.close()
  return run_id


def get_latest_openrouter_rankings_snapshot(view: str = "week", category: str = "all"):
  conn = get_conn()
  v = str(view or "week").strip().lower() or "week"
  c = str(category or "all").strip().lower() or "all"
  run = conn.execute(
    """
    SELECT id, view, category, source_url, parse_mode, fetched_at
    FROM openrouter_fetch_runs
    WHERE view = ? AND category = ?
    ORDER BY datetime(replace(replace(fetched_at, 'T', ' '), 'Z', '')) DESC, id DESC
    LIMIT 1
    """,
    (v, c),
  ).fetchone()
  if not run:
    conn.close()
    return None
  run_id = int(run["id"])

  def _rows(table_name: str):
    rows = conn.execute(
      f"""
      SELECT rank_num, name, creator, tokens, share
      FROM {table_name}
      WHERE fetch_run_id = ?
      ORDER BY rank_num ASC, id ASC
      """,
      (run_id,),
    ).fetchall()
    return [
      {
        "rank": int(r["rank_num"] or 0),
        "name": str(r["name"] or ""),
        "creator": str(r["creator"] or ""),
        "tokens": str(r["tokens"] or ""),
        "share": str(r["share"] or ""),
      }
      for r in rows
    ]

  payload = {
    "ok": True,
    "sourceUrl": str(run["source_url"] or ""),
    "parseMode": str(run["parse_mode"] or ""),
    "view": str(run["view"] or v),
    "category": str(run["category"] or c),
    "fetchedAt": str(run["fetched_at"] or ""),
    "models": _rows("openrouter_top_models"),
    "apps": _rows("openrouter_top_apps"),
    "providers": _rows("openrouter_top_providers"),
    "prompts": _rows("openrouter_top_prompts"),
  }
  conn.close()
  return payload


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


def upsert_daily_report(report_date: str, text: str, meta: dict, report_path: str = "", payload=None, ai_analysis=None):
  conn = get_conn()
  ts = now_iso()
  existing = conn.execute(
    """
    SELECT score, status, summary, report_path, payload_json,
           ai_short_summary, ai_detailed_interpretation,
           ai_short_summary_zh, ai_short_summary_en,
           ai_detailed_interpretation_zh, ai_detailed_interpretation_en,
           ai_model, ai_status, ai_generated_at
    FROM daily_reports
    WHERE report_date = ?
    """,
    (str(report_date or "").strip(),),
  ).fetchone()
  score = meta.get("score")
  status = meta.get("status")
  summary = meta.get("summary", "")
  ai = ai_analysis if isinstance(ai_analysis, dict) else {}
  has_new_ai = any(
    str(ai.get(k) or "").strip()
    for k in (
      "short_summary",
      "detailed_interpretation",
      "short_summary_zh",
      "short_summary_en",
      "detailed_interpretation_zh",
      "detailed_interpretation_en",
      "model",
      "status",
      "generated_at",
    )
  )
  if has_new_ai:
    ai_short_zh = str(ai.get("short_summary_zh") or "")
    ai_short_en = str(ai.get("short_summary_en") or "")
    ai_detail_zh = str(ai.get("detailed_interpretation_zh") or "")
    ai_detail_en = str(ai.get("detailed_interpretation_en") or "")
    ai_short = str(ai.get("short_summary") or ai_short_zh or ai_short_en or "")
    ai_detail = str(ai.get("detailed_interpretation") or ai_detail_zh or ai_detail_en or "")
    ai_model = str(ai.get("model") or "")
    ai_status = str(ai.get("status") or "")
    ai_generated_at = str(ai.get("generated_at") or "")
  else:
    ai_short = str((existing["ai_short_summary"] if existing else "") or "")
    ai_detail = str((existing["ai_detailed_interpretation"] if existing else "") or "")
    ai_short_zh = str((existing["ai_short_summary_zh"] if existing else "") or "")
    ai_short_en = str((existing["ai_short_summary_en"] if existing else "") or "")
    ai_detail_zh = str((existing["ai_detailed_interpretation_zh"] if existing else "") or "")
    ai_detail_en = str((existing["ai_detailed_interpretation_en"] if existing else "") or "")
    ai_model = str((existing["ai_model"] if existing else "") or "")
    ai_status = str((existing["ai_status"] if existing else "") or "")
    ai_generated_at = str((existing["ai_generated_at"] if existing else "") or "")
  if score is None and existing:
    score = existing["score"]
  if (status is None or str(status).strip() == "") and existing:
    status = existing["status"]
  if (summary is None or str(summary).strip() == "") and existing:
    summary = existing["summary"] or ""
  if (not report_path) and existing:
    report_path = str(existing["report_path"] or "")
  if payload is not None:
    payload_json = json.dumps(payload, ensure_ascii=False)
  else:
    payload_json = existing["payload_json"] if existing else None
  conn.execute(
    """
    INSERT INTO daily_reports
    (report_date, score, status, summary, ai_short_summary, ai_detailed_interpretation, ai_short_summary_zh, ai_short_summary_en, ai_detailed_interpretation_zh, ai_detailed_interpretation_en, ai_model, ai_status, ai_generated_at, report_text, report_path, payload_json, generated_at, created_at, updated_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(report_date) DO UPDATE SET
      score=excluded.score,
      status=excluded.status,
      summary=excluded.summary,
      ai_short_summary=excluded.ai_short_summary,
      ai_detailed_interpretation=excluded.ai_detailed_interpretation,
      ai_short_summary_zh=excluded.ai_short_summary_zh,
      ai_short_summary_en=excluded.ai_short_summary_en,
      ai_detailed_interpretation_zh=excluded.ai_detailed_interpretation_zh,
      ai_detailed_interpretation_en=excluded.ai_detailed_interpretation_en,
      ai_model=excluded.ai_model,
      ai_status=excluded.ai_status,
      ai_generated_at=excluded.ai_generated_at,
      report_text=excluded.report_text,
      report_path=excluded.report_path,
      payload_json=excluded.payload_json,
      generated_at=excluded.generated_at,
      updated_at=excluded.updated_at
    """,
    (
      report_date,
      score,
      status,
      summary,
      ai_short,
      ai_detail,
      ai_short_zh,
      ai_short_en,
      ai_detail_zh,
      ai_detail_en,
      ai_model,
      ai_status,
      ai_generated_at,
      text,
      report_path,
      payload_json,
      ts,
      ts,
      ts,
    ),
  )
  conn.commit()
  conn.close()


def list_daily_reports(limit: int = 200):
  conn = get_conn()
  rows = conn.execute(
    """
    SELECT report_date, score, status, summary,
           ai_short_summary, ai_detailed_interpretation,
           ai_short_summary_zh, ai_short_summary_en,
           ai_detailed_interpretation_zh, ai_detailed_interpretation_en,
           ai_model, ai_status, ai_generated_at,
           report_text, report_path, payload_json, generated_at
    FROM daily_reports
    ORDER BY report_date DESC LIMIT ?
    """,
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
        "aiAnalysis": {
          "short_summary": r["ai_short_summary"] or "",
          "detailed_interpretation": r["ai_detailed_interpretation"] or "",
          "short_summary_zh": r["ai_short_summary_zh"] or "",
          "short_summary_en": r["ai_short_summary_en"] or "",
          "detailed_interpretation_zh": r["ai_detailed_interpretation_zh"] or "",
          "detailed_interpretation_en": r["ai_detailed_interpretation_en"] or "",
          "model": r["ai_model"] or "",
          "status": r["ai_status"] or "",
          "generated_at": r["ai_generated_at"] or "",
        },
        "generatedAt": r["generated_at"],
      }
    )
  return out


def get_daily_report(report_date: str):
  conn = get_conn()
  r = conn.execute(
    """
    SELECT report_date, score, status, summary,
           ai_short_summary, ai_detailed_interpretation,
           ai_short_summary_zh, ai_short_summary_en,
           ai_detailed_interpretation_zh, ai_detailed_interpretation_en,
           ai_model, ai_status, ai_generated_at,
           report_text, report_path, payload_json, generated_at
    FROM daily_reports
    WHERE report_date = ?
    """,
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
    "aiAnalysis": {
      "short_summary": r["ai_short_summary"] or "",
      "detailed_interpretation": r["ai_detailed_interpretation"] or "",
      "short_summary_zh": r["ai_short_summary_zh"] or "",
      "short_summary_en": r["ai_short_summary_en"] or "",
      "detailed_interpretation_zh": r["ai_detailed_interpretation_zh"] or "",
      "detailed_interpretation_en": r["ai_detailed_interpretation_en"] or "",
      "model": r["ai_model"] or "",
      "status": r["ai_status"] or "",
      "generated_at": r["ai_generated_at"] or "",
    },
    "generatedAt": r["generated_at"],
  }


def update_daily_report_analysis(
  report_date: str,
  short_summary: str = "",
  detailed_interpretation: str = "",
  short_summary_zh: str = "",
  short_summary_en: str = "",
  detailed_interpretation_zh: str = "",
  detailed_interpretation_en: str = "",
  model: str = "",
  status: str = "",
  generated_at: str = "",
):
  conn = get_conn()
  ts = now_iso()
  ss_zh = str(short_summary_zh or "")
  ss_en = str(short_summary_en or "")
  di_zh = str(detailed_interpretation_zh or "")
  di_en = str(detailed_interpretation_en or "")
  ss = str(short_summary or ss_zh or ss_en or "")
  di = str(detailed_interpretation or di_zh or di_en or "")
  conn.execute(
    """
    UPDATE daily_reports
    SET ai_short_summary = ?,
        ai_detailed_interpretation = ?,
        ai_short_summary_zh = ?,
        ai_short_summary_en = ?,
        ai_detailed_interpretation_zh = ?,
        ai_detailed_interpretation_en = ?,
        ai_model = ?,
        ai_status = ?,
        ai_generated_at = ?,
        updated_at = ?
    WHERE report_date = ?
    """,
    (
      ss,
      di,
      ss_zh,
      ss_en,
      di_zh,
      di_en,
      str(model or ""),
      str(status or ""),
      str(generated_at or ""),
      ts,
      str(report_date or "").strip(),
    ),
  )
  conn.commit()
  conn.close()


def get_daily_report_analysis(report_date: str):
  conn = get_conn()
  row = conn.execute(
    """
    SELECT report_date, ai_short_summary, ai_detailed_interpretation,
           ai_short_summary_zh, ai_short_summary_en,
           ai_detailed_interpretation_zh, ai_detailed_interpretation_en,
           ai_model, ai_status, ai_generated_at, updated_at
    FROM daily_reports
    WHERE report_date = ?
    LIMIT 1
    """,
    (str(report_date or "").strip(),),
  ).fetchone()
  conn.close()
  if not row:
    return None
  return {
    "report_date": row["report_date"],
    "short_summary": row["ai_short_summary"] or "",
    "detailed_interpretation": row["ai_detailed_interpretation"] or "",
    "short_summary_zh": row["ai_short_summary_zh"] or "",
    "short_summary_en": row["ai_short_summary_en"] or "",
    "detailed_interpretation_zh": row["ai_detailed_interpretation_zh"] or "",
    "detailed_interpretation_en": row["ai_detailed_interpretation_en"] or "",
    "model": row["ai_model"] or "",
    "status": row["ai_status"] or "",
    "generated_at": row["ai_generated_at"] or "",
    "updated_at": row["updated_at"] or "",
  }


def save_online_check(checked_at: str, summary: dict, rows):
  conn = get_conn()
  conn.execute(
    "INSERT INTO online_checks (checked_at, summary_json, rows_json) VALUES (?, ?, ?)",
    (checked_at, json.dumps(summary, ensure_ascii=False), json.dumps(rows, ensure_ascii=False)),
  )
  conn.commit()
  conn.close()


def get_latest_online_check():
  conn = get_conn()
  row = conn.execute(
    """
    SELECT checked_at, summary_json, rows_json
    FROM online_checks
    ORDER BY checked_at DESC, id DESC
    LIMIT 1
    """
  ).fetchone()
  conn.close()
  if not row:
    return None
  try:
    summary = json.loads(row["summary_json"] or "{}")
  except Exception:
    summary = {}
  try:
    rows = json.loads(row["rows_json"] or "[]")
  except Exception:
    rows = []
  return {
    "checkedAt": row["checked_at"] or "",
    "summary": summary,
    "rows": rows,
  }


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


def deactivate_subscriber(email: str):
  conn = get_conn()
  ts = now_iso()
  conn.execute(
    "UPDATE subscribers SET status='inactive', updated_at=? WHERE email=?",
    (ts, str(email or "").strip().lower()),
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


def upsert_daily_report_ai_insight(
  report_date: str,
  short_summary: str = "",
  detailed_text: str = "",
  insight=None,
  status: str = "ok",
  model: str = "",
  prompt_version: str = "",
  generated_at: str = "",
  error: str = "",
):
  conn = get_conn()
  ts = now_iso()
  insight_json = json.dumps(insight or {}, ensure_ascii=False)
  conn.execute(
    """
    INSERT INTO daily_report_ai_insights
    (report_date, short_summary, detailed_text, insight_json, status, model, prompt_version, generated_at, error, created_at, updated_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(report_date) DO UPDATE SET
      short_summary=excluded.short_summary,
      detailed_text=excluded.detailed_text,
      insight_json=excluded.insight_json,
      status=excluded.status,
      model=excluded.model,
      prompt_version=excluded.prompt_version,
      generated_at=excluded.generated_at,
      error=excluded.error,
      updated_at=excluded.updated_at
    """,
    (
      str(report_date or "").strip(),
      str(short_summary or ""),
      str(detailed_text or ""),
      insight_json,
      str(status or "").strip() or "ok",
      str(model or "").strip(),
      str(prompt_version or "").strip(),
      str(generated_at or "").strip() or ts,
      str(error or "")[:500],
      ts,
      ts,
    ),
  )
  conn.commit()
  conn.close()


def get_daily_report_ai_insight(report_date: str):
  conn = get_conn()
  row = conn.execute(
    """
    SELECT report_date, short_summary, detailed_text, insight_json, status, model, prompt_version, generated_at, error
    FROM daily_report_ai_insights
    WHERE report_date = ?
    LIMIT 1
    """,
    (str(report_date or "").strip(),),
  ).fetchone()
  conn.close()
  if not row:
    return None
  insight = {}
  try:
    insight = json.loads(row["insight_json"]) if row["insight_json"] else {}
  except Exception:
    insight = {}
  return {
    "report_date": row["report_date"],
    "short_summary": row["short_summary"] or "",
    "detailed_text": row["detailed_text"] or "",
    "insight": insight,
    "status": row["status"] or "",
    "model": row["model"] or "",
    "prompt_version": row["prompt_version"] or "",
    "generated_at": row["generated_at"] or "",
    "error": row["error"] or "",
  }


def get_page_visit_daily(days: int = 30):
  conn = get_conn()
  rows = conn.execute(
    """
    SELECT substr(visited_at,1,10) AS day, COUNT(*) AS visits
    FROM monitor_page_events
    WHERE datetime(replace(replace(visited_at, 'T', ' '), 'Z', '')) >= datetime('now', ?)
    GROUP BY day
    ORDER BY day ASC
    """,
    (f"-{max(1,int(days))} day",),
  ).fetchall()
  conn.close()
  return [dict(r) for r in rows]


def get_page_visit_minute(minutes: int = 180):
  conn = get_conn()
  rows = conn.execute(
    """
    SELECT strftime('%Y-%m-%d %H:%M', datetime(replace(replace(visited_at, 'T', ' '), 'Z', ''))) AS minute,
           COUNT(*) AS visits
    FROM monitor_page_events
    WHERE datetime(replace(replace(visited_at, 'T', ' '), 'Z', '')) >= datetime('now', ?)
    GROUP BY minute
    ORDER BY minute ASC
    """,
    (f"-{max(1,int(minutes))} minute",),
  ).fetchall()
  conn.close()
  return [dict(r) for r in rows]


def get_page_visit_by_path(days: int = 30, limit: int = 50):
  conn = get_conn()
  rows = conn.execute(
    """
    SELECT path, COUNT(*) AS visits
    FROM monitor_page_events
    WHERE datetime(replace(replace(visited_at, 'T', ' '), 'Z', '')) >= datetime('now', ?)
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
    WHERE datetime(replace(replace(logged_at, 'T', ' '), 'Z', '')) >= datetime('now', ?)
    GROUP BY day
    ORDER BY day ASC
    """,
    (f"-{max(1,int(days))} day",),
  ).fetchall()
  conn.close()
  return [dict(r) for r in rows]


def get_token_usage_minute(minutes: int = 180):
  conn = get_conn()
  rows = conn.execute(
    """
    SELECT strftime('%Y-%m-%d %H:%M', datetime(replace(replace(logged_at, 'T', ' '), 'Z', ''))) AS minute,
           SUM(input_tokens) AS input_tokens,
           SUM(output_tokens) AS output_tokens,
           SUM(total_tokens) AS total_tokens
    FROM monitor_token_usage
    WHERE datetime(replace(replace(logged_at, 'T', ' '), 'Z', '')) >= datetime('now', ?)
    GROUP BY minute
    ORDER BY minute ASC
    """,
    (f"-{max(1,int(minutes))} minute",),
  ).fetchall()
  conn.close()
  return [dict(r) for r in rows]
