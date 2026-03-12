#!/usr/bin/env python3
import os
import re
import time
import json
import html
import hashlib
import base64
import secrets
import urllib.parse
import urllib.request
import urllib.error
from threading import Lock
from pathlib import Path
from datetime import datetime, timedelta, timezone

from flask import Flask, jsonify, request, send_from_directory, redirect, session

try:
  from .db import (
    add_subscriber,
    deactivate_subscriber,
    get_daily_report,
    get_daily_report_analysis,
    get_latest_model_snapshot,
    get_latest_openrouter_rankings_snapshot,
    init_db,
    list_active_subscribers,
    list_active_subscribers_with_status,
    list_daily_reports,
    get_page_visit_daily,
    get_page_visit_minute,
    get_page_visit_by_path,
    get_monitor_totals_all_time,
    get_token_usage_daily,
    get_token_usage_minute,
    get_daily_report_ai_insight,
    get_latest_online_check,
    has_email_event,
    log_page_event,
    log_token_usage,
    save_email_event,
    save_model_snapshot,
    save_online_check,
    update_daily_report_analysis,
    upsert_daily_report_ai_insight,
    upsert_daily_report,
    upsert_openrouter_rankings_snapshot,
    now_iso,
  )
  from .mailer import send_email
  from .stock_service import (
    import_csv_uploads,
    train_and_refresh_ticker,
    list_tickers as list_stock_tickers,
    get_ticker_profile as get_stock_ticker_profile,
    get_latest_prediction_payload,
    get_backtest_summary as get_stock_backtest_summary,
    get_backtest_history as get_stock_backtest_history,
    get_latest_features as get_stock_latest_features,
    get_admin_data_status as get_stock_admin_data_status,
    get_ticker_admin_status as get_stock_ticker_admin_status,
    list_upload_history as list_stock_upload_history,
    get_stock_form_rows,
    list_model_runs as list_stock_model_runs,
    inspect_csv_uploads,
  )
except ImportError:
  from db import (
    add_subscriber,
    deactivate_subscriber,
    get_daily_report,
    get_daily_report_analysis,
    get_latest_model_snapshot,
    get_latest_openrouter_rankings_snapshot,
    init_db,
    list_active_subscribers,
    list_active_subscribers_with_status,
    list_daily_reports,
    get_page_visit_daily,
    get_page_visit_minute,
    get_page_visit_by_path,
    get_monitor_totals_all_time,
    get_token_usage_daily,
    get_token_usage_minute,
    get_daily_report_ai_insight,
    get_latest_online_check,
    has_email_event,
    log_page_event,
    log_token_usage,
    save_email_event,
    save_model_snapshot,
    save_online_check,
    update_daily_report_analysis,
    upsert_daily_report_ai_insight,
    upsert_daily_report,
    upsert_openrouter_rankings_snapshot,
    now_iso,
  )
  from mailer import send_email
  from stock_service import (
    import_csv_uploads,
    train_and_refresh_ticker,
    list_tickers as list_stock_tickers,
    get_ticker_profile as get_stock_ticker_profile,
    get_latest_prediction_payload,
    get_backtest_summary as get_stock_backtest_summary,
    get_backtest_history as get_stock_backtest_history,
    get_latest_features as get_stock_latest_features,
    get_admin_data_status as get_stock_admin_data_status,
    get_ticker_admin_status as get_stock_ticker_admin_status,
    list_upload_history as list_stock_upload_history,
    get_stock_form_rows,
    list_model_runs as list_stock_model_runs,
    inspect_csv_uploads,
  )

ROOT = Path(__file__).resolve().parents[1]
EMAIL_RE = re.compile(r"^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$", re.I)
CACHE_TTL_SECONDS = int(os.environ.get("API_CACHE_TTL_SECONDS", "90"))
MONITOR_ALLOWED_EMAILS = {x.strip().lower() for x in str(os.environ.get("MONITOR_ALLOWED_EMAILS", "")).split(",") if x.strip()}
MONITOR_ALLOWED_DOMAINS = {x.strip().lower() for x in str(os.environ.get("MONITOR_ALLOWED_DOMAINS", "")).split(",") if x.strip()}
MONITOR_FIXED_WHITELIST = {"xiayiping@gmail.com", "simobot001@gmail.com"}
GOOGLE_CLIENT_ID = str(os.environ.get("GOOGLE_OAUTH_CLIENT_ID", "")).strip()
GOOGLE_CLIENT_SECRET = str(os.environ.get("GOOGLE_OAUTH_CLIENT_SECRET", "")).strip()
GOOGLE_REDIRECT_URI = str(os.environ.get("GOOGLE_OAUTH_REDIRECT_URI", "https://monitor.nexo.hk/monitor-api/auth/google/callback")).strip()
GOOGLE_AUTH_URL = "https://accounts.google.com/o/oauth2/v2/auth"
GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
GOOGLE_USERINFO_URL = "https://openidconnect.googleapis.com/v1/userinfo"
OPENAI_API_KEY = str(os.environ.get("OPENAI_API_KEY", "")).strip()
OPENAI_MODEL = str(os.environ.get("OPENAI_MODEL", "gpt-5.4")).strip()
OPENAI_BASE_URL = str(os.environ.get("OPENAI_BASE_URL", "https://api.openai.com/v1")).strip().rstrip("/")
OPENAI_MAX_RETRIES = max(1, int(os.environ.get("OPENAI_MAX_RETRIES", "3")))
OPENAI_FORCE_PRIMARY = str(os.environ.get("OPENAI_FORCE_PRIMARY", "true")).strip().lower() in {"1", "true", "yes", "on"}
OPENAI_MIN_INTERVAL_SEC = max(0.0, float(os.environ.get("OPENAI_MIN_INTERVAL_SEC", "3.0")))
OPENAI_MAX_QUEUE_WAIT_SEC = max(1.0, float(os.environ.get("OPENAI_MAX_QUEUE_WAIT_SEC", "20.0")))
OPENROUTER_USE_PLAYWRIGHT = str(os.environ.get("OPENROUTER_USE_PLAYWRIGHT", "true")).strip().lower() in {"1", "true", "yes", "on"}

try:
  from playwright.sync_api import sync_playwright
except Exception:
  sync_playwright = None

app = Flask(__name__, static_folder=str(ROOT), static_url_path="")
app.secret_key = os.environ.get("MONITOR_SESSION_SECRET") or base64.urlsafe_b64encode(os.urandom(32)).decode("ascii")
app.config.update(
  SESSION_COOKIE_HTTPONLY=True,
  SESSION_COOKIE_SAMESITE="Lax",
  SESSION_COOKIE_SECURE=True,
  MAX_CONTENT_LENGTH=max(1, int(os.environ.get("MAX_UPLOAD_MB", "100"))) * 1024 * 1024,
)
init_db()
_cache = {}
_cache_lock = Lock()
_openai_queue_lock = Lock()
_openai_next_allowed_at = 0.0


@app.errorhandler(413)
def handle_payload_too_large(_err):
  return jsonify({"ok": False, "error": "payload_too_large", "detail": "request body exceeds upload limit"}), 413


def _cache_get(key):
  now = time.time()
  with _cache_lock:
    item = _cache.get(key)
    if not item:
      return None
    if now - item["ts"] > CACHE_TTL_SECONDS:
      _cache.pop(key, None)
      return None
    return item["value"]


def _cache_set(key, value):
  with _cache_lock:
    _cache[key] = {"ts": time.time(), "value": value}
  return value


def _invalidate_cache(*prefixes):
  if not prefixes:
    return
  with _cache_lock:
    for key in list(_cache.keys()):
      if any(key.startswith(p) for p in prefixes):
        _cache.pop(key, None)


def _build_model_summary(model, latest_report=None):
  if not isinstance(model, dict):
    return {"error": "not_found"}
  dimensions = model.get("dimensions") or []
  alerts = model.get("triggerAlerts") or model.get("alerts") or []
  top_contributors = model.get("topDimensionContributors") or sorted(
    dimensions,
    key=lambda x: float(x.get("contribution") or 0),
    reverse=True,
  )[:5]
  watch_items = model.get("dailyWatchedItems") or model.get("keyWatch") or []
  key_indicators = model.get("keyIndicatorsSnapshot") or []
  primary_drivers = model.get("primaryDrivers") or model.get("drivers") or []
  report_meta = latest_report.get("meta") if isinstance(latest_report, dict) else {}
  report_date = latest_report.get("date") if isinstance(latest_report, dict) else ""
  report_ai = latest_report.get("aiAnalysis") if isinstance(latest_report, dict) else {}
  ai = get_daily_report_ai_insight(report_date) if report_date else None
  short_ai = (
    (report_ai or {}).get("short_summary")
    or ((ai or {}).get("short_summary") if isinstance(ai, dict) else "")
  )

  return {
    "asOf": model.get("asOf") or "",
    "totalScore": model.get("totalScore") or 0,
    "status": model.get("status") or "",
    "alerts": alerts,
    "topDimensionContributors": top_contributors,
    "primaryDrivers": primary_drivers,
    "keyIndicatorsSnapshot": key_indicators,
    "dailyWatchedItems": watch_items,
    "latestReportSummary": short_ai or model.get("latestReportSummary") or report_meta.get("summary") or "",
    "latestReportDate": report_date or "",
    "latestReportAiInsight": report_ai or ai or None,
    "generatedAt": model.get("generatedAt") or "",
  }


def _build_model_core(model):
  if not isinstance(model, dict):
    return {"error": "not_found"}
  return {
    "asOf": model.get("asOf") or "",
    "totalScore": model.get("totalScore") or 0,
    "status": model.get("status") or "",
    "alerts": model.get("triggerAlerts") or model.get("alerts") or [],
    "dimensions": model.get("dimensions") or [],
    "drivers": model.get("primaryDrivers") or model.get("drivers") or [],
    "keyIndicatorsSnapshot": model.get("keyIndicatorsSnapshot") or [],
    "topDimensionContributors": model.get("topDimensionContributors") or [],
    "dailyWatchedItems": model.get("dailyWatchedItems") or model.get("keyWatch") or [],
    "latestReportSummary": model.get("latestReportSummary") or "",
    "reportDate": model.get("reportDate") or "",
    "generatedAt": model.get("generatedAt") or "",
  }


def _build_model_tables(model):
  if not isinstance(model, dict):
    return {"error": "not_found"}
  tables = model.get("tables") or {}
  return {
    "asOf": model.get("asOf") or "",
    "tables": {
      "dimensions": tables.get("dimensions") or [],
      "inputs": tables.get("inputs") or [],
      "indicators": tables.get("indicators") or [],
      "scores": tables.get("scores") or [],
      "alerts": tables.get("alerts") or [],
    },
  }


def _build_single_table(model, table_name):
  if not isinstance(model, dict):
    return {"error": "not_found"}
  tables = model.get("tables") or {}
  return {
    "asOf": model.get("asOf") or "",
    "table": table_name,
    "rows": tables.get(table_name) or [],
  }


def _build_model_workbook(model):
  if not isinstance(model, dict):
    return {"error": "not_found"}
  workbook = model.get("workbook") or {}
  return {
    "asOf": model.get("asOf") or "",
    "workbook": {
      "sheets": workbook.get("sheets") or [],
    },
  }


def _etag_response(payload, status_code=200):
  body = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
  etag = hashlib.sha1(body.encode("utf-8")).hexdigest()
  inm = request.headers.get("If-None-Match", "").strip().strip('"')
  if inm and inm == etag:
    resp = app.response_class(status=304)
    resp.headers["ETag"] = f'"{etag}"'
    resp.headers["Cache-Control"] = "public, max-age=30"
    return resp
  resp = jsonify(payload)
  resp.status_code = status_code
  resp.headers["ETag"] = f'"{etag}"'
  resp.headers["Cache-Control"] = "public, max-age=30"
  return resp


def _estimate_tokens_from_bytes(raw: bytes) -> int:
  if not raw:
    return 0
  return max(1, (len(raw) + 3) // 4)


def _normalize_openrouter_text(raw_html: str):
  content = re.sub(r"<script[\s\S]*?</script>", " ", raw_html, flags=re.I)
  content = re.sub(r"<style[\s\S]*?</style>", " ", content, flags=re.I)
  content = re.sub(r"<[^>]+>", "\n", content)
  content = html.unescape(content)
  lines = [x.strip() for x in content.splitlines() if x and x.strip()]
  return lines


def _extract_openrouter_hidden_section(raw_html: str, section_id: str, boundary_id: str):
  start_tag = f'<div hidden id="{section_id}">'
  end_tag = f'<script>$RC("{boundary_id}","{section_id}")</script>'
  start = raw_html.find(start_tag)
  if start < 0:
    return ""
  start += len(start_tag)
  end = raw_html.find(end_tag, start)
  if end < 0:
    return ""
  return raw_html[start:end]


def _openrouter_section_to_lines(section_html: str):
  if not section_html:
    return []
  content = re.sub(r"<!--[\s\S]*?-->", "", section_html, flags=re.I)
  content = re.sub(r"<script[\s\S]*?</script>", " ", content, flags=re.I)
  content = re.sub(r"<style[\s\S]*?</style>", " ", content, flags=re.I)
  content = re.sub(r"<[^>]+>", "\n", content)
  content = html.unescape(content)
  return [x.strip() for x in content.splitlines() if x and x.strip()]


def _parse_openrouter_ranked_lines(lines, with_share: bool):
  out = []
  i = 0
  while i < len(lines):
    line = lines[i]
    m = re.match(r"^(\d+)\.$", line)
    if not m:
      i += 1
      continue
    rank = int(m.group(1))
    item = {"rank": rank, "name": "", "creator": "", "tokens": "", "share": "" if with_share else ""}
    i += 1
    prev = ""
    while i < len(lines):
      current = lines[i]
      if re.match(r"^\d+\.$", current):
        break
      low = current.lower()
      if not item["name"]:
        item["name"] = current
      elif low.startswith("by "):
        item["creator"] = current[3:].strip()
      elif low == "by" and i + 1 < len(lines):
        item["creator"] = lines[i + 1].strip()
        i += 1
      elif "token" in low and not item["tokens"]:
        item["tokens"] = current if " " in current else f"{prev} tokens".strip()
      elif with_share and re.match(r"^\d+(\.\d+)?%$", current) and not item.get("share"):
        item["share"] = current
      prev = current
      i += 1
    if item["name"]:
      out.append(item)
    if len(out) >= 50:
      break
  return out


def _parse_openrouter_section(lines, start_label: str, stop_labels, with_share: bool):
  start = -1
  for i, line in enumerate(lines):
    if line.lower() == start_label.lower():
      start = i + 1
      break
  if start < 0:
    return []
  section = []
  for i in range(start, len(lines)):
    line = lines[i]
    if any(line.lower() == s.lower() for s in stop_labels):
      break
    section.append(line)
  return _parse_openrouter_ranked_lines(section, with_share=with_share)


def _openrouter_extract_rendered_lists(page):
  payload = page.evaluate(
    """
() => {
  const normalize = (s) => (s || "").replace(/\\s+/g, " ").trim();
  const takeUnique = (arr) => {
    const seen = new Set();
    const out = [];
    for (const v of arr) {
      if (!v || seen.has(v)) continue;
      seen.add(v);
      out.push(v);
    }
    return out;
  };
  const extractRowMetrics = (el) => {
    let cur = el;
    for (let i = 0; i < 7 && cur; i += 1) {
      const txt = normalize(cur.innerText || "");
      if (/tokens?/i.test(txt) && /\\d/.test(txt)) {
        const tokenMatch = txt.match(/(\\d+(?:\\.\\d+)?\\s*[KMBT]?\\s*tokens?)/i);
        const shareMatch = txt.match(/(\\d+(?:\\.\\d+)?)%/);
        return {
          tokens: tokenMatch ? tokenMatch[1].replace(/\\s+/g, " ").trim() : "",
          share: shareMatch ? `${shareMatch[1]}%` : "",
        };
      }
      cur = cur.parentElement;
    }
    return { tokens: "", share: "" };
  };

  const appAnchors = Array.from(document.querySelectorAll('#apps a[href^="/apps?url="]'));
  const apps = appAnchors.slice(0, 20).map((a, idx) => {
    const m = extractRowMetrics(a);
    return { rank: idx + 1, name: normalize(a.textContent), creator: "", tokens: m.tokens, share: "" };
  });

  const modelIds = takeUnique(
    Array.from(document.querySelectorAll('[id]'))
      .map((el) => (el.id || "").trim())
      .filter((id) => id.includes("/") && id.length <= 80 && !id.startsWith("radix") && !id.startsWith("recharts"))
  ).slice(0, 20);
  const models = modelIds.map((id, idx) => {
    const creator = id.split("/")[0] || "";
    const namePart = id.split("/").slice(1).join("/");
    const title = namePart ? `${creator}/${namePart}` : id;
    return { rank: idx + 1, name: title, creator, tokens: "", share: "" };
  });

  const filterExcludes = new Set([
    "OpenRouter", "Models", "Chat", "Rankings", "Apps", "Enterprise", "Pricing", "Docs",
    "Sign Up", "Show more", "Intelligence Index Score", "Highest throughput",
    "This Week", "This Month", "All Time", "Today"
  ]);
  const filterButtons = takeUnique(
    Array.from(document.querySelectorAll('button'))
      .map((b) => normalize(b.textContent))
      .filter((t) => t && t.length <= 40 && !filterExcludes.has(t))
  ).slice(0, 20);
  const prompts = filterButtons.map((name, idx) => ({ rank: idx + 1, name, creator: "", tokens: "", share: "" }));

  return { apps, models, prompts };
}
"""
  ) or {}
  return {
    "apps": payload.get("apps") or [],
    "models": payload.get("models") or [],
    "prompts": payload.get("prompts") or [],
  }


def _fetch_openrouter_rankings(view: str = "week", category: str = "all"):
  allowed_views = {"day", "week", "month", "all"}
  safe_view = view if view in allowed_views else "week"
  safe_category = (category or "all").strip().lower()
  path = "/rankings" if safe_category in {"", "all"} else f"/rankings/{urllib.parse.quote(safe_category)}"
  q = urllib.parse.urlencode({"view": safe_view})
  url = f"https://openrouter.ai{path}?{q}"
  raw = ""
  parse_mode = "html-fallback"
  rendered = {"models": [], "apps": [], "prompts": []}
  if OPENROUTER_USE_PLAYWRIGHT and sync_playwright is not None:
    try:
      with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--no-sandbox"])
        page = browser.new_page()
        page.goto(url, wait_until="networkidle", timeout=120000)
        page.wait_for_timeout(6000)
        raw = page.content() or ""
        rendered = _openrouter_extract_rendered_lists(page)
        browser.close()
        parse_mode = "playwright-rendered"
    except Exception:
      raw = ""

  if not raw:
    req = urllib.request.Request(
      url,
      headers={
        "User-Agent": "Mozilla/5.0 (NexoMacroMonitor; +https://nexo.hk)",
        "Accept": "text/html,application/xhtml+xml",
      },
      method="GET",
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
      raw = resp.read().decode("utf-8", errors="ignore")

  models_hidden = _openrouter_section_to_lines(_extract_openrouter_hidden_section(raw, "S:3", "B:3"))
  apps_hidden = _openrouter_section_to_lines(_extract_openrouter_hidden_section(raw, "S:d", "B:d"))

  models = _parse_openrouter_ranked_lines(models_hidden, with_share=True) if models_hidden else []
  apps = _parse_openrouter_ranked_lines(apps_hidden, with_share=False) if apps_hidden else []
  prompts = []
  providers = []
  if rendered.get("models") and not models:
    models = rendered.get("models") or []
  if rendered.get("apps") and not apps:
    apps = rendered.get("apps") or []
  if rendered.get("prompts"):
    prompts = rendered.get("prompts") or []

  if not models or not apps or not prompts or not providers:
    lines = _normalize_openrouter_text(raw)
    if not models:
      models = _parse_openrouter_section(lines, "Top Models", ("Top Apps", "Top Prompts", "Top Providers"), with_share=True)
    if not apps:
      apps = _parse_openrouter_section(lines, "Top Apps", ("Top Prompts", "Top Providers", "API", "Developer Docs"), with_share=False)
    if not prompts:
      prompts = _parse_openrouter_section(lines, "Top Prompts", ("Top Providers", "Top Apps", "API", "Developer Docs"), with_share=True)
    providers = _parse_openrouter_section(lines, "Top Providers", ("Top Prompts", "Top Apps", "API", "Developer Docs"), with_share=True)

  if not providers:
    provider_counts = {}
    for row in models:
      creator = (row.get("creator") or "").strip().lower()
      if not creator:
        name = (row.get("name") or "").strip()
        if "/" in name:
          creator = name.split("/", 1)[0].strip().lower()
      if not creator:
        continue
      provider_counts[creator] = provider_counts.get(creator, 0) + 1
    if provider_counts:
      sorted_items = sorted(provider_counts.items(), key=lambda it: (-it[1], it[0]))
      providers = [
        {"rank": i + 1, "name": k, "creator": "", "tokens": f"{v} models", "share": ""}
        for i, (k, v) in enumerate(sorted_items[:20])
      ]

  if not providers:
    try:
      req_providers = urllib.request.Request(
        "https://openrouter.ai/api/frontend/all-providers",
        headers={"User-Agent": "Mozilla/5.0 (NexoMacroMonitor; +https://nexo.hk)"},
        method="GET",
      )
      with urllib.request.urlopen(req_providers, timeout=30) as resp:
        providers_payload = json.loads(resp.read().decode("utf-8", errors="ignore"))
      plist = (providers_payload or {}).get("data") if isinstance(providers_payload, dict) else []
      if isinstance(plist, list):
        providers = [
          {
            "rank": i + 1,
            "name": str(p.get("displayName") or p.get("name") or p.get("slug") or "").strip(),
            "creator": str(p.get("headquarters") or "").strip(),
            "tokens": "",
            "share": "",
          }
          for i, p in enumerate(plist[:20])
          if isinstance(p, dict)
        ]
    except Exception:
      pass

  if not models:
    try:
      req_models = urllib.request.Request(
        "https://openrouter.ai/api/frontend/models",
        headers={"User-Agent": "Mozilla/5.0 (NexoMacroMonitor; +https://nexo.hk)"},
        method="GET",
      )
      with urllib.request.urlopen(req_models, timeout=30) as resp:
        models_payload = json.loads(resp.read().decode("utf-8", errors="ignore"))
      mlist = (models_payload or {}).get("data") if isinstance(models_payload, dict) else []
      if isinstance(mlist, list):
        picked = []
        for item in mlist:
          if not isinstance(item, dict):
            continue
          author = str(item.get("author") or "").strip()
          short_name = str(item.get("short_name") or item.get("name") or "").strip()
          if not short_name:
            continue
          picked.append(
            {
              "rank": len(picked) + 1,
              "name": short_name,
              "creator": author,
              "tokens": "",
              "share": "",
            }
          )
          if len(picked) >= 20:
            break
        models = picked
    except Exception:
      pass

  return {
    "ok": True,
    "sourceUrl": url,
    "parseMode": parse_mode,
    "view": safe_view,
    "category": safe_category or "all",
    "fetchedAt": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
    "models": models,
    "apps": apps,
    "providers": providers,
    "prompts": prompts,
  }


def _should_autotrack_token() -> bool:
  path = request.path or ""
  if request.method == "OPTIONS":
    return False
  if path == "/monitor-api/track/token":
    return False
  return path.startswith("/api/") or path.startswith("/monitor-api/")


@app.after_request
def auto_track_request_token_usage(response):
  try:
    if not _should_autotrack_token():
      return response
    req_body = request.get_data(cache=True, as_text=False) or b""
    query_text = (request.query_string or b"").decode("utf-8", errors="ignore")
    input_bytes = req_body + query_text.encode("utf-8")
    input_tokens = _estimate_tokens_from_bytes(input_bytes)

    resp_body = response.get_data(as_text=False) or b""
    output_tokens = _estimate_tokens_from_bytes(resp_body)
    total_tokens = int(input_tokens or 0) + int(output_tokens or 0)

    log_token_usage(
      source="api_request_estimation",
      model="http-estimated",
      input_tokens=input_tokens,
      output_tokens=output_tokens,
      total_tokens=total_tokens,
      meta={
        "path": request.path,
        "method": request.method,
        "status": int(response.status_code or 0),
      },
    )
  except Exception:
    pass
  return response


def _is_monitor_authorized(email: str):
  e = str(email or "").strip().lower()
  if not e:
    return False
  # Security policy: monitor console login must be restricted to the fixed
  # two-user whitelist regardless of domain-level env configuration.
  return e in MONITOR_FIXED_WHITELIST


def _require_monitor_auth():
  user = session.get("monitor_user") or {}
  if not user.get("email"):
    return None, (jsonify({"error": "unauthorized"}), 401)
  if not _is_monitor_authorized(user.get("email")):
    return None, (jsonify({"error": "forbidden"}), 403)
  return user, None


def _json_post(url: str, payload: dict, headers=None):
  data = urllib.parse.urlencode(payload).encode("utf-8")
  req = urllib.request.Request(url, data=data, method="POST")
  req.add_header("Content-Type", "application/x-www-form-urlencoded")
  for k, v in (headers or {}).items():
    req.add_header(k, v)
  with urllib.request.urlopen(req, timeout=20) as resp:
    return json.loads(resp.read().decode("utf-8"))


def _json_get(url: str, headers=None):
  req = urllib.request.Request(url, method="GET")
  for k, v in (headers or {}).items():
    req.add_header(k, v)
  with urllib.request.urlopen(req, timeout=20) as resp:
    return json.loads(resp.read().decode("utf-8"))


def _json_post_json(url: str, payload: dict, headers=None):
  data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
  req = urllib.request.Request(url, data=data, method="POST")
  req.add_header("Content-Type", "application/json")
  for k, v in (headers or {}).items():
    req.add_header(k, v)
  with urllib.request.urlopen(req, timeout=60) as resp:
    return json.loads(resp.read().decode("utf-8"))


def _openai_wait_turn_or_raise():
  global _openai_next_allowed_at
  while True:
    with _openai_queue_lock:
      now = time.time()
      wait_sec = _openai_next_allowed_at - now
      if wait_sec <= 0:
        _openai_next_allowed_at = now + OPENAI_MIN_INTERVAL_SEC
        return
      if wait_sec > OPENAI_MAX_QUEUE_WAIT_SEC:
        raise RuntimeError(f"openai_queue_timeout wait={wait_sec:.2f}s")
    # Sleep outside lock so other requests can evaluate their own wait.
    time.sleep(min(wait_sec, 0.5))


def _extract_chat_text(resp: dict):
  if not isinstance(resp, dict):
    return ""
  choices = resp.get("choices") or []
  if not choices:
    return ""
  msg = choices[0].get("message") or {}
  return str(msg.get("content") or "").strip()


def _send_welcome_email_if_needed(email: str):
  e = str(email or "").strip().lower()
  if not e:
    return {"sent": False, "reason": "empty_email"}
  if has_email_event(e, "welcome_sent"):
    return {"sent": False, "reason": "already_sent"}
  subject = "Welcome to Nexo Marco Intelligence | 欢迎订阅 Nexo Marco Intelligence"
  html = f"""
  <div style="font-family:Arial,sans-serif;line-height:1.6;color:#10242b">
    <h2 style="margin:0 0 8px 0;">Welcome to Nexo Marco Intelligence</h2>
    <p style="margin:0 0 10px 0;">Thanks for subscribing, {e}.</p>
    <p style="margin:0 0 10px 0;">
      Nexo Marco Intelligence is a macro risk monitoring platform using a 14-dimension model to track global financial risk signals.
    </p>
    <p style="margin:0 0 14px 0;">
      You will receive the daily report every day around 09:00 China time (UTC+8).
    </p>
    <hr style="border:none;border-top:1px solid #dfe7ea;margin:14px 0;" />
    <h2 style="margin:0 0 8px 0;">欢迎订阅 Nexo Marco Intelligence</h2>
    <p style="margin:0 0 10px 0;">感谢订阅，{e}。</p>
    <p style="margin:0 0 10px 0;">
      Nexo Marco Intelligence 是一个基于 14 维模型的宏观风险监控平台，用于跟踪全球金融风险信号。
    </p>
    <p style="margin:0;">
      你将在每天中国时间早上 09:00 左右收到当日的每日报告。
    </p>
  </div>
  """
  send_email(e, subject, html)
  save_email_event(e, "welcome_sent", {"subject": subject})
  return {"sent": True}


def _build_ai_context(model: dict):
  details = model.get("indicatorDetails") or []
  failed = [d for d in details if not d.get("VerifiedOnline")]
  failed_compact = []
  for d in failed[:20]:
    failed_compact.append(
      {
        "indicatorCode": d.get("IndicatorCode"),
        "indicatorName": d.get("IndicatorName"),
        "value": d.get("LatestValue"),
        "valueDate": d.get("ValueDate"),
        "freshness": d.get("Freshness"),
        "verificationStatus": d.get("VerificationStatus"),
      }
    )
  top_dims = (model.get("topDimensionContributors") or [])[:8]
  key_ind = (model.get("keyIndicatorsSnapshot") or [])[:12]
  latest_report = None
  rep_date = str(model.get("reportDate") or model.get("asOf") or "").strip()
  if rep_date:
    latest_report = get_daily_report(rep_date)
  if not latest_report:
    rows = list_daily_reports(limit=1)
    latest_report = rows[0] if rows else None

  return {
    "asOf": model.get("asOf") or "",
    "reportDate": model.get("reportDate") or "",
    "generatedAt": model.get("generatedAt") or "",
    "totalScore": model.get("totalScore") or 0,
    "status": model.get("status") or "",
    "onlineUpdate": model.get("onlineUpdate") or {},
    "freshness": model.get("freshness") or {},
    "topDimensionContributors": top_dims,
    "keyIndicatorsSnapshot": key_ind,
    "failedIndicators": failed_compact,
    "latestReportMeta": (latest_report or {}).get("meta") if isinstance(latest_report, dict) else {},
    "latestReportDate": (latest_report or {}).get("date") if isinstance(latest_report, dict) else "",
  }


def _extract_report_date_from_question(question: str):
  text = str(question or "").strip()
  m = re.search(r"(\d{4}-\d{2}-\d{2})", text)
  if m:
    return m.group(1)
  m = re.search(r"(\d{1,2})月(\d{1,2})日?", text)
  if m:
    cn_now = datetime.now(timezone.utc) + timedelta(hours=8)
    month = int(m.group(1))
    day = int(m.group(2))
    try:
      return datetime(cn_now.year, month, day).date().isoformat()
    except Exception:
      return ""
  return ""


def _build_local_ai_answer(question: str, lang: str, context_payload: dict):
  target_date = _extract_report_date_from_question(question)
  report = None
  if target_date:
    report = get_daily_report(target_date)
  if not report:
    ref_date = str(context_payload.get("reportDate") or context_payload.get("asOf") or "").strip()
    if ref_date:
      report = get_daily_report(ref_date)
  if not report:
    latest = list_daily_reports(limit=1)
    report = latest[0] if latest else None

  total_score = context_payload.get("totalScore")
  regime = context_payload.get("status") or "--"
  failed_count = len(context_payload.get("failedIndicators") or [])
  top_dims = context_payload.get("topDimensionContributors") or []
  top_text = ", ".join([str(x.get("name") or x.get("id") or "--") for x in top_dims[:3]])

  if report:
    date_text = report.get("date") or context_payload.get("reportDate") or context_payload.get("asOf") or "--"
    meta = report.get("meta") or {}
    score_text = meta.get("score") if meta.get("score") is not None else total_score
    status_text = meta.get("status") or regime
    summary_text = str(meta.get("summary") or "").strip() or ("暂无摘要" if lang.startswith("zh") else "No summary available")
  else:
    date_text = context_payload.get("reportDate") or context_payload.get("asOf") or "--"
    score_text = total_score
    status_text = regime
    summary_text = "暂无可用日报，请先生成日报。" if lang.startswith("zh") else "No daily report is available yet. Please generate one first."

  if lang.startswith("zh"):
    return (
      f"日报日期：{date_text}\n"
      f"综合得分：{score_text}\n"
      f"状态：{status_text}\n"
      f"核心摘要：{summary_text}\n"
      f"主要贡献维度：{top_text or '--'}\n"
      f"在线校验未通过指标数：{failed_count}"
    )
  return (
    f"Report date: {date_text}\n"
    f"Composite score: {score_text}\n"
    f"Regime: {status_text}\n"
    f"Summary: {summary_text}\n"
    f"Top contributing dimensions: {top_text or '--'}\n"
    f"Failed online-verification indicators: {failed_count}"
  )


@app.after_request
def set_cors(resp):
  resp.headers["Access-Control-Allow-Origin"] = "*"
  resp.headers["Access-Control-Allow-Headers"] = "Content-Type"
  resp.headers["Access-Control-Allow-Methods"] = "GET,POST,OPTIONS"
  return resp


@app.route("/api/health", methods=["GET"])
def health():
  return _etag_response({"ok": True})


@app.route("/monitor-api/health", methods=["GET"])
def monitor_health():
  return _etag_response(
    {
      "ok": True,
      "oauthConfigured": bool(GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET and GOOGLE_REDIRECT_URI),
    }
  )


@app.route("/monitor-api/auth/google/start", methods=["GET"])
def monitor_google_start():
  if not GOOGLE_CLIENT_ID or not GOOGLE_CLIENT_SECRET or not GOOGLE_REDIRECT_URI:
    return jsonify({"error": "oauth_not_configured"}), 503
  state = secrets.token_urlsafe(24)
  session["monitor_oauth_state"] = state
  query = urllib.parse.urlencode(
    {
      "client_id": GOOGLE_CLIENT_ID,
      "redirect_uri": GOOGLE_REDIRECT_URI,
      "response_type": "code",
      "scope": "openid email profile",
      "access_type": "online",
      "prompt": "select_account",
      "state": state,
    }
  )
  return redirect(f"{GOOGLE_AUTH_URL}?{query}", code=302)


@app.route("/monitor-api/auth/google/callback", methods=["GET"])
def monitor_google_callback():
  code = str(request.args.get("code") or "").strip()
  state = str(request.args.get("state") or "").strip()
  if not code or not state or state != session.get("monitor_oauth_state"):
    return redirect("/index.html?auth=failed", code=302)
  try:
    token = _json_post(
      GOOGLE_TOKEN_URL,
      {
        "code": code,
        "client_id": GOOGLE_CLIENT_ID,
        "client_secret": GOOGLE_CLIENT_SECRET,
        "redirect_uri": GOOGLE_REDIRECT_URI,
        "grant_type": "authorization_code",
      },
    )
    access_token = token.get("access_token")
    if not access_token:
      return redirect("/index.html?auth=failed", code=302)
    userinfo = _json_get(GOOGLE_USERINFO_URL, headers={"Authorization": f"Bearer {access_token}"})
    email = str(userinfo.get("email") or "").lower().strip()
    if not email or not _is_monitor_authorized(email):
      session.pop("monitor_user", None)
      return redirect("/index.html?auth=forbidden", code=302)
    session["monitor_user"] = {
      "email": email,
      "name": userinfo.get("name") or "",
      "picture": userinfo.get("picture") or "",
      "loginAt": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    return redirect("/dashboard.html", code=302)
  except Exception:
    return redirect("/index.html?auth=failed", code=302)


@app.route("/monitor-api/auth/me", methods=["GET"])
def monitor_auth_me():
  user, err = _require_monitor_auth()
  if err:
    return err
  return _etag_response({"ok": True, "user": user})


@app.route("/monitor-api/auth/logout", methods=["POST", "OPTIONS"])
def monitor_auth_logout():
  if request.method == "OPTIONS":
    return ("", 204)
  session.pop("monitor_user", None)
  session.pop("monitor_oauth_state", None)
  return jsonify({"ok": True})


@app.route("/monitor-api/track/page", methods=["POST", "OPTIONS"])
def monitor_track_page():
  if request.method == "OPTIONS":
    return ("", 204)
  payload = request.get_json(silent=True) or {}
  path = str(payload.get("path") or request.path).strip()
  referrer = str(payload.get("referrer") or request.headers.get("Referer") or "").strip()
  user_agent = str(request.headers.get("User-Agent") or "")[:255]
  ip = str(request.headers.get("X-Forwarded-For") or request.remote_addr or "")[:128]
  if path:
    log_page_event(path=path[:500], referrer=referrer[:500], user_agent=user_agent, ip=ip)
  return jsonify({"ok": True})


@app.route("/monitor-api/track/token", methods=["POST", "OPTIONS"])
def monitor_track_token():
  if request.method == "OPTIONS":
    return ("", 204)
  user, err = _require_monitor_auth()
  if err:
    return err
  payload = request.get_json(silent=True) or {}
  source = str(payload.get("source") or "unknown")
  model = str(payload.get("model") or "")
  input_tokens = int(payload.get("input_tokens") or 0)
  output_tokens = int(payload.get("output_tokens") or 0)
  total_tokens = int(payload.get("total_tokens") or (input_tokens + output_tokens))
  meta = payload.get("meta") or {"user": user.get("email")}
  log_token_usage(source=source, model=model, input_tokens=input_tokens, output_tokens=output_tokens, total_tokens=total_tokens, meta=meta)
  return jsonify({"ok": True})


@app.route("/monitor-api/ops/overview", methods=["GET"])
def monitor_ops_overview():
  _, err = _require_monitor_auth()
  if err:
    return err
  days = int(request.args.get("days", "30"))
  minutes = int(request.args.get("minutes", "180"))
  minutes = max(5, min(minutes, 24 * 60))
  visits_daily = get_page_visit_daily(days=max(1, min(days, 365)))
  visits_minute = get_page_visit_minute(minutes=minutes)
  visits_by_path = get_page_visit_by_path(days=0, limit=30)
  tokens_daily = get_token_usage_daily(days=max(1, min(days, 365)))
  tokens_minute = get_token_usage_minute(minutes=minutes)
  totals = get_monitor_totals_all_time()
  return _etag_response(
    {
      "days": days,
      "minutes": minutes,
      "totals": totals,
      "visitsMinute": visits_minute,
      "tokensMinute": tokens_minute,
      "visitsDaily": visits_daily,
      "visitsByPath": visits_by_path,
      "tokensDaily": tokens_daily,
    }
  )


@app.route("/monitor-api/biz/subscribers", methods=["GET"])
def monitor_biz_subscribers():
  _, err = _require_monitor_auth()
  if err:
    return err
  cn_today = (datetime.now(timezone.utc) + timedelta(hours=8)).date().isoformat()
  rows = list_active_subscribers_with_status(report_date=cn_today)
  return _etag_response({"count": len(rows), "subscribers": rows})


@app.route("/monitor-api/biz/subscribers/<path:email>", methods=["DELETE", "OPTIONS"])
def monitor_biz_subscriber_delete(email):
  if request.method == "OPTIONS":
    return ("", 204)
  _, err = _require_monitor_auth()
  if err:
    return err
  target = str(email or "").strip().lower()
  if not EMAIL_RE.match(target):
    return jsonify({"error": "invalid_email"}), 400
  deactivate_subscriber(target)
  return jsonify({"ok": True, "email": target})


@app.route("/monitor-api/data/forms", methods=["GET"])
def monitor_data_forms():
  _, err = _require_monitor_auth()
  if err:
    return err
  model = get_latest_model_snapshot() or {}
  tables = model.get("tables") or {}
  stock_counts = (get_stock_admin_data_status().get("counts") or {})
  forms = [
    {"name": "dimensions", "label": "Dimensions", "count": len(tables.get("dimensions") or [])},
    {"name": "indicators", "label": "Indicators", "count": len(tables.get("indicators") or [])},
    {"name": "inputs", "label": "Inputs", "count": len(tables.get("inputs") or [])},
    {"name": "scores", "label": "Scores", "count": len(tables.get("scores") or [])},
    {"name": "alerts", "label": "Alerts", "count": len(tables.get("alerts") or [])},
    {"name": "daily_reports", "label": "Daily Reports", "count": len(list_daily_reports(limit=500))},
    {"name": "subscribers", "label": "Subscribers", "count": len(list_active_subscribers())},
    {"name": "ticker_profiles", "label": "Ticker Profiles", "count": int(stock_counts.get("ticker_profiles") or 0)},
    {"name": "stock_prices", "label": "Stock Prices", "count": int(stock_counts.get("stock_prices") or 0)},
    {"name": "stock_valuations", "label": "Stock Valuations", "count": int(stock_counts.get("stock_valuations") or 0)},
    {"name": "stock_financials", "label": "Stock Financials", "count": int(stock_counts.get("stock_financials") or 0)},
    {"name": "uploaded_files", "label": "Uploaded Files", "count": int(stock_counts.get("uploaded_files") or 0)},
    {"name": "model_runs", "label": "Model Runs", "count": int(stock_counts.get("model_runs") or 0)},
    {"name": "prediction_results", "label": "Prediction Results", "count": int(stock_counts.get("prediction_results") or 0)},
    {"name": "feature_importance", "label": "Feature Importance", "count": int(stock_counts.get("feature_importance") or 0)},
    {"name": "latest_signals", "label": "Latest Signals", "count": int(stock_counts.get("latest_signals") or 0)},
  ]
  return _etag_response({"forms": forms})


@app.route("/monitor-api/data/forms/<name>", methods=["GET"])
def monitor_data_form_rows(name):
  _, err = _require_monitor_auth()
  if err:
    return err
  key = str(name or "").strip().lower()
  model = get_latest_model_snapshot() or {}
  tables = model.get("tables") or {}
  if key in {"dimensions", "indicators", "inputs", "scores", "alerts"}:
    return _etag_response({"name": key, "rows": tables.get(key) or []})
  if key == "daily_reports":
    return _etag_response({"name": key, "rows": list_daily_reports(limit=500)})
  if key == "subscribers":
    return _etag_response({"name": key, "rows": list_active_subscribers()})
  if key in {
    "ticker_profiles",
    "stock_prices",
    "stock_valuations",
    "stock_financials",
    "uploaded_files",
    "model_runs",
    "prediction_results",
    "feature_importance",
    "latest_signals",
  }:
    return _etag_response({"name": key, "rows": get_stock_form_rows(key, limit=500)})
  return jsonify({"error": "not_found"}), 404


@app.route("/api/model/current", methods=["GET", "POST", "OPTIONS"])
def model_current():
  if request.method == "OPTIONS":
    return ("", 204)
  if request.method == "GET":
    view = str(request.args.get("view") or "full").strip().lower()
    row = _cache_get("model:current")
    if row is None:
      row = _cache_set("model:current", get_latest_model_snapshot())
    if not row:
      return jsonify({"error": "not_found"}), 404
    if view == "core":
      core_key = "model:current:core"
      core = _cache_get(core_key)
      if core is None:
        core = _cache_set(core_key, _build_model_core(row))
      return _etag_response(core)
    return _etag_response(row)

  payload = request.get_json(silent=True) or {}
  if not isinstance(payload, dict):
    return jsonify({"error": "invalid_payload"}), 400
  if not payload:
    return jsonify({"error": "empty_payload"}), 400
  save_model_snapshot(payload)
  _invalidate_cache("model:", "reports:")
  return jsonify({"ok": True})


@app.route("/api/model/summary", methods=["GET"])
def model_summary():
  cached = _cache_get("model:summary")
  if cached is not None:
    return _etag_response(cached)
  model = _cache_get("model:current")
  if model is None:
    model = _cache_set("model:current", get_latest_model_snapshot())
  if not model:
    return jsonify({"error": "not_found"}), 404
  reports = _cache_get("reports:1")
  if reports is None:
    reports = _cache_set("reports:1", list_daily_reports(limit=1))
  latest = reports[0] if reports else None
  summary = _build_model_summary(model, latest_report=latest)
  return _etag_response(_cache_set("model:summary", summary))


@app.route("/api/model/tables", methods=["GET"])
def model_tables():
  cached = _cache_get("model:tables")
  if cached is not None:
    return _etag_response(cached)
  model = _cache_get("model:current")
  if model is None:
    model = _cache_set("model:current", get_latest_model_snapshot())
  if not model:
    return jsonify({"error": "not_found"}), 404
  tables_payload = _cache_set("model:tables", _build_model_tables(model))
  return _etag_response(tables_payload)


@app.route("/api/model/workbook", methods=["GET"])
def model_workbook():
  cached = _cache_get("model:workbook")
  if cached is not None:
    return _etag_response(cached)
  model = _cache_get("model:current")
  if model is None:
    model = _cache_set("model:current", get_latest_model_snapshot())
  if not model:
    return jsonify({"error": "not_found"}), 404
  workbook_payload = _cache_set("model:workbook", _build_model_workbook(model))
  return _etag_response(workbook_payload)


@app.route("/api/model/table/<table_name>", methods=["GET"])
def model_single_table(table_name):
  allowed = {"dimensions", "inputs", "indicators", "scores", "alerts"}
  name = str(table_name or "").strip().lower()
  if name not in allowed:
    return jsonify({"error": "invalid_table"}), 400
  cache_key = f"model:table:{name}"
  cached = _cache_get(cache_key)
  if cached is not None:
    return _etag_response(cached)
  model = _cache_get("model:current")
  if model is None:
    model = _cache_set("model:current", get_latest_model_snapshot())
  if not model:
    return jsonify({"error": "not_found"}), 404
  payload = _cache_set(cache_key, _build_single_table(model, name))
  return _etag_response(payload)


@app.route("/api/reports", methods=["GET", "POST", "OPTIONS"])
def reports():
  if request.method == "OPTIONS":
    return ("", 204)
  if request.method == "GET":
    try:
      limit = int(request.args.get("limit", "200"))
    except Exception:
      limit = 200
    limit = max(1, min(limit, 1000))
    cache_key = f"reports:{limit}"
    rows = _cache_get(cache_key)
    if rows is None:
      rows = _cache_set(cache_key, list_daily_reports(limit=limit))
    return _etag_response({"reports": rows})

  payload = request.get_json(silent=True) or {}
  date = str(payload.get("date") or "").strip()
  text = str(payload.get("text") or "")
  meta = payload.get("meta") or {}
  report_payload = payload.get("reportPayload")
  ai_analysis = payload.get("aiAnalysis") or {}
  report_path = str(payload.get("path") or f"reports/{date}.html")
  if not date:
    return jsonify({"error": "missing_date"}), 400
  upsert_daily_report(date, text, meta, report_path=report_path, payload=report_payload, ai_analysis=ai_analysis)
  _invalidate_cache("reports:", "model:summary")
  return jsonify({"ok": True})


@app.route("/api/reports/<report_date>", methods=["GET"])
def report_by_date(report_date):
  cache_key = f"report:{report_date}"
  row = _cache_get(cache_key)
  if row is None:
    row = _cache_set(cache_key, get_daily_report(report_date))
  if not row:
    return jsonify({"error": "not_found"}), 404
  row = dict(row)
  row["aiInsight"] = row.get("aiAnalysis") or get_daily_report_ai_insight(report_date) or None
  return _etag_response(row)


@app.route("/api/reports/<report_date>/analysis", methods=["GET", "POST", "OPTIONS"])
def report_analysis_by_date(report_date):
  if request.method == "OPTIONS":
    return ("", 204)
  date = str(report_date or "").strip()
  if not date:
    return jsonify({"error": "missing_date"}), 400
  if request.method == "GET":
    row = get_daily_report_analysis(date)
    insight = get_daily_report_ai_insight(date)
    if not row and not insight:
      return jsonify({"error": "not_found"}), 404
    # Backward-compatible fallback:
    # if daily_reports ai columns were accidentally blanked, serve the richer insight payload.
    base = row or {}
    insight_data = (insight or {}).get("insight") if isinstance(insight, dict) else {}
    has_text = any(
      str(base.get(k) or "").strip()
      for k in (
        "short_summary",
        "short_summary_zh",
        "short_summary_en",
        "detailed_interpretation",
        "detailed_interpretation_zh",
        "detailed_interpretation_en",
      )
    )
    if not has_text and isinstance(insight, dict):
      base = {
        "report_date": date,
        "short_summary": str(base.get("short_summary") or insight.get("short_summary") or insight_data.get("short_summary_zh") or ""),
        "short_summary_zh": str(base.get("short_summary_zh") or insight_data.get("short_summary_zh") or ""),
        "short_summary_en": str(base.get("short_summary_en") or insight_data.get("short_summary_en") or ""),
        "detailed_interpretation": str(
          base.get("detailed_interpretation")
          or insight.get("detailed_text")
          or insight_data.get("detailed_markdown_zh")
          or ""
        ),
        "detailed_interpretation_zh": str(base.get("detailed_interpretation_zh") or insight_data.get("detailed_markdown_zh") or ""),
        "detailed_interpretation_en": str(base.get("detailed_interpretation_en") or insight_data.get("detailed_markdown_en") or ""),
        "model": str(base.get("model") or insight.get("model") or ""),
        "status": str(base.get("status") or insight.get("status") or ""),
        "generated_at": str(base.get("generated_at") or insight.get("generated_at") or ""),
        "updated_at": str(base.get("updated_at") or ""),
      }
    return _etag_response(base)

  payload = request.get_json(silent=True) or {}
  update_daily_report_analysis(
    report_date=date,
    short_summary=str(payload.get("short_summary") or ""),
    detailed_interpretation=str(payload.get("detailed_interpretation") or ""),
    short_summary_zh=str(payload.get("short_summary_zh") or ""),
    short_summary_en=str(payload.get("short_summary_en") or ""),
    detailed_interpretation_zh=str(payload.get("detailed_interpretation_zh") or ""),
    detailed_interpretation_en=str(payload.get("detailed_interpretation_en") or ""),
    model=str(payload.get("model") or ""),
    status=str(payload.get("status") or ""),
    generated_at=str(payload.get("generated_at") or ""),
  )
  _invalidate_cache("model:summary", "reports:", f"report:{date}")
  return jsonify({"ok": True, "report_date": date})


@app.route("/api/reports/<report_date>/insight", methods=["GET", "POST", "OPTIONS"])
def report_insight_by_date(report_date):
  if request.method == "OPTIONS":
    return ("", 204)
  date = str(report_date or "").strip()
  if not date:
    return jsonify({"error": "missing_date"}), 400
  if request.method == "GET":
    row = get_daily_report_ai_insight(date)
    if not row:
      return jsonify({"error": "not_found"}), 404
    return _etag_response(row)

  payload = request.get_json(silent=True) or {}
  insight = payload.get("insight") or {}
  upsert_daily_report_ai_insight(
    report_date=date,
    short_summary=str(payload.get("short_summary") or ""),
    detailed_text=str(payload.get("detailed_text") or ""),
    insight=insight,
    status=str(payload.get("status") or "ok"),
    model=str(payload.get("model") or ""),
    prompt_version=str(payload.get("prompt_version") or ""),
    generated_at=str(payload.get("generated_at") or ""),
    error=str(payload.get("error") or ""),
  )
  update_daily_report_analysis(
    report_date=date,
    short_summary=str(payload.get("short_summary") or insight.get("short_summary_zh") or ""),
    detailed_interpretation=str(payload.get("detailed_text") or insight.get("detailed_markdown_zh") or ""),
    short_summary_zh=str(insight.get("short_summary_zh") or ""),
    short_summary_en=str(insight.get("short_summary_en") or ""),
    detailed_interpretation_zh=str(insight.get("detailed_markdown_zh") or ""),
    detailed_interpretation_en=str(insight.get("detailed_markdown_en") or ""),
    model=str(payload.get("model") or ""),
    status=str(payload.get("status") or "ok"),
    generated_at=str(payload.get("generated_at") or ""),
  )
  _invalidate_cache("model:summary", f"report:{date}")
  return jsonify({"ok": True, "report_date": date})


@app.route("/api/subscribers", methods=["GET", "POST", "OPTIONS"])
def subscribers():
  if request.method == "OPTIONS":
    return ("", 204)
  if request.method == "GET":
    rows = list_active_subscribers()
    return _etag_response({"count": len(rows), "subscribers": rows})

  payload = request.get_json(silent=True) or {}
  email = str(payload.get("email") or "").strip().lower()
  if not EMAIL_RE.match(email):
    return jsonify({"error": "invalid_email"}), 400
  add_subscriber(email, source="web")
  welcome = _send_welcome_email_if_needed(email)
  return jsonify({"ok": True, "email": email, "welcome": welcome})


@app.route("/api/checks", methods=["POST", "OPTIONS"])
def checks():
  if request.method == "OPTIONS":
    return ("", 204)
  payload = request.get_json(silent=True) or {}
  checked_at = str(payload.get("checkedAt") or "")
  summary = payload.get("summary") or {}
  rows = payload.get("rows") or []
  if not checked_at:
    return jsonify({"error": "missing_checkedAt"}), 400
  save_online_check(checked_at, summary, rows)
  return jsonify({"ok": True})


@app.route("/api/checks/latest", methods=["GET", "OPTIONS"])
def checks_latest():
  if request.method == "OPTIONS":
    return ("", 204)
  row = get_latest_online_check()
  if not row:
    return jsonify({"error": "not_found"}), 404
  return _etag_response(row)


@app.route("/api/ai/data-query", methods=["POST", "OPTIONS"])
def ai_data_query():
  if request.method == "OPTIONS":
    return ("", 204)
  payload = request.get_json(silent=True) or {}
  question = str(payload.get("question") or "").strip()
  lang = str(payload.get("lang") or "zh").strip().lower()
  if not question:
    return jsonify({"error": "missing_question"}), 400
  if len(question) > 1600:
    return jsonify({"error": "question_too_long"}), 400

  # Small query cache to reduce duplicate burst traffic.
  qkey = hashlib.sha1(f"{lang}::{question}".encode("utf-8")).hexdigest()
  qcache_key = f"ai:query:{qkey}"
  cached = _cache_get(qcache_key)
  if cached is not None:
    return _etag_response(cached)

  model = _cache_get("model:current")
  if model is None:
    model = _cache_set("model:current", get_latest_model_snapshot())
  if not model:
    return jsonify({"error": "model_not_found"}), 404

  context_payload = _build_ai_context(model)
  system_text = (
    "You are a macro monitoring data assistant. "
    "Answer ONLY based on the provided JSON context. "
    "If data is missing, say it clearly. "
    "For questions about freshness, use ValueDate/VerificationStatus/Freshness fields and explain exact dates. "
    "Keep response concise and actionable."
  )
  if lang.startswith("zh"):
    system_text += " Respond in Chinese."
  else:
    system_text += " Respond in English."

  user_text = (
    f"Question:\n{question}\n\n"
    f"Context JSON:\n{json.dumps(context_payload, ensure_ascii=False)}"
  )
  req_body = {
    "model": OPENAI_MODEL,
    "temperature": 0.2,
    "messages": [
      {"role": "system", "content": system_text},
      {"role": "user", "content": user_text},
    ],
  }
  if not OPENAI_API_KEY:
    local_answer = _build_local_ai_answer(question, lang, context_payload)
    return jsonify(
      {
        "ok": True,
        "answer": local_answer,
        "model": "local-fallback",
        "asOf": context_payload.get("asOf"),
        "generatedAt": context_payload.get("generatedAt"),
        "usage": {"input_tokens": 0, "output_tokens": 0, "total_tokens": 0},
        "fallback": True,
      }
    )
  try:
    _openai_wait_turn_or_raise()
    resp = None
    last_err = None
    for i in range(OPENAI_MAX_RETRIES):
      try:
        resp = _json_post_json(
          f"{OPENAI_BASE_URL}/chat/completions",
          req_body,
          headers={"Authorization": f"Bearer {OPENAI_API_KEY}"},
        )
        break
      except urllib.error.HTTPError as he:
        last_err = he
        if he.code == 429 and i < OPENAI_MAX_RETRIES - 1:
          time.sleep(1.2 * (i + 1))
          continue
        raise
      except Exception as e:
        last_err = e
        if i < OPENAI_MAX_RETRIES - 1:
          time.sleep(0.8 * (i + 1))
          continue
        raise
    if resp is None and last_err:
      raise last_err
    text = _extract_chat_text(resp)
    usage = resp.get("usage") or {}
    input_tokens = int(usage.get("prompt_tokens") or 0)
    output_tokens = int(usage.get("completion_tokens") or 0)
    total_tokens = int(usage.get("total_tokens") or (input_tokens + output_tokens))
    if not str(text or "").strip():
      text = _build_local_ai_answer(question, lang, context_payload)
    if total_tokens > 0:
      log_token_usage(
        source="ai_data_query",
        model=OPENAI_MODEL,
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        total_tokens=total_tokens,
        meta={"lang": lang, "question": question[:240]},
      )
    result = {
      "ok": True,
      "answer": text,
      "model": OPENAI_MODEL,
      "asOf": context_payload.get("asOf"),
      "generatedAt": context_payload.get("generatedAt"),
      "usage": {
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "total_tokens": total_tokens,
      },
    }
    _cache_set(qcache_key, result)
    return jsonify(result)
  except Exception as e:
    if OPENAI_FORCE_PRIMARY:
      return jsonify({"error": "openai_request_failed", "detail": str(e)[:260]}), 502
    local_answer = _build_local_ai_answer(question, lang, context_payload)
    return jsonify(
      {
        "ok": True,
        "answer": local_answer,
        "model": "local-fallback",
        "asOf": context_payload.get("asOf"),
        "generatedAt": context_payload.get("generatedAt"),
        "usage": {"input_tokens": 0, "output_tokens": 0, "total_tokens": 0},
        "fallback": True,
        "warning": f"openai_request_failed: {str(e)[:120]}",
      }
    )


@app.route("/api/openrouter/rankings", methods=["GET"])
def openrouter_rankings():
  view = str(request.args.get("view") or "week").strip().lower()
  category = str(request.args.get("category") or "all").strip().lower()
  refresh = str(request.args.get("refresh") or "").strip().lower() in {"1", "true", "yes", "on"}
  cache_key = f"openrouter:rankings:{view}:{category}"
  if not refresh:
    cached = _cache_get(cache_key)
    if cached:
      return _etag_response(cached)
    db_payload = get_latest_openrouter_rankings_snapshot(view=view, category=category)
    if db_payload:
      return _etag_response(_cache_set(cache_key, db_payload))
  try:
    payload = _fetch_openrouter_rankings(view=view, category=category)
    upsert_openrouter_rankings_snapshot(payload)
    return _etag_response(_cache_set(cache_key, payload))
  except Exception as e:
    db_stale = get_latest_openrouter_rankings_snapshot(view=view, category=category)
    if db_stale:
      db_stale = dict(db_stale)
      db_stale["warning"] = f"db_stale: {str(e)[:120]}"
      return _etag_response(db_stale)
    stale = _cache_get(cache_key)
    if stale:
      stale = dict(stale)
      stale["warning"] = f"stale_cache: {str(e)[:120]}"
      return _etag_response(stale)
    return jsonify({"ok": False, "error": "openrouter_fetch_failed", "detail": str(e)[:180]}), 502


@app.route("/api/stocks/health", methods=["GET"])
def stocks_health():
  return jsonify({"ok": True, "service": "stocks", "timestamp": now_iso()})


@app.route("/api/stocks/tickers", methods=["GET"])
def stocks_tickers():
  rows = list_stock_tickers()
  return _etag_response({"ok": True, "tickers": rows, "count": len(rows)})


@app.route("/api/stocks/<ticker>/profile", methods=["GET"])
def stocks_profile(ticker):
  row = get_stock_ticker_profile(ticker)
  if not row:
    return jsonify({"error": "not_found"}), 404
  return _etag_response({"ok": True, "profile": row})


@app.route("/api/stocks/<ticker>/predict/latest", methods=["GET"])
def stocks_predict_latest(ticker):
  row = get_latest_prediction_payload(ticker)
  if not row:
    return jsonify({"error": "not_found"}), 404
  return _etag_response({"ok": True, **row})


@app.route("/api/stocks/<ticker>/backtest/summary", methods=["GET"])
def stocks_backtest_summary(ticker):
  row = get_stock_backtest_summary(ticker)
  if not row:
    return jsonify({"error": "not_found"}), 404
  return _etag_response({"ok": True, **row})


@app.route("/api/stocks/<ticker>/backtest/history", methods=["GET"])
def stocks_backtest_history(ticker):
  limit = int(request.args.get("limit") or 120)
  rows = get_stock_backtest_history(ticker, limit=limit)
  return _etag_response({"ok": True, "ticker": str(ticker or "").upper(), "rows": rows, "count": len(rows)})


@app.route("/api/stocks/<ticker>/features/latest", methods=["GET"])
def stocks_features_latest(ticker):
  limit = int(request.args.get("limit") or 10)
  rows = get_stock_latest_features(ticker, limit=limit)
  return _etag_response({"ok": True, "ticker": str(ticker or "").upper(), "rows": rows, "count": len(rows)})


def _stocks_admin_guard():
  user, err = _require_monitor_auth()
  if err:
    return None, err
  return user, None


@app.route("/api/stocks/admin/upload-csv", methods=["POST", "OPTIONS"])
@app.route("/monitor-api/stocks/admin/upload-csv", methods=["POST", "OPTIONS"])
def stocks_admin_upload_csv():
  if request.method == "OPTIONS":
    return ("", 204)
  _, err = _stocks_admin_guard()
  if err:
    return err
  ticker = str(request.form.get("ticker") or request.args.get("ticker") or "").strip().upper()
  auto_refresh = str(request.form.get("autoRefresh") or request.args.get("autoRefresh") or "").strip().lower() in {"1", "true", "yes", "on"}
  files = request.files.getlist("files")
  mode = str(request.form.get("mode") or request.args.get("mode") or "").strip().lower()
  if not ticker:
    return jsonify({"error": "missing_ticker"}), 400
  if not files:
    return jsonify({"error": "missing_files"}), 400
  try:
    if mode == "inspect":
      rows = inspect_csv_uploads(files)
      return jsonify({"ok": True, "ticker": ticker, "mode": "inspect", "recognized": rows, "count": len(rows)})
    result = import_csv_uploads(ticker=ticker, uploaded_files=files, auto_refresh=auto_refresh)
    _invalidate_cache("stocks:")
    return jsonify({"ok": True, **result})
  except Exception as e:
    return jsonify({"ok": False, "error": "upload_failed", "detail": str(e)[:240]}), 500


@app.route("/api/stocks/admin/import-csv", methods=["POST", "OPTIONS"])
@app.route("/monitor-api/stocks/admin/import-csv", methods=["POST", "OPTIONS"])
def stocks_admin_import_csv():
  if request.method == "OPTIONS":
    return ("", 204)
  _, err = _stocks_admin_guard()
  if err:
    return err
  ticker = str(request.form.get("ticker") or request.args.get("ticker") or "").strip().upper()
  files = request.files.getlist("files")
  if not ticker:
    return jsonify({"error": "missing_ticker"}), 400
  if not files:
    return jsonify({"error": "missing_files"}), 400
  try:
    result = import_csv_uploads(ticker=ticker, uploaded_files=files, auto_refresh=False)
    _invalidate_cache("stocks:")
    return jsonify({"ok": True, "mode": "import_only", **result})
  except Exception as e:
    return jsonify({"ok": False, "error": "import_failed", "detail": str(e)[:240]}), 500


@app.route("/api/stocks/admin/import-and-refresh", methods=["POST", "OPTIONS"])
@app.route("/monitor-api/stocks/admin/import-and-refresh", methods=["POST", "OPTIONS"])
def stocks_admin_import_and_refresh():
  if request.method == "OPTIONS":
    return ("", 204)
  _, err = _stocks_admin_guard()
  if err:
    return err
  ticker = str(request.form.get("ticker") or request.args.get("ticker") or "").strip().upper()
  files = request.files.getlist("files")
  if not ticker:
    return jsonify({"error": "missing_ticker"}), 400
  if not files:
    return jsonify({"error": "missing_files"}), 400
  try:
    result = import_csv_uploads(ticker=ticker, uploaded_files=files, auto_refresh=True)
    _invalidate_cache("stocks:")
    return jsonify({"ok": True, "mode": "import_and_refresh", **result})
  except Exception as e:
    return jsonify({"ok": False, "error": "import_and_refresh_failed", "detail": str(e)[:240]}), 500


@app.route("/api/stocks/admin/refresh/<ticker>", methods=["POST", "OPTIONS"])
@app.route("/monitor-api/stocks/admin/refresh/<ticker>", methods=["POST", "OPTIONS"])
def stocks_admin_refresh(ticker):
  if request.method == "OPTIONS":
    return ("", 204)
  _, err = _stocks_admin_guard()
  if err:
    return err
  result = train_and_refresh_ticker(ticker)
  _invalidate_cache("stocks:")
  code = 200 if result.get("ok") else 400
  return jsonify(result), code


@app.route("/api/stocks/admin/data-status", methods=["GET"])
@app.route("/monitor-api/stocks/admin/data-status", methods=["GET"])
def stocks_admin_data_status():
  _, err = _stocks_admin_guard()
  if err:
    return err
  return jsonify({"ok": True, **get_stock_admin_data_status()})


@app.route("/api/stocks/admin/tickers/<ticker>/status", methods=["GET"])
@app.route("/monitor-api/stocks/admin/tickers/<ticker>/status", methods=["GET"])
def stocks_admin_ticker_status(ticker):
  _, err = _stocks_admin_guard()
  if err:
    return err
  return jsonify({"ok": True, **get_stock_ticker_admin_status(ticker)})


@app.route("/api/stocks/admin/upload-history", methods=["GET"])
@app.route("/monitor-api/stocks/admin/upload-history", methods=["GET"])
def stocks_admin_upload_history():
  _, err = _stocks_admin_guard()
  if err:
    return err
  ticker = str(request.args.get("ticker") or "").strip().upper()
  limit = int(request.args.get("limit") or 120)
  rows = list_stock_upload_history(limit=limit, ticker=ticker)
  return jsonify({"ok": True, "rows": rows, "count": len(rows)})


@app.route("/api/stocks/admin/train-history", methods=["GET"])
@app.route("/monitor-api/stocks/admin/train-history", methods=["GET"])
def stocks_admin_train_history():
  _, err = _stocks_admin_guard()
  if err:
    return err
  ticker = str(request.args.get("ticker") or "").strip().upper()
  limit = int(request.args.get("limit") or 80)
  rows = list_stock_model_runs(limit=limit, ticker=ticker)
  return jsonify({"ok": True, "rows": rows, "count": len(rows)})


@app.route("/api/migrate", methods=["POST", "OPTIONS"])
def migrate():
  if request.method == "OPTIONS":
    return ("", 204)
  payload = request.get_json(silent=True) or {}
  model = payload.get("model")
  reports = payload.get("reports") or []
  checks = payload.get("checks") or []
  if isinstance(model, dict) and model:
    save_model_snapshot(model)
  for r in reports:
    date = str(r.get("date") or "").strip()
    if not date:
      continue
    upsert_daily_report(
      report_date=date,
      text=str(r.get("text") or ""),
      meta=r.get("meta") or {},
      report_path=str(r.get("path") or f"reports/{date}.html"),
      payload=r.get("reportPayload"),
    )
  for c in checks:
    checked_at = str(c.get("checkedAt") or "").strip()
    if not checked_at:
      continue
    save_online_check(checked_at, c.get("summary") or {}, c.get("results") or [])
  _invalidate_cache("model:", "reports:", "report:")
  return jsonify({"ok": True, "migrated_reports": len(reports), "migrated_checks": len(checks)})


@app.route("/", defaults={"path": "index.html"})
@app.route("/<path:path>")
def static_files(path):
  target = ROOT / path
  if target.is_file():
    return send_from_directory(str(ROOT), path)
  return send_from_directory(str(ROOT), "index.html")


def main():
  init_db()
  host = os.environ.get("HOST", "0.0.0.0")
  port = int(os.environ.get("PORT", "5000"))
  app.run(host=host, port=port, debug=False)


if __name__ == "__main__":
  main()
