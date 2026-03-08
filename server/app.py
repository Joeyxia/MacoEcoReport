#!/usr/bin/env python3
import os
import re
import time
import json
import hashlib
import base64
import secrets
import urllib.parse
import urllib.request
from threading import Lock
from pathlib import Path
from datetime import datetime, timedelta, timezone

from flask import Flask, jsonify, request, send_from_directory, redirect, session

try:
  from .db import (
    add_subscriber,
    deactivate_subscriber,
    get_daily_report,
    get_latest_model_snapshot,
    init_db,
    list_active_subscribers,
    list_active_subscribers_with_status,
    list_daily_reports,
    get_page_visit_daily,
    get_page_visit_minute,
    get_page_visit_by_path,
    get_token_usage_daily,
    get_token_usage_minute,
    has_email_event,
    log_page_event,
    log_token_usage,
    save_email_event,
    save_model_snapshot,
    save_online_check,
    upsert_daily_report,
  )
  from .mailer import send_email
except ImportError:
  from db import (
    add_subscriber,
    deactivate_subscriber,
    get_daily_report,
    get_latest_model_snapshot,
    init_db,
    list_active_subscribers,
    list_active_subscribers_with_status,
    list_daily_reports,
    get_page_visit_daily,
    get_page_visit_minute,
    get_page_visit_by_path,
    get_token_usage_daily,
    get_token_usage_minute,
    has_email_event,
    log_page_event,
    log_token_usage,
    save_email_event,
    save_model_snapshot,
    save_online_check,
    upsert_daily_report,
  )
  from mailer import send_email

ROOT = Path(__file__).resolve().parents[1]
EMAIL_RE = re.compile(r"^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$", re.I)
CACHE_TTL_SECONDS = int(os.environ.get("API_CACHE_TTL_SECONDS", "90"))
MONITOR_ALLOWED_EMAILS = {x.strip().lower() for x in str(os.environ.get("MONITOR_ALLOWED_EMAILS", "")).split(",") if x.strip()}
MONITOR_ALLOWED_DOMAINS = {x.strip().lower() for x in str(os.environ.get("MONITOR_ALLOWED_DOMAINS", "")).split(",") if x.strip()}
GOOGLE_CLIENT_ID = str(os.environ.get("GOOGLE_OAUTH_CLIENT_ID", "")).strip()
GOOGLE_CLIENT_SECRET = str(os.environ.get("GOOGLE_OAUTH_CLIENT_SECRET", "")).strip()
GOOGLE_REDIRECT_URI = str(os.environ.get("GOOGLE_OAUTH_REDIRECT_URI", "https://monitor.nexo.hk/monitor-api/auth/google/callback")).strip()
GOOGLE_AUTH_URL = "https://accounts.google.com/o/oauth2/v2/auth"
GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
GOOGLE_USERINFO_URL = "https://openidconnect.googleapis.com/v1/userinfo"
OPENAI_API_KEY = str(os.environ.get("OPENAI_API_KEY", "")).strip()
OPENAI_MODEL = str(os.environ.get("OPENAI_MODEL", "gpt-5.4")).strip()
OPENAI_BASE_URL = str(os.environ.get("OPENAI_BASE_URL", "https://api.openai.com/v1")).strip().rstrip("/")

app = Flask(__name__, static_folder=str(ROOT), static_url_path="")
app.secret_key = os.environ.get("MONITOR_SESSION_SECRET") or base64.urlsafe_b64encode(os.urandom(32)).decode("ascii")
app.config.update(
  SESSION_COOKIE_HTTPONLY=True,
  SESSION_COOKIE_SAMESITE="Lax",
  SESSION_COOKIE_SECURE=True,
)
init_db()
_cache = {}
_cache_lock = Lock()


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

  return {
    "asOf": model.get("asOf") or "",
    "totalScore": model.get("totalScore") or 0,
    "status": model.get("status") or "",
    "alerts": alerts,
    "topDimensionContributors": top_contributors,
    "primaryDrivers": primary_drivers,
    "keyIndicatorsSnapshot": key_indicators,
    "dailyWatchedItems": watch_items,
    "latestReportSummary": model.get("latestReportSummary") or report_meta.get("summary") or "",
    "latestReportDate": report_date or "",
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
  if MONITOR_ALLOWED_EMAILS and e in MONITOR_ALLOWED_EMAILS:
    return True
  if MONITOR_ALLOWED_DOMAINS:
    domain = e.split("@", 1)[1] if "@" in e else ""
    return domain in MONITOR_ALLOWED_DOMAINS
  return True


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
  return {
    "asOf": model.get("asOf") or "",
    "reportDate": model.get("reportDate") or "",
    "generatedAt": model.get("generatedAt") or "",
    "totalScore": model.get("totalScore") or 0,
    "status": model.get("status") or "",
    "onlineUpdate": model.get("onlineUpdate") or {},
    "freshness": model.get("freshness") or {},
    "topDimensionContributors": model.get("topDimensionContributors") or [],
    "keyIndicatorsSnapshot": model.get("keyIndicatorsSnapshot") or [],
    "failedIndicators": failed[:80],
  }


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
  visits_by_path = get_page_visit_by_path(days=max(1, min(days, 365)), limit=30)
  tokens_daily = get_token_usage_daily(days=max(1, min(days, 365)))
  tokens_minute = get_token_usage_minute(minutes=minutes)
  totals = {
    "pageVisits": sum(int(x.get("visits") or 0) for x in visits_minute),
    "inputTokens": sum(int(x.get("input_tokens") or 0) for x in tokens_minute),
    "outputTokens": sum(int(x.get("output_tokens") or 0) for x in tokens_minute),
    "totalTokens": sum(int(x.get("total_tokens") or 0) for x in tokens_minute),
  }
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
  forms = [
    {"name": "dimensions", "label": "Dimensions", "count": len(tables.get("dimensions") or [])},
    {"name": "indicators", "label": "Indicators", "count": len(tables.get("indicators") or [])},
    {"name": "inputs", "label": "Inputs", "count": len(tables.get("inputs") or [])},
    {"name": "scores", "label": "Scores", "count": len(tables.get("scores") or [])},
    {"name": "alerts", "label": "Alerts", "count": len(tables.get("alerts") or [])},
    {"name": "daily_reports", "label": "Daily Reports", "count": len(list_daily_reports(limit=500))},
    {"name": "subscribers", "label": "Subscribers", "count": len(list_active_subscribers())},
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
  report_path = str(payload.get("path") or f"reports/{date}.html")
  if not date:
    return jsonify({"error": "missing_date"}), 400
  upsert_daily_report(date, text, meta, report_path=report_path, payload=report_payload)
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
  return _etag_response(row)


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


@app.route("/api/ai/data-query", methods=["POST", "OPTIONS"])
def ai_data_query():
  if request.method == "OPTIONS":
    return ("", 204)
  if not OPENAI_API_KEY:
    return jsonify({"error": "openai_not_configured"}), 503
  payload = request.get_json(silent=True) or {}
  question = str(payload.get("question") or "").strip()
  lang = str(payload.get("lang") or "zh").strip().lower()
  if not question:
    return jsonify({"error": "missing_question"}), 400
  if len(question) > 1600:
    return jsonify({"error": "question_too_long"}), 400

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
  try:
    resp = _json_post_json(
      f"{OPENAI_BASE_URL}/chat/completions",
      req_body,
      headers={"Authorization": f"Bearer {OPENAI_API_KEY}"},
    )
    text = _extract_chat_text(resp)
    usage = resp.get("usage") or {}
    input_tokens = int(usage.get("prompt_tokens") or 0)
    output_tokens = int(usage.get("completion_tokens") or 0)
    total_tokens = int(usage.get("total_tokens") or (input_tokens + output_tokens))
    if total_tokens > 0:
      log_token_usage(
        source="ai_data_query",
        model=OPENAI_MODEL,
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        total_tokens=total_tokens,
        meta={"lang": lang, "question": question[:240]},
      )
    return jsonify(
      {
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
    )
  except Exception as e:
    return jsonify({"error": "openai_request_failed", "detail": str(e)[:260]}), 502


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
