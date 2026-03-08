#!/usr/bin/env python3
import os
import re
import time
import json
import hashlib
from threading import Lock
from pathlib import Path

from flask import Flask, jsonify, request, send_from_directory

try:
  from .db import (
    add_subscriber,
    get_daily_report,
    get_latest_model_snapshot,
    init_db,
    list_active_subscribers,
    list_daily_reports,
    save_model_snapshot,
    save_online_check,
    upsert_daily_report,
  )
except ImportError:
  from db import (
    add_subscriber,
    get_daily_report,
    get_latest_model_snapshot,
    init_db,
    list_active_subscribers,
    list_daily_reports,
    save_model_snapshot,
    save_online_check,
    upsert_daily_report,
  )

ROOT = Path(__file__).resolve().parents[1]
EMAIL_RE = re.compile(r"^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$", re.I)
CACHE_TTL_SECONDS = int(os.environ.get("API_CACHE_TTL_SECONDS", "90"))

app = Flask(__name__, static_folder=str(ROOT), static_url_path="")
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


@app.after_request
def set_cors(resp):
  resp.headers["Access-Control-Allow-Origin"] = "*"
  resp.headers["Access-Control-Allow-Headers"] = "Content-Type"
  resp.headers["Access-Control-Allow-Methods"] = "GET,POST,OPTIONS"
  return resp


@app.route("/api/health", methods=["GET"])
def health():
  return _etag_response({"ok": True})


@app.route("/api/model/current", methods=["GET", "POST", "OPTIONS"])
def model_current():
  if request.method == "OPTIONS":
    return ("", 204)
  if request.method == "GET":
    row = _cache_get("model:current")
    if row is None:
      row = _cache_set("model:current", get_latest_model_snapshot())
    if not row:
      return jsonify({"error": "not_found"}), 404
    return _etag_response(row)

  payload = request.get_json(silent=True) or {}
  if not isinstance(payload, dict):
    return jsonify({"error": "invalid_payload"}), 400
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
  return jsonify({"ok": True, "email": email})


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
