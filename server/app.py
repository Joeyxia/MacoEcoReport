#!/usr/bin/env python3
import os
import re
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

app = Flask(__name__, static_folder=str(ROOT), static_url_path="")
init_db()


@app.after_request
def set_cors(resp):
  resp.headers["Access-Control-Allow-Origin"] = "*"
  resp.headers["Access-Control-Allow-Headers"] = "Content-Type"
  resp.headers["Access-Control-Allow-Methods"] = "GET,POST,OPTIONS"
  return resp


@app.route("/api/health", methods=["GET"])
def health():
  return jsonify({"ok": True})


@app.route("/api/model/current", methods=["GET", "POST", "OPTIONS"])
def model_current():
  if request.method == "OPTIONS":
    return ("", 204)
  if request.method == "GET":
    row = get_latest_model_snapshot()
    if not row:
      return jsonify({"error": "not_found"}), 404
    return jsonify(row)

  payload = request.get_json(silent=True) or {}
  if not isinstance(payload, dict):
    return jsonify({"error": "invalid_payload"}), 400
  save_model_snapshot(payload)
  return jsonify({"ok": True})


@app.route("/api/reports", methods=["GET", "POST", "OPTIONS"])
def reports():
  if request.method == "OPTIONS":
    return ("", 204)
  if request.method == "GET":
    try:
      limit = int(request.args.get("limit", "200"))
    except Exception:
      limit = 200
    return jsonify({"reports": list_daily_reports(limit=max(1, min(limit, 1000)))})

  payload = request.get_json(silent=True) or {}
  date = str(payload.get("date") or "").strip()
  text = str(payload.get("text") or "")
  meta = payload.get("meta") or {}
  report_payload = payload.get("reportPayload")
  report_path = str(payload.get("path") or f"reports/{date}.html")
  if not date:
    return jsonify({"error": "missing_date"}), 400
  upsert_daily_report(date, text, meta, report_path=report_path, payload=report_payload)
  return jsonify({"ok": True})


@app.route("/api/reports/<report_date>", methods=["GET"])
def report_by_date(report_date):
  row = get_daily_report(report_date)
  if not row:
    return jsonify({"error": "not_found"}), 404
  return jsonify(row)


@app.route("/api/subscribers", methods=["GET", "POST", "OPTIONS"])
def subscribers():
  if request.method == "OPTIONS":
    return ("", 204)
  if request.method == "GET":
    rows = list_active_subscribers()
    return jsonify({"count": len(rows), "subscribers": rows})

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
