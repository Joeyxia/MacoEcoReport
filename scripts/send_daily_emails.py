#!/usr/bin/env python3
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]
SUBSCRIBERS_PATH = ROOT / "data" / "subscribers.json"
SNAPSHOT_PATH = ROOT / "data" / "latest_snapshot.json"
REPORTS_INDEX_PATH = ROOT / "reports" / "index.json"
EMAIL_LOG_PATH = ROOT / "data" / "email_dispatch_log.json"

PAGES_BASE_URL = os.environ.get("PAGES_BASE_URL", "https://joeyxia.github.io/MacoEcoReport").rstrip("/")
RESEND_API_KEY = os.environ.get("RESEND_API_KEY", "").strip()
RESEND_FROM = os.environ.get("RESEND_FROM", "").strip()


def load_json(path: Path, default):
  if not path.exists():
    return default
  try:
    return json.loads(path.read_text(encoding="utf-8"))
  except Exception:
    return default


def resend_send(to_email: str, subject: str, html: str):
  payload = {
    "from": RESEND_FROM,
    "to": [to_email],
    "subject": subject,
    "html": html,
  }
  req = Request(
    "https://api.resend.com/emails",
    data=json.dumps(payload).encode("utf-8"),
    headers={
      "Authorization": f"Bearer {RESEND_API_KEY}",
      "Content-Type": "application/json",
      "Accept": "application/json",
      "User-Agent": "macro-monitor-bot",
    },
    method="POST",
  )
  with urlopen(req, timeout=30) as resp:
    return json.loads(resp.read().decode("utf-8"))


def build_email_content(snapshot, report_date: str, report_link: str):
  score = snapshot.get("totalScore", "--")
  status = snapshot.get("status", "--")
  summary = snapshot.get("latestReportSummary", "")
  watched = snapshot.get("dailyWatchedItems", [])[:6]
  watched_html = "".join([f"<li>{str(x)}</li>" for x in watched]) or "<li>No watched items.</li>"
  subject = f"Macro Daily Report {report_date} | Score {score} ({status})"
  html = f"""
  <div style="font-family:Arial,sans-serif;line-height:1.5;color:#10242b">
    <h2 style="margin:0 0 8px 0;">Macro Daily Report ({report_date})</h2>
    <p style="margin:0 0 8px 0;"><strong>Composite Score:</strong> {score} ({status})</p>
    <p style="margin:0 0 12px 0;"><strong>Summary:</strong> {summary}</p>
    <p style="margin:0 0 6px 0;"><strong>Daily Watched Items:</strong></p>
    <ul>{watched_html}</ul>
    <p style="margin:12px 0 0 0;">
      Full report: <a href="{report_link}">{report_link}</a>
    </p>
  </div>
  """
  return subject, html


def main():
  if not RESEND_API_KEY or not RESEND_FROM:
    print("skip: RESEND_API_KEY or RESEND_FROM not set")
    return

  subscribers = load_json(SUBSCRIBERS_PATH, {"subscribers": []}).get("subscribers", [])
  recipients = [s.get("email", "").strip().lower() for s in subscribers if s.get("status") == "active" and s.get("email")]
  recipients = sorted(set([x for x in recipients if x]))
  if not recipients:
    print("skip: no active subscribers")
    return

  snapshot = load_json(SNAPSHOT_PATH, {})
  reports = load_json(REPORTS_INDEX_PATH, {"reports": []}).get("reports", [])
  report_date = snapshot.get("reportDate") or snapshot.get("asOf") or (reports[0].get("date") if reports else datetime.now(timezone.utc).date().isoformat())
  report_link = f"{PAGES_BASE_URL}/reports/{report_date}.html"
  subject, html = build_email_content(snapshot, report_date, report_link)

  sent = 0
  failed = 0
  errors = []
  for email in recipients:
    try:
      resend_send(email, subject, html)
      sent += 1
    except Exception as e:
      failed += 1
      errors.append({"email": email, "error": str(e)[:240]})

  log = {
    "date": report_date,
    "generatedAt": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
    "sent": sent,
    "failed": failed,
    "recipients": len(recipients),
    "errors": errors,
  }
  EMAIL_LOG_PATH.write_text(json.dumps(log, ensure_ascii=False, indent=2), encoding="utf-8")
  print(f"email_dispatch date={report_date} recipients={len(recipients)} sent={sent} failed={failed}")


if __name__ == "__main__":
  main()
