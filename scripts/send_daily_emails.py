#!/usr/bin/env python3
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "server"))
from db import init_db, list_active_subscribers, save_email_dispatch_log
from mailer import send_email

SUBSCRIBERS_PATH = ROOT / "data" / "subscribers.json"
SNAPSHOT_PATH = ROOT / "data" / "latest_snapshot.json"
REPORTS_INDEX_PATH = ROOT / "reports" / "index.json"
EMAIL_LOG_PATH = ROOT / "data" / "email_dispatch_log.json"

PAGES_BASE_URL = os.environ.get("PAGES_BASE_URL", "https://nexo.hk").rstrip("/")
RESEND_API_KEY = os.environ.get("RESEND_API_KEY", "").strip()
RESEND_FROM = os.environ.get("RESEND_FROM", "").strip()
REFRESH_STATUS = os.environ.get("REFRESH_STATUS", "").strip().lower()


def load_json(path: Path, default):
  if not path.exists():
    return default
  try:
    return json.loads(path.read_text(encoding="utf-8"))
  except Exception:
    return default


def build_daily_report_email(snapshot, report_date: str, report_link: str):
  score = snapshot.get("totalScore", "--")
  status = snapshot.get("status", "--")
  summary = snapshot.get("latestReportSummary", "")
  watched = snapshot.get("dailyWatchedItems", [])[:6]
  watched_html = "".join([f"<li>{str(x)}</li>" for x in watched]) or "<li>No watched items.</li>"
  subject = f"Macro Daily Report {report_date} | 每日报告 | Score {score} ({status})"
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
    <hr style="border:none;border-top:1px solid #dfe7ea;margin:14px 0;" />
    <h2 style="margin:0 0 8px 0;">宏观每日报告（{report_date}）</h2>
    <p style="margin:0 0 8px 0;"><strong>综合得分：</strong>{score}（{status}）</p>
    <p style="margin:0 0 12px 0;"><strong>摘要：</strong>{summary}</p>
    <p style="margin:0 0 6px 0;"><strong>当日关注项：</strong></p>
    <ul>{watched_html}</ul>
    <p style="margin:12px 0 0 0;">
      报告链接：<a href="{report_link}">{report_link}</a>
    </p>
  </div>
  """
  return subject, html


def build_failure_email(report_date: str, reason: str):
  subject = f"Daily Report Delay Notice {report_date} | 每日报告延迟通知"
  html = f"""
  <div style="font-family:Arial,sans-serif;line-height:1.6;color:#10242b">
    <h2 style="margin:0 0 8px 0;">Daily Report Delay Notice ({report_date})</h2>
    <p style="margin:0 0 10px 0;">
      Today's report has not been generated on schedule due to data refresh issues.
    </p>
    <p style="margin:0 0 10px 0;"><strong>Current status:</strong> {reason}</p>
    <p style="margin:0;">
      We will automatically send the daily report once generation is recovered.
    </p>
    <hr style="border:none;border-top:1px solid #dfe7ea;margin:14px 0;" />
    <h2 style="margin:0 0 8px 0;">每日报告延迟通知（{report_date}）</h2>
    <p style="margin:0 0 10px 0;">
      由于数据更新异常，今日日报暂未按时生成。
    </p>
    <p style="margin:0 0 10px 0;"><strong>当前状态：</strong>{reason}</p>
    <p style="margin:0;">
      日报恢复后，我们会自动补发当日报告邮件。
    </p>
  </div>
  """
  return subject, html


def main():
  init_db()
  if not ((RESEND_API_KEY and RESEND_FROM) or (os.environ.get("SMTP_USER") and os.environ.get("SMTP_APP_PASSWORD"))):
    print("skip: no mail provider configured")
    return

  db_subs = list_active_subscribers()
  file_subs = load_json(SUBSCRIBERS_PATH, {"subscribers": []}).get("subscribers", [])
  subscribers = db_subs or file_subs
  recipients = [s.get("email", "").strip().lower() for s in subscribers if s.get("email")]
  recipients = sorted(set([x for x in recipients if x]))
  if not recipients:
    print("skip: no active subscribers")
    return

  snapshot = load_json(SNAPSHOT_PATH, {})
  reports = load_json(REPORTS_INDEX_PATH, {"reports": []}).get("reports", [])
  cn_today = (datetime.now(timezone.utc) + timedelta(hours=8)).date().isoformat()
  report_date = snapshot.get("reportDate") or snapshot.get("asOf") or (reports[0].get("date") if reports else cn_today)

  refresh_ok = REFRESH_STATUS in {"success", "ok", "passed", "true", "1"}
  has_today_report = any(str(r.get("date") or "") == cn_today for r in reports)
  is_success = bool(refresh_ok and has_today_report and report_date == cn_today)

  if is_success:
    report_link = f"{PAGES_BASE_URL}/reports/{report_date}.html"
    subject, html = build_daily_report_email(snapshot, report_date, report_link)
    email_type = "daily_report"
  else:
    reason = f"refresh_status={REFRESH_STATUS or 'unknown'}, has_today_report={has_today_report}, snapshot_report_date={report_date}"
    subject, html = build_failure_email(cn_today, reason)
    email_type = "delay_notice"

  sent = 0
  failed = 0
  errors = []
  for email in recipients:
    try:
      send_email(email, subject, html)
      sent += 1
    except Exception as e:
      failed += 1
      errors.append({"email": email, "error": str(e)[:240]})

  log = {
    "date": report_date if is_success else cn_today,
    "generatedAt": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
    "sent": sent,
    "failed": failed,
    "recipients": len(recipients),
    "type": email_type,
    "errors": errors,
  }
  EMAIL_LOG_PATH.write_text(json.dumps(log, ensure_ascii=False, indent=2), encoding="utf-8")
  save_email_dispatch_log(log)
  print(f"email_dispatch date={report_date} recipients={len(recipients)} sent={sent} failed={failed}")


if __name__ == "__main__":
  main()
