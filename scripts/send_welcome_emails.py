#!/usr/bin/env python3
import json
import os
import sys
from pathlib import Path
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "server"))
from db import has_email_event, init_db, list_active_subscribers, save_email_event

RESEND_API_KEY = os.environ.get("RESEND_API_KEY", "").strip()
RESEND_FROM = os.environ.get("RESEND_FROM", "").strip()


def resend_send(to_email: str, subject: str, html: str):
  payload = {"from": RESEND_FROM, "to": [to_email], "subject": subject, "html": html}
  req = Request(
    "https://api.resend.com/emails",
    data=json.dumps(payload).encode("utf-8"),
    headers={
      "Authorization": f"Bearer {RESEND_API_KEY}",
      "Content-Type": "application/json",
      "Accept": "application/json",
      "User-Agent": "macro-monitor-welcome-bot",
    },
    method="POST",
  )
  with urlopen(req, timeout=30) as resp:
    return json.loads(resp.read().decode("utf-8"))


def build_welcome(email: str):
  subject = "Welcome to Nexo Marco Intelligence | 欢迎订阅 Nexo Marco Intelligence"
  html = f"""
  <div style="font-family:Arial,sans-serif;line-height:1.6;color:#10242b">
    <h2 style="margin:0 0 8px 0;">Welcome to Nexo Marco Intelligence</h2>
    <p style="margin:0 0 10px 0;">Thanks for subscribing, {email}.</p>
    <p style="margin:0 0 10px 0;">
      Nexo Marco Intelligence is a macro risk monitoring platform using a 14-dimension model to track global financial risk signals.
    </p>
    <p style="margin:0 0 14px 0;">
      You will receive the daily report every day around 09:00 China time (UTC+8).
    </p>
    <hr style="border:none;border-top:1px solid #dfe7ea;margin:14px 0;" />
    <h2 style="margin:0 0 8px 0;">欢迎订阅 Nexo Marco Intelligence</h2>
    <p style="margin:0 0 10px 0;">感谢订阅，{email}。</p>
    <p style="margin:0 0 10px 0;">
      Nexo Marco Intelligence 是一个基于 14 维模型的宏观风险监控平台，用于跟踪全球金融风险信号。
    </p>
    <p style="margin:0;">
      你将在每天中国时间早上 09:00 左右收到当日的每日报告。
    </p>
  </div>
  """
  return subject, html


def main():
  init_db()
  if not RESEND_API_KEY or not RESEND_FROM:
    raise RuntimeError("RESEND_API_KEY/RESEND_FROM not configured")
  subs = list_active_subscribers()
  sent = 0
  skipped = 0
  failed = 0
  for s in subs:
    email = str(s.get("email") or "").strip().lower()
    if not email:
      continue
    if has_email_event(email, "welcome_sent"):
      skipped += 1
      continue
    subject, html = build_welcome(email)
    try:
      resend_send(email, subject, html)
      save_email_event(email, "welcome_sent", {"subject": subject})
      sent += 1
    except Exception:
      failed += 1
  print(f"welcome_dispatch sent={sent} skipped={skipped} failed={failed} total={len(subs)}")


if __name__ == "__main__":
  main()
