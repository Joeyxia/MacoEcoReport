#!/usr/bin/env python3
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "server"))
from db import has_email_event, init_db, list_active_subscribers, save_email_event
from mailer import send_email

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
  if not ((os.environ.get("RESEND_API_KEY") and os.environ.get("RESEND_FROM")) or (os.environ.get("SMTP_USER") and os.environ.get("SMTP_APP_PASSWORD"))):
    raise RuntimeError("mail provider not configured")
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
      send_email(email, subject, html)
      save_email_event(email, "welcome_sent", {"subject": subject})
      sent += 1
    except Exception:
      failed += 1
  print(f"welcome_dispatch sent={sent} skipped={skipped} failed={failed} total={len(subs)}")


if __name__ == "__main__":
  main()
