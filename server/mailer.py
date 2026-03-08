#!/usr/bin/env python3
import json
import os
import smtplib
from email.mime.text import MIMEText
from email.utils import formataddr
from urllib.request import Request, urlopen


def _env(name: str) -> str:
  return str(os.environ.get(name, "")).strip()


def _parse_from(raw: str):
  text = str(raw or "").strip()
  if "<" in text and ">" in text:
    label = text.split("<", 1)[0].strip().strip('"')
    addr = text.split("<", 1)[1].split(">", 1)[0].strip()
    return label, addr
  return "", text


def send_email(to_email: str, subject: str, html: str):
  resend_key = _env("RESEND_API_KEY")
  resend_from = _env("RESEND_FROM")
  if resend_key and resend_from:
    payload = {"from": resend_from, "to": [to_email], "subject": subject, "html": html}
    req = Request(
      "https://api.resend.com/emails",
      data=json.dumps(payload).encode("utf-8"),
      headers={
        "Authorization": f"Bearer {resend_key}",
        "Content-Type": "application/json",
        "Accept": "application/json",
        "User-Agent": "macro-monitor-mailer",
      },
      method="POST",
    )
    with urlopen(req, timeout=30) as resp:
      return {"provider": "resend", "response": json.loads(resp.read().decode("utf-8"))}

  smtp_user = _env("SMTP_USER")
  smtp_pass = _env("SMTP_APP_PASSWORD")
  smtp_from = _env("SMTP_FROM") or smtp_user
  smtp_host = _env("SMTP_HOST") or "smtp.gmail.com"
  smtp_port = int(_env("SMTP_PORT") or "587")
  if not smtp_user or not smtp_pass or not smtp_from:
    raise RuntimeError("email provider not configured: set RESEND_* or SMTP_* env")

  name, addr = _parse_from(smtp_from)
  msg = MIMEText(html, "html", "utf-8")
  msg["Subject"] = subject
  msg["From"] = formataddr((name, addr)) if name else addr
  msg["To"] = to_email
  reply_to = _env("SMTP_REPLY_TO")
  if reply_to:
    msg["Reply-To"] = reply_to

  with smtplib.SMTP(smtp_host, smtp_port, timeout=30) as server:
    server.starttls()
    server.login(smtp_user, smtp_pass)
    server.sendmail(addr, [to_email], msg.as_string())
  return {"provider": "smtp", "response": {"ok": True}}
