#!/usr/bin/env python3
import argparse
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "server"))
from db import delete_token_usage_by_source, init_db, log_token_usage

OPENAI_API_KEY = os.environ.get("OPENAI_ADMIN_API_KEY", "").strip()
USAGE_URL = "https://api.openai.com/v1/organization/usage/completions"
SOURCE_NAME = "openai_usage_api"


def to_ts(d: datetime) -> int:
  return int(d.timestamp())


def day_start_utc(s: str) -> datetime:
  return datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=timezone.utc)


def fmt_iso(d: datetime) -> str:
  return d.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def fetch_json(url: str):
  req = Request(
    url,
    headers={
      "Authorization": f"Bearer {OPENAI_API_KEY}",
      "Accept": "application/json",
      "User-Agent": "macro-monitor-openai-usage-importer",
    },
    method="GET",
  )
  with urlopen(req, timeout=60) as resp:
    return json.loads(resp.read().decode("utf-8"))


def normalize_rows(bucket: dict):
  rows = bucket.get("results") or []
  if rows:
    return rows
  # Fallback for possible single-row bucket payload.
  keys = {"input_tokens", "output_tokens", "num_model_requests", "model"}
  if any(k in bucket for k in keys):
    return [bucket]
  return []


def main():
  parser = argparse.ArgumentParser(description="Import OpenAI completion usage into monitor_token_usage")
  parser.add_argument("--start-date", default="", help="UTC date, format YYYY-MM-DD")
  parser.add_argument("--end-date", default="", help="UTC date, format YYYY-MM-DD (inclusive)")
  parser.add_argument("--days", type=int, default=2, help="Fallback lookback days if start/end not provided")
  args = parser.parse_args()

  if not OPENAI_API_KEY:
    raise RuntimeError("OPENAI_ADMIN_API_KEY is required")

  if args.start_date:
    start_dt = day_start_utc(args.start_date)
  else:
    start_dt = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=max(1, args.days))

  if args.end_date:
    end_dt = day_start_utc(args.end_date) + timedelta(days=1)
  else:
    end_dt = datetime.now(timezone.utc) + timedelta(minutes=1)

  params = {
    "start_time": to_ts(start_dt),
    "end_time": to_ts(end_dt),
    "bucket_width": "1d",
    "limit": 90,
  }

  init_db()
  delete_token_usage_by_source(SOURCE_NAME, start_iso=fmt_iso(start_dt), end_iso=fmt_iso(end_dt))

  imported = 0
  next_page = None
  while True:
    query = dict(params)
    if next_page:
      query["page"] = next_page
    payload = fetch_json(f"{USAGE_URL}?{urlencode(query)}")
    for bucket in payload.get("data") or []:
      bucket_start = int(bucket.get("start_time") or 0)
      logged_at = fmt_iso(datetime.fromtimestamp(bucket_start, tz=timezone.utc)) if bucket_start else fmt_iso(start_dt)
      for row in normalize_rows(bucket):
        input_tokens = int(row.get("input_tokens") or 0)
        output_tokens = int(row.get("output_tokens") or 0)
        total_tokens = int(row.get("total_tokens") or (input_tokens + output_tokens))
        model = str(row.get("model") or "")
        meta = {
          "requests": int(row.get("num_model_requests") or 0),
          "window_start": logged_at,
          "window_end": fmt_iso(datetime.fromtimestamp(int(bucket.get("end_time") or 0), tz=timezone.utc)) if bucket.get("end_time") else "",
        }
        log_token_usage(
          source=SOURCE_NAME,
          model=model,
          input_tokens=input_tokens,
          output_tokens=output_tokens,
          total_tokens=total_tokens,
          meta=meta,
          logged_at=logged_at,
        )
        imported += 1
    next_page = payload.get("next_page")
    if not next_page:
      break

  print(
    f"openai_usage_import_done source={SOURCE_NAME} start={fmt_iso(start_dt)} "
    f"end={fmt_iso(end_dt)} rows={imported}"
  )


if __name__ == "__main__":
  main()
