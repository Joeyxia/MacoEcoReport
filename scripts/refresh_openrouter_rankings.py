#!/usr/bin/env python3
import argparse
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ENV_FILE = Path("/etc/macro-monitor.env")
if ENV_FILE.exists():
  try:
    for line in ENV_FILE.read_text(encoding="utf-8").splitlines():
      line = line.strip()
      if not line or line.startswith("#") or "=" not in line:
        continue
      k, v = line.split("=", 1)
      if k.strip() and v.strip() and not os.environ.get(k.strip()):
        os.environ[k.strip()] = v.strip()
  except Exception:
    pass

sys.path.insert(0, str(ROOT / "server"))
from app import _fetch_openrouter_rankings  # noqa: E402
from db import init_db, upsert_openrouter_rankings_snapshot  # noqa: E402


def parse_csv(text: str, default_values):
  raw = [x.strip() for x in str(text or "").split(",") if x.strip()]
  return raw or list(default_values)


def main():
  parser = argparse.ArgumentParser(description="Fetch OpenRouter rankings and store to database tables.")
  parser.add_argument("--views", default="day,week,month,all", help="Comma-separated views")
  parser.add_argument(
    "--categories",
    default="all,roleplay,coding,reasoning,translation",
    help="Comma-separated categories",
  )
  args = parser.parse_args()

  init_db()
  views = parse_csv(args.views, ["day", "week", "month", "all"])
  categories = parse_csv(args.categories, ["all", "roleplay", "coding", "reasoning", "translation"])

  ok = 0
  failed = 0
  for view in views:
    for category in categories:
      try:
        payload = _fetch_openrouter_rankings(view=view, category=category)
        upsert_openrouter_rankings_snapshot(payload)
        ok += 1
        print(
          f"[OK] view={view} category={category} "
          f"models={len(payload.get('models') or [])} apps={len(payload.get('apps') or [])} "
          f"providers={len(payload.get('providers') or [])} prompts={len(payload.get('prompts') or [])}"
        )
      except Exception as e:
        failed += 1
        print(f"[FAILED] view={view} category={category} error={e}")

  print(f"[DONE] success={ok} failed={failed}")
  if failed > 0:
    raise SystemExit(1)


if __name__ == "__main__":
  main()
