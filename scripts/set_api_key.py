#!/usr/bin/env python3
import argparse
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "server"))
from db import init_db, upsert_api_key


def main():
  parser = argparse.ArgumentParser(description="Store API key in database")
  parser.add_argument("--service", required=True, help="service name, e.g. fred")
  parser.add_argument("--key", default="", help="API key value")
  args = parser.parse_args()

  key = (args.key or "").strip() or os.environ.get("API_KEY", "").strip()
  if not key:
    raise RuntimeError("missing key: provide --key or API_KEY env")

  init_db()
  upsert_api_key(args.service, key)
  print(f"ok service={args.service.lower()} key_len={len(key)}")


if __name__ == "__main__":
  main()
