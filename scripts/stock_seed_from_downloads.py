#!/usr/bin/env python3
import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
  sys.path.insert(0, str(ROOT))

from server.db import init_db
from server.stock_service import import_csv_paths


def main():
  parser = argparse.ArgumentParser(description="Seed stock prediction data from local CSV files")
  parser.add_argument("--ticker", default="PDD")
  parser.add_argument("--dir", default="/Users/joe.xia/Downloads")
  parser.add_argument("--auto-refresh", action="store_true", default=True)
  args = parser.parse_args()

  data_dir = Path(args.dir).expanduser()
  candidates = [
    data_dir / f"{args.ticker}.csv",
    data_dir / f"{args.ticker}_monthly_valuation_measures.csv",
    data_dir / f"{args.ticker}_quarterly_cash-flow.csv",
    data_dir / f"{args.ticker}_quarterly_balance-sheet.csv",
    data_dir / f"{args.ticker}_quarterly_financials.csv",
  ]
  missing = [str(p) for p in candidates if not p.exists()]
  if missing:
    raise SystemExit(f"missing files: {missing}")

  init_db()
  res = import_csv_paths(args.ticker, candidates, auto_refresh=args.auto_refresh)
  print(res)


if __name__ == "__main__":
  main()
