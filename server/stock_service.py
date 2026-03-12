#!/usr/bin/env python3
import csv
import io
import json
import math
import os
import re
import tempfile
import urllib.parse
import urllib.request
from datetime import date, datetime, timedelta
from pathlib import Path

try:
  from .db import get_conn, now_iso
except ImportError:
  from db import get_conn, now_iso

try:
  import numpy as np
  from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
except Exception:
  np = None
  RandomForestClassifier = None
  RandomForestRegressor = None

try:
  from playwright.sync_api import sync_playwright
except Exception:
  sync_playwright = None

ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = Path(os.environ.get("STOCK_OUTPUT_DIR", str(ROOT / "outputs")))
MODEL_VERSION = "rf-walkforward-v1"
RF_ESTIMATORS = max(20, int(os.environ.get("STOCK_RF_ESTIMATORS", "60")))
RF_MAX_DEPTH = max(3, int(os.environ.get("STOCK_RF_MAX_DEPTH", "5")))
RF_MIN_SAMPLES_LEAF = max(1, int(os.environ.get("STOCK_RF_MIN_SAMPLES_LEAF", "2")))
RF_REFIT_EVERY = max(1, int(os.environ.get("STOCK_RF_REFIT_EVERY", "3")))
YAHOO_TIMEOUT_MS = max(10_000, int(os.environ.get("YAHOO_TIMEOUT_MS", "120000")))
YAHOO_HEADLESS = str(os.environ.get("YAHOO_HEADLESS", "true")).strip().lower() in {"1", "true", "yes", "on"}
YAHOO_ALLOW_GUEST_FALLBACK = str(os.environ.get("YAHOO_ALLOW_GUEST_FALLBACK", "true")).strip().lower() in {"1", "true", "yes", "on"}
YAHOO_STORAGE_DIR = Path(os.environ.get("YAHOO_STORAGE_DIR", str(ROOT / "data" / "yahoo_storage")))
YAHOO_START_DATE = str(os.environ.get("YAHOO_HISTORY_START_DATE", "2000-01-01")).strip() or "2000-01-01"

FILE_TYPE_PRICE = "price"
FILE_TYPE_VALUATION = "valuation"
FILE_TYPE_CASH_FLOW = "cash_flow"
FILE_TYPE_BALANCE_SHEET = "balance_sheet"
FILE_TYPE_FINANCIALS = "financials"
REQUIRED_FILE_TYPES = {FILE_TYPE_PRICE, FILE_TYPE_VALUATION, FILE_TYPE_CASH_FLOW, FILE_TYPE_BALANCE_SHEET, FILE_TYPE_FINANCIALS}

FEATURE_NAMES = [
  "ret_1m",
  "ret_3m",
  "ret_6m",
  "ret_12m",
  "vol_3m",
  "vol_6m",
  "ma3_dev",
  "ma6_dev",
  "ma12_dev",
  "momentum",
  "trend",
  "reversal",
  "pe",
  "ps",
  "pb",
  "ev_to_mc",
  "revenue_growth_yoy",
  "net_income_growth_yoy",
  "ocf_growth_yoy",
  "liabilities_to_assets",
  "asset_growth_yoy",
]


def _ensure_output_dir():
  OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def _parse_number(v):
  if v is None:
    return None
  s = str(v).strip()
  if not s:
    return None
  negative = False
  if s.startswith("(") and s.endswith(")"):
    negative = True
    s = s[1:-1]
  s = s.replace(",", "").replace("$", "").replace("%", "").replace("\u00a0", "")
  try:
    num = float(s)
  except Exception:
    return None
  return -num if negative else num


def _parse_date(v):
  if v is None:
    return None
  s = str(v).strip()
  if not s:
    return None
  for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y", "%Y/%m/%d"):
    try:
      return datetime.strptime(s, fmt).date()
    except Exception:
      pass
  return None


def _month_key(dt: date):
  return f"{dt.year:04d}-{dt.month:02d}-01"


def _last_day_of_month(dt: date):
  if dt.month == 12:
    return date(dt.year + 1, 1, 1) - timedelta(days=1)
  return date(dt.year, dt.month + 1, 1) - timedelta(days=1)


def _safe_ticker(ticker: str):
  t = str(ticker or "").strip().upper()
  if not t:
    raise ValueError("ticker is required")
  return t


def detect_file_type(file_name: str, headers=None):
  name = str(file_name or "").strip().lower()
  headers = [str(x or "").strip() for x in (headers or [])]
  header_set = {h.lower() for h in headers}
  if {"date", "open", "high", "low", "close"}.issubset(header_set):
    return FILE_TYPE_PRICE
  if "valuation" in name:
    return FILE_TYPE_VALUATION
  if "cash-flow" in name or "cash_flow" in name:
    return FILE_TYPE_CASH_FLOW
  if "balance-sheet" in name or "balance_sheet" in name:
    return FILE_TYPE_BALANCE_SHEET
  if "financials" in name:
    return FILE_TYPE_FINANCIALS
  if "name" in header_set and any(_parse_date(h) for h in headers):
    return FILE_TYPE_VALUATION
  return "unknown"


def _upsert_ticker_profile(conn, ticker: str):
  ts = now_iso()
  conn.execute(
    """
    INSERT INTO ticker_profiles (ticker, company_name, exchange, sector, industry, currency, is_active, created_at, updated_at)
    VALUES (?, ?, '', '', '', 'USD', 1, ?, ?)
    ON CONFLICT(ticker) DO UPDATE SET
      is_active=1,
      updated_at=excluded.updated_at
    """,
    (ticker, ticker, ts, ts),
  )


def _create_uploaded_file_row(conn, ticker: str, file_name: str, file_type: str, upload_source: str = "monitor-web"):
  ts = now_iso()
  cur = conn.execute(
    """
    INSERT INTO uploaded_files (ticker, file_name, file_type, upload_source, file_status, uploaded_at, imported_at, notes)
    VALUES (?, ?, ?, ?, 'uploaded', ?, NULL, '')
    """,
    (ticker, file_name, file_type, upload_source, ts),
  )
  return int(cur.lastrowid or 0)


def _finish_uploaded_file_row(conn, row_id: int, status: str, notes: str = ""):
  ts = now_iso()
  conn.execute(
    """
    UPDATE uploaded_files
    SET file_status = ?, imported_at = ?, notes = ?
    WHERE id = ?
    """,
    (status, ts, str(notes or "")[:800], int(row_id or 0)),
  )


def _import_price_csv(conn, ticker: str, file_name: str, rows):
  ts = now_iso()
  count = 0
  for row in rows:
    trade_dt = _parse_date(row.get("Date"))
    if not trade_dt:
      continue
    conn.execute(
      """
      INSERT INTO stock_prices (ticker, trade_date, open, high, low, close, adj_close, volume, source_file, created_at, updated_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(ticker, trade_date) DO UPDATE SET
        open=excluded.open,
        high=excluded.high,
        low=excluded.low,
        close=excluded.close,
        adj_close=excluded.adj_close,
        volume=excluded.volume,
        source_file=excluded.source_file,
        updated_at=excluded.updated_at
      """,
      (
        ticker,
        trade_dt.isoformat(),
        _parse_number(row.get("Open")),
        _parse_number(row.get("High")),
        _parse_number(row.get("Low")),
        _parse_number(row.get("Close")),
        _parse_number(row.get("Adj Close")),
        _parse_number(row.get("Volume")),
        file_name,
        ts,
        ts,
      ),
    )
    count += 1
  return count


def _import_wide_metric_csv(conn, ticker: str, file_name: str, rows, statement_type: str = ""):
  ts = now_iso()
  count = 0
  for row in rows:
    metric_name = str(row.get("name") or "").strip().replace("\t", "")
    if not metric_name:
      continue
    for k, v in row.items():
      if str(k).strip().lower() in {"name", "ttm"}:
        continue
      dt = _parse_date(k)
      if not dt:
        continue
      val = _parse_number(v)
      if val is None:
        continue
      if statement_type:
        conn.execute(
          """
          INSERT INTO stock_financials (ticker, report_date, statement_type, metric_name, metric_value, source_file, created_at, updated_at)
          VALUES (?, ?, ?, ?, ?, ?, ?, ?)
          ON CONFLICT(ticker, report_date, statement_type, metric_name) DO UPDATE SET
            metric_value=excluded.metric_value,
            source_file=excluded.source_file,
            updated_at=excluded.updated_at
          """,
          (ticker, dt.isoformat(), statement_type, metric_name, val, file_name, ts, ts),
        )
      else:
        conn.execute(
          """
          INSERT INTO stock_valuations (ticker, valuation_date, metric_name, metric_value, source_file, created_at, updated_at)
          VALUES (?, ?, ?, ?, ?, ?, ?)
          ON CONFLICT(ticker, valuation_date, metric_name) DO UPDATE SET
            metric_value=excluded.metric_value,
            source_file=excluded.source_file,
            updated_at=excluded.updated_at
          """,
          (ticker, dt.isoformat(), metric_name, val, file_name, ts, ts),
        )
      count += 1
  return count


def _parse_csv_stream(file_storage):
  raw = file_storage.read()
  file_storage.seek(0)
  text = raw.decode("utf-8-sig", errors="ignore")
  reader = csv.DictReader(io.StringIO(text))
  rows = list(reader)
  return reader.fieldnames or [], rows


def import_csv_uploads(ticker: str, uploaded_files, auto_refresh: bool = False):
  ticker = _safe_ticker(ticker)
  conn = get_conn()
  _upsert_ticker_profile(conn, ticker)
  imported = []
  failed = []
  total_rows = 0
  try:
    for f in uploaded_files:
      file_name = str(getattr(f, "filename", "") or "").strip()
      if not file_name:
        continue
      headers, rows = _parse_csv_stream(f)
      file_type = detect_file_type(file_name, headers)
      upload_id = _create_uploaded_file_row(conn, ticker, file_name, file_type)
      try:
        if file_type == FILE_TYPE_PRICE:
          rows_count = _import_price_csv(conn, ticker, file_name, rows)
        elif file_type == FILE_TYPE_VALUATION:
          rows_count = _import_wide_metric_csv(conn, ticker, file_name, rows, statement_type="")
        elif file_type == FILE_TYPE_CASH_FLOW:
          rows_count = _import_wide_metric_csv(conn, ticker, file_name, rows, statement_type=FILE_TYPE_CASH_FLOW)
        elif file_type == FILE_TYPE_BALANCE_SHEET:
          rows_count = _import_wide_metric_csv(conn, ticker, file_name, rows, statement_type=FILE_TYPE_BALANCE_SHEET)
        elif file_type == FILE_TYPE_FINANCIALS:
          rows_count = _import_wide_metric_csv(conn, ticker, file_name, rows, statement_type=FILE_TYPE_FINANCIALS)
        else:
          raise ValueError("unsupported_file_type")
        _finish_uploaded_file_row(conn, upload_id, "imported", f"rows={rows_count}")
        imported.append({"file": file_name, "type": file_type, "rows": rows_count})
        total_rows += rows_count
      except Exception as e:
        _finish_uploaded_file_row(conn, upload_id, "failed", str(e))
        failed.append({"file": file_name, "type": file_type, "error": str(e)})
    conn.commit()
  finally:
    conn.close()

  run_result = None
  if auto_refresh and not failed:
    run_result = train_and_refresh_ticker(ticker)

  return {
    "ticker": ticker,
    "importedCount": len(imported),
    "failedCount": len(failed),
    "totalRows": total_rows,
    "imported": imported,
    "failed": failed,
    "modelRun": run_result,
  }


def inspect_csv_uploads(uploaded_files):
  out = []
  for f in (uploaded_files or []):
    file_name = str(getattr(f, "filename", "") or "").strip()
    if not file_name:
      continue
    try:
      headers, rows = _parse_csv_stream(f)
      file_type = detect_file_type(file_name, headers)
      out.append(
        {
          "file": file_name,
          "type": file_type,
          "rowCount": len(rows),
          "columns": headers,
          "ok": file_type in REQUIRED_FILE_TYPES,
        }
      )
    except Exception as e:
      out.append(
        {
          "file": file_name,
          "type": "unknown",
          "rowCount": 0,
          "columns": [],
          "ok": False,
          "error": str(e)[:180],
        }
      )
  return out


class _PathUpload:
  def __init__(self, path: Path):
    self.path = Path(path)
    self.filename = self.path.name
    self._raw = self.path.read_bytes()

  def read(self):
    return self._raw

  def seek(self, _pos: int):
    return 0


def import_csv_paths(ticker: str, csv_paths, auto_refresh: bool = False):
  files = [_PathUpload(Path(p)) for p in (csv_paths or [])]
  return import_csv_uploads(ticker=ticker, uploaded_files=files, auto_refresh=auto_refresh)


def _safe_ymd(s: str, fallback: str = "2000-01-01"):
  x = str(s or "").strip()
  try:
    datetime.strptime(x, "%Y-%m-%d")
    return x
  except Exception:
    return fallback


def _maybe_click(page, pattern: str):
  try:
    btn = page.get_by_role("button", name=re.compile(pattern, re.I))
    if btn.count() > 0:
      btn.first.click(timeout=4000)
      return True
  except Exception:
    pass
  return False


def _accept_cookies(page):
  for p in ["accept all", "i agree", "accept", "同意", "接受"]:
    if _maybe_click(page, p):
      return


def _is_login_page(page):
  u = str(getattr(page, "url", "") or "").lower()
  return "login.yahoo.com" in u


def _ensure_yahoo_login(page):
  username = str(os.environ.get("YAHOO_USERNAME") or os.environ.get("YAHOO_EMAIL") or "").strip()
  password = str(os.environ.get("YAHOO_PASSWORD") or "").strip()
  if not username or not password:
    raise ValueError("missing_yahoo_credentials_env")
  page.goto("https://finance.yahoo.com/", wait_until="domcontentloaded", timeout=YAHOO_TIMEOUT_MS)
  _accept_cookies(page)
  if not _is_login_page(page):
    try:
      sign_in = page.get_by_role("link", name=re.compile("sign in", re.I))
      if sign_in.count() > 0:
        sign_in.first.click(timeout=6000)
    except Exception:
      pass
  if _is_login_page(page):
    page.locator('input[name="username"]').first.fill(username, timeout=YAHOO_TIMEOUT_MS)
    page.locator("#login-signin").first.click(timeout=YAHOO_TIMEOUT_MS)
    page.locator('input[name="password"]').first.wait_for(timeout=YAHOO_TIMEOUT_MS)
    page.locator('input[name="password"]').first.fill(password, timeout=YAHOO_TIMEOUT_MS)
    page.locator("#login-signin").first.click(timeout=YAHOO_TIMEOUT_MS)
    page.wait_for_timeout(2500)
  if _is_login_page(page):
    raise RuntimeError("yahoo_login_failed_or_needs_2fa")


def _extract_crumb(page):
  html = page.content()
  m = re.search(r'"CrumbStore":\{"crumb":"(.*?)"\}', html)
  if not m:
    raise RuntimeError("yahoo_crumb_not_found")
  crumb = m.group(1).encode("utf-8").decode("unicode_escape")
  return crumb


def _download_historical_csv(page, context, ticker: str, start_date: str, out_path: Path):
  start_dt = datetime.strptime(_safe_ymd(start_date, YAHOO_START_DATE), "%Y-%m-%d")
  period1 = int(start_dt.timestamp())
  period2 = int(datetime.utcnow().timestamp())
  hist_url = f"https://finance.yahoo.com/quote/{ticker}/history?p={ticker}"
  page.goto(hist_url, wait_until="domcontentloaded", timeout=YAHOO_TIMEOUT_MS)
  _accept_cookies(page)
  try:
    crumb = _extract_crumb(page)
    url = (
      f"https://query1.finance.yahoo.com/v7/finance/download/{urllib.parse.quote(ticker)}"
      f"?period1={period1}&period2={period2}&interval=1d&events=history&includeAdjustedClose=true&crumb={urllib.parse.quote(crumb)}"
    )
    r = context.request.get(url, timeout=YAHOO_TIMEOUT_MS)
    if not r.ok:
      raise RuntimeError(f"yahoo_historical_download_failed_{r.status}")
    out_path.write_bytes(r.body())
    if out_path.stat().st_size < 64:
      raise RuntimeError("yahoo_historical_csv_too_small")
    return
  except Exception:
    pass

  chart_url = (
    f"https://query1.finance.yahoo.com/v8/finance/chart/{urllib.parse.quote(ticker)}"
    f"?period1={period1}&period2={period2}&interval=1d&events=history&includeAdjustedClose=true"
  )
  req = urllib.request.Request(chart_url, headers={"User-Agent": "Mozilla/5.0"})
  with urllib.request.urlopen(req, timeout=max(20, int(YAHOO_TIMEOUT_MS / 1000))) as resp:
    payload = json.loads(resp.read().decode("utf-8", errors="ignore"))
  result = (((payload or {}).get("chart") or {}).get("result") or [None])[0] or {}
  ts = result.get("timestamp") or []
  quote = (((result.get("indicators") or {}).get("quote") or [None])[0] or {})
  adj = (((result.get("indicators") or {}).get("adjclose") or [None])[0] or {}).get("adjclose") or []
  opens = quote.get("open") or []
  highs = quote.get("high") or []
  lows = quote.get("low") or []
  closes = quote.get("close") or []
  vols = quote.get("volume") or []
  if not ts:
    raise RuntimeError("yahoo_chart_no_data")
  with out_path.open("w", encoding="utf-8", newline="") as f:
    w = csv.writer(f)
    w.writerow(["Date", "Open", "High", "Low", "Close", "Adj Close", "Volume"])
    for i, tsv in enumerate(ts):
      try:
        d = datetime.utcfromtimestamp(int(tsv)).date().isoformat()
      except Exception:
        continue
      w.writerow(
        [
          d,
          opens[i] if i < len(opens) else "",
          highs[i] if i < len(highs) else "",
          lows[i] if i < len(lows) else "",
          closes[i] if i < len(closes) else "",
          adj[i] if i < len(adj) else (closes[i] if i < len(closes) else ""),
          vols[i] if i < len(vols) else "",
        ]
      )
  if out_path.stat().st_size < 64:
    raise RuntimeError("yahoo_historical_csv_too_small")


def _extract_best_table_rows(page, heading_hint: str = ""):
  hint = str(heading_hint or "").strip().lower()
  rows = page.evaluate(
    """(hint) => {
      const txt = (v) => (v || '').replace(/\\s+/g, ' ').trim();
      const parseTable = (tbl) => {
        const out = [];
        tbl.querySelectorAll('tr').forEach((tr) => {
          const cells = [...tr.querySelectorAll('th,td')].map((c) => txt(c.textContent));
          if (cells.some(Boolean)) out.push(cells);
        });
        return out;
      };
      const score = (rows) => {
        if (!rows || rows.length < 2) return 0;
        const cols = Math.max(...rows.map((r) => r.length));
        return rows.length * cols;
      };
      const allTables = [...document.querySelectorAll('table')];
      let candidates = allTables;
      if (hint) {
        const hs = [...document.querySelectorAll('h1,h2,h3,h4,section,article,div,span')].filter((n) => txt(n.textContent).toLowerCase().includes(hint));
        for (const h of hs) {
          const box = h.closest('section,article,div') || h.parentElement;
          const t = box ? [...box.querySelectorAll('table')] : [];
          if (t.length) {
            candidates = t;
            break;
          }
        }
      }
      let best = null;
      let bestScore = -1;
      candidates.forEach((tbl) => {
        const r = parseTable(tbl);
        const s = score(r);
        if (s > bestScore) { best = r; bestScore = s; }
      });
      return best || [];
    }""",
    hint,
  )
  return rows or []


def _write_rows_to_csv(rows, out_path: Path):
  if not rows:
    raise RuntimeError("yahoo_table_empty")
  max_cols = max(len(r) for r in rows)
  normalized = []
  for r in rows:
    x = list(r) + [""] * (max_cols - len(r))
    normalized.append(x)
  header = normalized[0]
  if header:
    header[0] = "name"
  with out_path.open("w", encoding="utf-8", newline="") as f:
    w = csv.writer(f)
    for r in normalized:
      w.writerow(r)


def _camel_to_title(k: str):
  s = re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", str(k or ""))
  s = s.replace("_", " ").strip()
  return s[:1].upper() + s[1:] if s else s


def _yahoo_json_get(url: str, context=None):
  if context is not None:
    r = context.request.get(url, timeout=YAHOO_TIMEOUT_MS)
    if not r.ok:
      raise RuntimeError(f"yahoo_json_http_{r.status}")
    try:
      return r.json()
    except Exception:
      return json.loads((r.text() or "{}"))
  req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
  with urllib.request.urlopen(req, timeout=max(20, int(YAHOO_TIMEOUT_MS / 1000))) as resp:
    return json.loads(resp.read().decode("utf-8", errors="ignore"))


def _fallback_quarterly_statement_csv(ticker: str, statement_type: str, out_path: Path, context=None):
  module_map = {
    FILE_TYPE_FINANCIALS: "incomeStatementHistoryQuarterly",
    FILE_TYPE_CASH_FLOW: "cashflowStatementHistoryQuarterly",
    FILE_TYPE_BALANCE_SHEET: "balanceSheetHistoryQuarterly",
  }
  mod = module_map.get(statement_type)
  if not mod:
    return False
  url = f"https://query1.finance.yahoo.com/v10/finance/quoteSummary/{urllib.parse.quote(ticker)}?modules={mod}"
  payload = _yahoo_json_get(url, context=context)
  result = (((payload or {}).get("quoteSummary") or {}).get("result") or [None])[0] or {}
  block = result.get(mod) or {}
  raw_list = (
    block.get("incomeStatementHistory")
    or block.get("cashflowStatements")
    or block.get("balanceSheetStatements")
    or []
  )
  if not raw_list:
    return False
  date_cols = []
  metrics = {}
  for row in raw_list:
    dt_raw = ((row.get("endDate") or {}).get("raw"))
    if not dt_raw:
      continue
    dt = datetime.utcfromtimestamp(int(dt_raw)).date().isoformat()
    date_cols.append(dt)
    for k, v in row.items():
      if k in {"maxAge", "endDate"}:
        continue
      val = v.get("raw") if isinstance(v, dict) else None
      if val is None:
        continue
      name = _camel_to_title(k)
      metrics.setdefault(name, {})[dt] = val
  date_cols = sorted(set(date_cols), reverse=True)
  if not date_cols or not metrics:
    return False
  with out_path.open("w", encoding="utf-8", newline="") as f:
    w = csv.writer(f)
    w.writerow(["name", *date_cols])
    for name, m in metrics.items():
      w.writerow([name] + [m.get(d, "") for d in date_cols])
  return out_path.exists() and out_path.stat().st_size > 64


def _fallback_monthly_valuation_csv(ticker: str, start_date: str, out_path: Path, context=None):
  start_dt = datetime.strptime(_safe_ymd(start_date, YAHOO_START_DATE), "%Y-%m-%d")
  period1 = int(start_dt.timestamp())
  period2 = int(datetime.utcnow().timestamp())
  chart_url = (
    f"https://query1.finance.yahoo.com/v8/finance/chart/{urllib.parse.quote(ticker)}"
    f"?period1={period1}&period2={period2}&interval=1mo&events=history&includeAdjustedClose=true"
  )
  chart = _yahoo_json_get(chart_url, context=context)
  result = (((chart or {}).get("chart") or {}).get("result") or [None])[0] or {}
  ts = result.get("timestamp") or []
  quote = (((result.get("indicators") or {}).get("quote") or [None])[0] or {})
  closes = quote.get("close") or []
  if not ts:
    return False
  modules = "defaultKeyStatistics,financialData,summaryDetail,price"
  qsum = _yahoo_json_get(f"https://query1.finance.yahoo.com/v10/finance/quoteSummary/{urllib.parse.quote(ticker)}?modules={modules}", context=context)
  qres = (((qsum or {}).get("quoteSummary") or {}).get("result") or [None])[0] or {}
  dks = qres.get("defaultKeyStatistics") or {}
  fdata = qres.get("financialData") or {}
  sdet = qres.get("summaryDetail") or {}
  price = qres.get("price") or {}
  shares = ((dks.get("sharesOutstanding") or {}).get("raw")) or ((price.get("sharesOutstanding") or {}).get("raw"))
  cur_mc = ((price.get("marketCap") or {}).get("raw")) or ((dks.get("marketCap") or {}).get("raw"))
  cur_ev = ((dks.get("enterpriseValue") or {}).get("raw")) or ((fdata.get("enterpriseValue") or {}).get("raw"))
  pe = ((sdet.get("trailingPE") or {}).get("raw")) or ((dks.get("trailingPE") or {}).get("raw"))
  ps = ((sdet.get("priceToSalesTrailing12Months") or {}).get("raw"))
  pb = ((sdet.get("priceToBook") or {}).get("raw")) or ((dks.get("priceToBook") or {}).get("raw"))
  cur_price = ((price.get("regularMarketPrice") or {}).get("raw")) or None
  dates = []
  mc_vals = {}
  ev_vals = {}
  pe_vals = {}
  ps_vals = {}
  pb_vals = {}
  for i, tsv in enumerate(ts):
    c = closes[i] if i < len(closes) else None
    if c is None:
      continue
    d = datetime.utcfromtimestamp(int(tsv)).date().isoformat()
    dates.append(d)
    if shares:
      mc = float(shares) * float(c)
    elif cur_mc and cur_price and float(cur_price) != 0:
      mc = float(cur_mc) * (float(c) / float(cur_price))
    else:
      mc = None
    if mc is not None:
      mc_vals[d] = mc
    if cur_ev and cur_mc and mc is not None:
      ev_vals[d] = float(mc) + float(cur_ev) - float(cur_mc)
    elif cur_ev:
      ev_vals[d] = float(cur_ev)
    if pe is not None:
      pe_vals[d] = float(pe)
    if ps is not None:
      ps_vals[d] = float(ps)
    if pb is not None:
      pb_vals[d] = float(pb)
  dates = sorted(set(dates), reverse=True)
  if not dates:
    return False
  with out_path.open("w", encoding="utf-8", newline="") as f:
    w = csv.writer(f)
    w.writerow(["name", *dates])
    w.writerow(["EnterpriseValue"] + [ev_vals.get(d, "") for d in dates])
    w.writerow(["MarketCap"] + [mc_vals.get(d, "") for d in dates])
    w.writerow(["Trailing PE"] + [pe_vals.get(d, "") for d in dates])
    w.writerow(["Price to Sales Ratio"] + [ps_vals.get(d, "") for d in dates])
    w.writerow(["Price to Book Ratio"] + [pb_vals.get(d, "") for d in dates])
  return out_path.exists() and out_path.stat().st_size > 64


def _download_statement_table_csv(page, url: str, out_path: Path, quarterly: bool = False, heading_hint: str = "", ticker: str = "", statement_type: str = "", start_date: str = "2000-01-01", context=None):
  page.goto(url, wait_until="domcontentloaded", timeout=YAHOO_TIMEOUT_MS)
  _accept_cookies(page)
  if quarterly:
    _maybe_click(page, "quarterly")
    page.wait_for_timeout(1200)
  # Prefer Yahoo's own CSV download button if present.
  try:
    with page.expect_download(timeout=7000) as dl_info:
      btn = page.get_by_role("button", name=re.compile("download", re.I))
      if btn.count() > 0:
        btn.first.click()
      else:
        raise RuntimeError("download_button_not_found")
    dl = dl_info.value
    dl.save_as(str(out_path))
    if out_path.exists() and out_path.stat().st_size > 64:
      return
  except Exception:
    pass
  rows = _extract_best_table_rows(page, heading_hint=heading_hint)
  if rows:
    _write_rows_to_csv(rows, out_path)
    return
  ok = False
  if statement_type == FILE_TYPE_VALUATION:
    ok = _fallback_monthly_valuation_csv(ticker, start_date, out_path, context=context)
  elif statement_type in {FILE_TYPE_FINANCIALS, FILE_TYPE_CASH_FLOW, FILE_TYPE_BALANCE_SHEET}:
    ok = _fallback_quarterly_statement_csv(ticker, statement_type, out_path, context=context)
  if not ok:
    raise RuntimeError("yahoo_table_empty")


def fetch_yahoo_csv_and_train(ticker: str, start_date: str = "2000-01-01", auto_refresh: bool = True):
  ticker = _safe_ticker(ticker)
  if sync_playwright is None:
    raise RuntimeError("playwright_not_installed")
  start_date = _safe_ymd(start_date, YAHOO_START_DATE)
  YAHOO_STORAGE_DIR.mkdir(parents=True, exist_ok=True)
  run_tag = datetime.utcnow().strftime("%Y%m%d%H%M%S")
  with tempfile.TemporaryDirectory(prefix=f"yahoo_{ticker}_{run_tag}_") as tmp:
    tmp_dir = Path(tmp)
    files = {
      FILE_TYPE_PRICE: tmp_dir / f"{ticker}.csv",
      FILE_TYPE_VALUATION: tmp_dir / f"{ticker}_monthly_valuation_measures.csv",
      FILE_TYPE_FINANCIALS: tmp_dir / f"{ticker}_quarterly_financials.csv",
      FILE_TYPE_CASH_FLOW: tmp_dir / f"{ticker}_quarterly_cash-flow.csv",
      FILE_TYPE_BALANCE_SHEET: tmp_dir / f"{ticker}_quarterly_balance-sheet.csv",
    }
    with sync_playwright() as p:
      context = p.chromium.launch_persistent_context(
        str(YAHOO_STORAGE_DIR),
        headless=YAHOO_HEADLESS,
        accept_downloads=True,
      )
      page = context.new_page()
      login_status = "logged_in"
      try:
        _ensure_yahoo_login(page)
      except Exception as e:
        if not YAHOO_ALLOW_GUEST_FALLBACK:
          raise
        login_status = f"guest_fallback:{str(e)[:80]}"
      _download_historical_csv(page, context, ticker, start_date, files[FILE_TYPE_PRICE])
      _download_statement_table_csv(
        page,
        f"https://finance.yahoo.com/quote/{ticker}/key-statistics?p={ticker}",
        files[FILE_TYPE_VALUATION],
        quarterly=False,
        heading_hint="valuation",
        ticker=ticker,
        statement_type=FILE_TYPE_VALUATION,
        start_date=start_date,
        context=context,
      )
      _download_statement_table_csv(
        page,
        f"https://finance.yahoo.com/quote/{ticker}/financials?p={ticker}",
        files[FILE_TYPE_FINANCIALS],
        quarterly=True,
        heading_hint="breakdown",
        ticker=ticker,
        statement_type=FILE_TYPE_FINANCIALS,
        start_date=start_date,
        context=context,
      )
      _download_statement_table_csv(
        page,
        f"https://finance.yahoo.com/quote/{ticker}/cash-flow?p={ticker}",
        files[FILE_TYPE_CASH_FLOW],
        quarterly=True,
        heading_hint="breakdown",
        ticker=ticker,
        statement_type=FILE_TYPE_CASH_FLOW,
        start_date=start_date,
        context=context,
      )
      _download_statement_table_csv(
        page,
        f"https://finance.yahoo.com/quote/{ticker}/balance-sheet?p={ticker}",
        files[FILE_TYPE_BALANCE_SHEET],
        quarterly=True,
        heading_hint="breakdown",
        ticker=ticker,
        statement_type=FILE_TYPE_BALANCE_SHEET,
        start_date=start_date,
        context=context,
      )
      context.close()
    result = import_csv_paths(ticker=ticker, csv_paths=[str(p) for p in files.values()], auto_refresh=bool(auto_refresh))
    result["source"] = "yahoo_playwright"
    result["loginStatus"] = login_status
    result["downloadedFiles"] = [{"file": p.name} for p in files.values()]
    result["startDate"] = start_date
    return result


def _fetch_price_monthly(conn, ticker: str):
  rows = conn.execute(
    """
    SELECT trade_date, close, volume
    FROM stock_prices
    WHERE ticker = ?
    ORDER BY trade_date ASC
    """,
    (ticker,),
  ).fetchall()
  by_month = {}
  for r in rows:
    dt = _parse_date(r["trade_date"])
    close = _parse_number(r["close"])
    if not dt or close is None:
      continue
    k = _month_key(dt)
    cur = by_month.get(k)
    if (not cur) or (dt > cur["date"]):
      by_month[k] = {
        "month": k,
        "date": dt,
        "close": float(close),
        "volume": float(_parse_number(r["volume"]) or 0.0),
      }
  out = list(by_month.values())
  out.sort(key=lambda x: x["month"])
  return out


def _fetch_long_metrics(conn, ticker: str, table_name: str, date_col: str, where=""):
  rows = conn.execute(
    f"""
    SELECT {date_col} AS dt, metric_name AS metric, metric_value AS value
    FROM {table_name}
    WHERE ticker = ? {where}
    ORDER BY {date_col} ASC
    """,
    (ticker,),
  ).fetchall()
  data = {}
  for r in rows:
    dt = _parse_date(r["dt"])
    metric = str(r["metric"] or "").strip().lower()
    val = _parse_number(r["value"])
    if not dt or not metric or val is None:
      continue
    data.setdefault(metric, []).append((dt, float(val)))
  return data


def _latest_before(metric_series, dt: date):
  if not metric_series:
    return None
  out = None
  for k, v in metric_series:
    if k <= dt:
      out = v
    else:
      break
  return out


def _pct_change(current, prior):
  if current is None or prior is None:
    return 0.0
  if abs(prior) < 1e-12:
    return 0.0
  return (current / prior) - 1.0


def _stdev(vals):
  if not vals or len(vals) < 2:
    return 0.0
  m = sum(vals) / len(vals)
  var = sum((x - m) ** 2 for x in vals) / (len(vals) - 1)
  return math.sqrt(max(var, 0.0))


def _build_feature_rows(conn, ticker: str):
  monthly = _fetch_price_monthly(conn, ticker)
  if len(monthly) < 24:
    return []
  valuation = _fetch_long_metrics(conn, ticker, "stock_valuations", "valuation_date")
  financials = _fetch_long_metrics(conn, ticker, "stock_financials", "report_date")

  ev_series = valuation.get("enterprisevalue", [])
  mc_series = valuation.get("marketcap", [])
  pe_series = valuation.get("trailing pe", []) or valuation.get("pe ratio", []) or valuation.get("pe", [])
  ps_series = valuation.get("price to sales ratio", []) or valuation.get("ps", [])
  pb_series = valuation.get("price to book ratio", []) or valuation.get("pb", [])
  rev_series = financials.get("total revenue", [])
  ni_series = financials.get("net income", []) or financials.get("net income common stockholders", [])
  ocf_series = financials.get("operating cash flow", [])
  assets_series = financials.get("total assets", [])
  liabilities_series = financials.get("total liabilities net minority interest", []) or financials.get("total liabilities", [])

  rows = []
  rets = []
  for i, m in enumerate(monthly):
    if i == 0:
      rets.append(0.0)
    else:
      rets.append(_pct_change(m["close"], monthly[i - 1]["close"]))

  for i in range(12, len(monthly) - 1):
    m = monthly[i]
    dt = _last_day_of_month(_parse_date(m["month"]))
    r1 = rets[i]
    r3 = _pct_change(m["close"], monthly[i - 3]["close"]) if i >= 3 else 0.0
    r6 = _pct_change(m["close"], monthly[i - 6]["close"]) if i >= 6 else 0.0
    r12 = _pct_change(m["close"], monthly[i - 12]["close"]) if i >= 12 else 0.0
    vol3 = _stdev(rets[max(1, i - 2):i + 1])
    vol6 = _stdev(rets[max(1, i - 5):i + 1])
    ma3 = sum(x["close"] for x in monthly[i - 2:i + 1]) / 3.0
    ma6 = sum(x["close"] for x in monthly[i - 5:i + 1]) / 6.0
    ma12 = sum(x["close"] for x in monthly[i - 11:i + 1]) / 12.0

    ev = _latest_before(ev_series, dt)
    mc = _latest_before(mc_series, dt)
    pe = _latest_before(pe_series, dt)
    ps = _latest_before(ps_series, dt)
    pb = _latest_before(pb_series, dt)

    lag_dt = dt - timedelta(days=45)
    lag_prev = lag_dt - timedelta(days=365)
    rev = _latest_before(rev_series, lag_dt)
    rev_prev = _latest_before(rev_series, lag_prev)
    ni = _latest_before(ni_series, lag_dt)
    ni_prev = _latest_before(ni_series, lag_prev)
    ocf = _latest_before(ocf_series, lag_dt)
    ocf_prev = _latest_before(ocf_series, lag_prev)
    assets = _latest_before(assets_series, lag_dt)
    assets_prev = _latest_before(assets_series, lag_prev)
    liabilities = _latest_before(liabilities_series, lag_dt)

    feature = {
      "month": m["month"],
      "ret_1m": r1,
      "ret_3m": r3,
      "ret_6m": r6,
      "ret_12m": r12,
      "vol_3m": vol3,
      "vol_6m": vol6,
      "ma3_dev": _pct_change(m["close"], ma3),
      "ma6_dev": _pct_change(m["close"], ma6),
      "ma12_dev": _pct_change(m["close"], ma12),
      "momentum": r6 - r1,
      "trend": _pct_change(m["close"], ma3) + _pct_change(m["close"], ma6),
      "reversal": -r1,
      "pe": (pe or 0.0),
      "ps": (ps or 0.0),
      "pb": (pb or 0.0),
      "ev_to_mc": (ev / mc - 1.0) if (ev and mc and abs(mc) > 1e-12) else 0.0,
      "revenue_growth_yoy": _pct_change(rev, rev_prev),
      "net_income_growth_yoy": _pct_change(ni, ni_prev),
      "ocf_growth_yoy": _pct_change(ocf, ocf_prev),
      "liabilities_to_assets": (liabilities / assets) if (liabilities is not None and assets and abs(assets) > 1e-12) else 0.0,
      "asset_growth_yoy": _pct_change(assets, assets_prev),
      "target_return": _pct_change(monthly[i + 1]["close"], m["close"]),
    }
    rows.append(feature)
  return rows


def _to_signal(pred_ret: float):
  if pred_ret > 0.03:
    return "Bullish"
  if pred_ret < -0.03:
    return "Bearish"
  return "Neutral"


def _fallback_walkforward(features):
  # If sklearn is unavailable, keep system running with deterministic fallback.
  out = []
  for row in features:
    pred = (
      0.18 * row["ret_1m"] +
      0.25 * row["ret_3m"] +
      0.25 * row["ret_6m"] +
      0.08 * row["ret_12m"] +
      0.08 * row["revenue_growth_yoy"] +
      0.07 * row["net_income_growth_yoy"] +
      0.06 * row["ocf_growth_yoy"] -
      0.08 * row["vol_3m"] -
      0.05 * row["vol_6m"]
    )
    pred = max(min(pred, 0.30), -0.30)
    up = 1.0 / (1.0 + math.exp(-pred * 18.0))
    out.append((pred, up, 1.0 - up, _to_signal(pred)))
  imps = sorted([(k, abs(float(features[-1].get(k) or 0.0))) for k in FEATURE_NAMES], key=lambda x: x[1], reverse=True)
  return out, imps


def _rf_walkforward(features):
  if not features:
    return [], []

  if np is None or RandomForestRegressor is None or RandomForestClassifier is None:
    return _fallback_walkforward(features)

  X = np.array([[float(row.get(f) or 0.0) for f in FEATURE_NAMES] for row in features], dtype=float)
  y_ret = np.array([float(row.get("target_return") or 0.0) for row in features], dtype=float)
  y_cls = np.array([1 if y > 0 else 0 for y in y_ret], dtype=int)

  n = len(features)
  min_train = 24
  if n <= min_train:
    min_train = max(12, n // 2)
  predictions = []
  last_importances = None

  reg = None
  clf = None
  for i in range(min_train, n):
    x_train = X[:i]
    y_train_ret = y_ret[:i]
    y_train_cls = y_cls[:i]

    should_refit = (reg is None) or ((i - min_train) % RF_REFIT_EVERY == 0)
    if should_refit:
      reg = RandomForestRegressor(
        n_estimators=RF_ESTIMATORS,
        max_depth=RF_MAX_DEPTH,
        min_samples_leaf=RF_MIN_SAMPLES_LEAF,
        random_state=42,
        n_jobs=1,
      )
      reg.fit(x_train, y_train_ret)
    pred = float(reg.predict(X[i:i + 1])[0])

    if len(set(y_train_cls.tolist())) >= 2:
      if should_refit or clf is None:
        clf = RandomForestClassifier(
          n_estimators=RF_ESTIMATORS,
          max_depth=RF_MAX_DEPTH,
          min_samples_leaf=RF_MIN_SAMPLES_LEAF,
          random_state=42,
          n_jobs=1,
          class_weight="balanced_subsample",
        )
        clf.fit(x_train, y_train_cls)
      probs = clf.predict_proba(X[i:i + 1])[0]
      up_prob = float(probs[1]) if len(probs) > 1 else float(probs[0])
    else:
      up_prob = 1.0 if int(y_train_cls[-1]) == 1 else 0.0

    pred = max(min(pred, 0.30), -0.30)
    predictions.append((pred, up_prob, 1.0 - up_prob, _to_signal(pred), i))
    last_importances = reg.feature_importances_

  # For initial warmup periods, fill with fallback to keep full timeline.
  if min_train > 0:
    warmup = _fallback_walkforward(features[:min_train])[0]
    for idx, item in enumerate(warmup):
      predictions.insert(idx, (*item, idx))

  if last_importances is None:
    imps = _fallback_walkforward(features)[1]
  else:
    imps = sorted([(FEATURE_NAMES[i], float(last_importances[i])) for i in range(len(FEATURE_NAMES))], key=lambda x: x[1], reverse=True)

  # Strip idx helper.
  cleaned = [(p[0], p[1], p[2], p[3]) for p in predictions]
  return cleaned, imps


def _calc_backtest_metrics(results):
  if not results:
    return {
      "direction_accuracy": 0.0,
      "precision": 0.0,
      "recall": 0.0,
      "f1": 0.0,
      "mae": 0.0,
      "rmse": 0.0,
      "strategy_cagr": 0.0,
      "buy_hold_cagr": 0.0,
      "max_drawdown": 0.0,
      "sharpe_ratio": 0.0,
    }
  n = len(results)
  hit = 0
  tp = fp = fn = 0
  abs_err = []
  sq_err = []
  strat = []
  bh = []
  for r in results:
    pr = float(r["predicted_return"] or 0.0)
    ar = float(r["actual_return"] or 0.0)
    if (pr >= 0 and ar >= 0) or (pr < 0 and ar < 0):
      hit += 1
    pred_up = pr > 0
    act_up = ar > 0
    if pred_up and act_up:
      tp += 1
    elif pred_up and not act_up:
      fp += 1
    elif (not pred_up) and act_up:
      fn += 1
    abs_err.append(abs(pr - ar))
    sq_err.append((pr - ar) ** 2)
    strat.append(float(r["strategy_return"] or 0.0))
    bh.append(ar)

  precision = (tp / (tp + fp)) if (tp + fp) > 0 else 0.0
  recall = (tp / (tp + fn)) if (tp + fn) > 0 else 0.0
  f1 = (2 * precision * recall / (precision + recall)) if (precision + recall) > 0 else 0.0
  mae = sum(abs_err) / n
  rmse = math.sqrt(sum(sq_err) / n)

  def _cagr(returns):
    wealth = 1.0
    for x in returns:
      wealth *= (1.0 + x)
    years = max(n / 12.0, 1e-9)
    return wealth ** (1.0 / years) - 1.0

  def _mdd(returns):
    wealth = 1.0
    peak = 1.0
    mdd = 0.0
    for x in returns:
      wealth *= (1.0 + x)
      peak = max(peak, wealth)
      dd = (wealth / peak) - 1.0
      mdd = min(mdd, dd)
    return mdd

  strat_mean = sum(strat) / n
  strat_std = _stdev(strat)
  sharpe = (strat_mean / strat_std * math.sqrt(12.0)) if strat_std > 1e-12 else 0.0
  return {
    "direction_accuracy": hit / n,
    "precision": precision,
    "recall": recall,
    "f1": f1,
    "mae": mae,
    "rmse": rmse,
    "strategy_cagr": _cagr(strat),
    "buy_hold_cagr": _cagr(bh),
    "max_drawdown": _mdd(strat),
    "sharpe_ratio": sharpe,
  }


def train_and_refresh_ticker(ticker: str):
  ticker = _safe_ticker(ticker)
  _ensure_output_dir()
  conn = get_conn()
  ts = now_iso()
  run_id = 0
  try:
    cur = conn.execute(
      """
      INSERT INTO model_runs (ticker, run_time, model_version, train_start, status, notes)
      VALUES (?, ?, ?, ?, 'running', '')
      """,
      (ticker, ts, MODEL_VERSION, ts),
    )
    run_id = int(cur.lastrowid or 0)

    features = _build_feature_rows(conn, ticker)
    if len(features) < 18:
      conn.execute(
        "UPDATE model_runs SET train_end=?, sample_count=?, feature_count=?, status='failed', notes=? WHERE id=?",
        (now_iso(), len(features), 0, "insufficient_monthly_samples", run_id),
      )
      conn.commit()
      return {"ok": False, "ticker": ticker, "runId": run_id, "error": "insufficient_monthly_samples"}

    preds, feature_importances = _rf_walkforward(features)
    results = []
    cum_strat = 1.0
    cum_bh = 1.0
    for row, pred_row in zip(features, preds):
      pred, up, down, signal = pred_row
      ar = float(row.get("target_return") or 0.0)
      sr = ar if signal == "Bullish" else 0.0
      cum_strat *= (1.0 + sr)
      cum_bh *= (1.0 + ar)
      results.append(
        {
          "month": row["month"],
          "predicted_return": float(pred),
          "up_probability": float(max(0.0, min(1.0, up))),
          "down_probability": float(max(0.0, min(1.0, down))),
          "signal": signal,
          "actual_return": ar,
          "strategy_return": sr,
          "cumulative_strategy": cum_strat - 1.0,
          "cumulative_buy_hold": cum_bh - 1.0,
          "feature_row": row,
        }
      )

    metrics = _calc_backtest_metrics(results)
    latest = results[-1]
    latest_month = latest["month"]
    top_feature_rows = [(n, float(v)) for n, v in feature_importances[:12]]

    conn.execute("DELETE FROM prediction_results WHERE ticker = ?", (ticker,))
    for r in results:
      conn.execute(
        """
        INSERT INTO prediction_results
        (ticker, prediction_month, predicted_return, up_probability, down_probability, signal, actual_return, strategy_return, cumulative_strategy, cumulative_buy_hold, run_id, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
          ticker,
          r["month"],
          r["predicted_return"],
          r["up_probability"],
          r["down_probability"],
          r["signal"],
          r["actual_return"],
          r["strategy_return"],
          r["cumulative_strategy"],
          r["cumulative_buy_hold"],
          run_id,
          now_iso(),
        ),
      )

    conn.execute("DELETE FROM feature_importance WHERE ticker = ?", (ticker,))
    for i, (name, imp) in enumerate(top_feature_rows, start=1):
      conn.execute(
        """
        INSERT INTO feature_importance (ticker, run_id, prediction_month, feature_name, importance, rank_num, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (ticker, run_id, latest_month, name, float(imp), i, now_iso()),
      )

    conn.execute(
      """
      INSERT INTO latest_signals
      (ticker, latest_month, predicted_return, up_probability, down_probability, signal, direction_accuracy, precision_score, recall_score, f1_score, mae, rmse, strategy_cagr, buy_hold_cagr, max_drawdown, sharpe_ratio, run_id, updated_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(ticker) DO UPDATE SET
        latest_month=excluded.latest_month,
        predicted_return=excluded.predicted_return,
        up_probability=excluded.up_probability,
        down_probability=excluded.down_probability,
        signal=excluded.signal,
        direction_accuracy=excluded.direction_accuracy,
        precision_score=excluded.precision_score,
        recall_score=excluded.recall_score,
        f1_score=excluded.f1_score,
        mae=excluded.mae,
        rmse=excluded.rmse,
        strategy_cagr=excluded.strategy_cagr,
        buy_hold_cagr=excluded.buy_hold_cagr,
        max_drawdown=excluded.max_drawdown,
        sharpe_ratio=excluded.sharpe_ratio,
        run_id=excluded.run_id,
        updated_at=excluded.updated_at
      """,
      (
        ticker,
        latest_month,
        latest["predicted_return"],
        latest["up_probability"],
        latest["down_probability"],
        latest["signal"],
        metrics["direction_accuracy"],
        metrics["precision"],
        metrics["recall"],
        metrics["f1"],
        metrics["mae"],
        metrics["rmse"],
        metrics["strategy_cagr"],
        metrics["buy_hold_cagr"],
        metrics["max_drawdown"],
        metrics["sharpe_ratio"],
        run_id,
        now_iso(),
      ),
    )

    conn.execute(
      """
      UPDATE model_runs
      SET train_end=?, sample_count=?, feature_count=?, status='success', notes=?
      WHERE id=?
      """,
      (
        now_iso(),
        len(features),
        len(top_feature_rows),
        json.dumps({"latestMonth": latest_month, "mode": "rf_walkforward"}, ensure_ascii=False),
        run_id,
      ),
    )
    conn.commit()
    _export_outputs(ticker, features, results, metrics, top_feature_rows, run_id, ts)
    return {"ok": True, "ticker": ticker, "runId": run_id, "latestMonth": latest_month}
  except Exception as e:
    if run_id:
      conn.execute(
        "UPDATE model_runs SET train_end=?, status='failed', notes=? WHERE id=?",
        (now_iso(), str(e)[:500], run_id),
      )
      conn.commit()
    return {"ok": False, "ticker": ticker, "runId": run_id, "error": str(e)}
  finally:
    conn.close()


def _export_outputs(ticker, features, results, metrics, top_feature_rows, run_id: int, generated_at: str):
  _ensure_output_dir()
  t = ticker.upper()
  feature_file = OUTPUT_DIR / f"monthly_feature_table_{t}.csv"
  pred_file = OUTPUT_DIR / f"prediction_backtest_{t}.csv"
  summary_file = OUTPUT_DIR / f"prediction_summary_{t}.json"
  fi_file = OUTPUT_DIR / f"feature_importance_{t}.csv"
  latest_file = OUTPUT_DIR / f"latest_prediction_{t}.json"

  if features:
    with feature_file.open("w", newline="", encoding="utf-8") as f:
      w = csv.DictWriter(f, fieldnames=list(features[0].keys()))
      w.writeheader()
      w.writerows(features)

  if results:
    with pred_file.open("w", newline="", encoding="utf-8") as f:
      fieldnames = ["month", "predicted_return", "actual_return", "up_probability", "down_probability", "signal", "strategy_return", "cumulative_strategy", "cumulative_buy_hold"]
      w = csv.DictWriter(f, fieldnames=fieldnames)
      w.writeheader()
      for r in results:
        w.writerow({k: r.get(k) for k in fieldnames})

  with fi_file.open("w", newline="", encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow(["feature_name", "importance", "rank"])
    for i, (name, imp) in enumerate(top_feature_rows, start=1):
      w.writerow([name, imp, i])

  latest = results[-1] if results else {}
  summary = {
    "ticker": t,
    "run_id": run_id,
    "generated_at": generated_at,
    "latest_month": latest.get("month", ""),
    "metrics": metrics,
    "latest_prediction": {
      "predicted_return": latest.get("predicted_return", 0.0),
      "up_probability": latest.get("up_probability", 0.0),
      "down_probability": latest.get("down_probability", 0.0),
      "signal": latest.get("signal", "Neutral"),
    },
    "top_features": [{"feature_name": n, "importance": v, "rank": i + 1} for i, (n, v) in enumerate(top_feature_rows)],
  }
  summary_file.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
  latest_file.write_text(json.dumps(summary.get("latest_prediction") or {}, ensure_ascii=False, indent=2), encoding="utf-8")


def _query_one(conn, sql, params=()):
  row = conn.execute(sql, params).fetchone()
  return dict(row) if row else None


def list_tickers():
  conn = get_conn()
  rows = conn.execute(
    "SELECT ticker, company_name, exchange, sector, industry, currency, is_active, updated_at FROM ticker_profiles WHERE is_active = 1 ORDER BY ticker ASC"
  ).fetchall()
  conn.close()
  return [dict(r) for r in rows]


def get_ticker_profile(ticker: str):
  ticker = _safe_ticker(ticker)
  conn = get_conn()
  row = _query_one(
    conn,
    "SELECT ticker, company_name, exchange, sector, industry, currency, is_active, created_at, updated_at FROM ticker_profiles WHERE ticker = ? LIMIT 1",
    (ticker,),
  )
  conn.close()
  return row


def get_latest_prediction_payload(ticker: str):
  ticker = _safe_ticker(ticker)
  conn = get_conn()
  sig = _query_one(
    conn,
    """
    SELECT ticker, latest_month, predicted_return, up_probability, down_probability, signal,
           direction_accuracy, precision_score, recall_score, f1_score, mae, rmse,
           strategy_cagr, buy_hold_cagr, max_drawdown, sharpe_ratio, run_id, updated_at
    FROM latest_signals
    WHERE ticker = ?
    LIMIT 1
    """,
    (ticker,),
  )
  profile = _query_one(
    conn,
    "SELECT ticker, company_name, exchange, sector, industry, currency FROM ticker_profiles WHERE ticker = ? LIMIT 1",
    (ticker,),
  )
  features = conn.execute(
    """
    SELECT feature_name, importance, rank_num
    FROM feature_importance
    WHERE ticker = ?
    ORDER BY rank_num ASC, id ASC
    LIMIT 12
    """,
    (ticker,),
  ).fetchall()
  conn.close()
  if not sig:
    return None
  return {
    "ticker": ticker,
    "company_name": (profile or {}).get("company_name") or ticker,
    "latest_month": sig.get("latest_month") or "",
    "predicted_return": float(sig.get("predicted_return") or 0.0),
    "up_probability": float(sig.get("up_probability") or 0.0),
    "down_probability": float(sig.get("down_probability") or 0.0),
    "signal": sig.get("signal") or "Neutral",
    "top_features": [{"feature_name": f["feature_name"], "importance": float(f["importance"] or 0.0), "rank": int(f["rank_num"] or 0)} for f in features],
    "model_metrics": {
      "direction_accuracy": float(sig.get("direction_accuracy") or 0.0),
      "precision": float(sig.get("precision_score") or 0.0),
      "recall": float(sig.get("recall_score") or 0.0),
      "f1": float(sig.get("f1_score") or 0.0),
      "mae": float(sig.get("mae") or 0.0),
      "rmse": float(sig.get("rmse") or 0.0),
      "strategy_cagr": float(sig.get("strategy_cagr") or 0.0),
      "buy_hold_cagr": float(sig.get("buy_hold_cagr") or 0.0),
      "max_drawdown": float(sig.get("max_drawdown") or 0.0),
      "sharpe_ratio": float(sig.get("sharpe_ratio") or 0.0),
    },
    "run_id": int(sig.get("run_id") or 0),
    "updated_at": sig.get("updated_at") or "",
  }


def get_backtest_summary(ticker: str):
  p = get_latest_prediction_payload(ticker)
  if not p:
    return None
  return p.get("model_metrics") or {}


def get_backtest_history(ticker: str, limit: int = 120):
  ticker = _safe_ticker(ticker)
  conn = get_conn()
  rows = conn.execute(
    """
    SELECT prediction_month, predicted_return, actual_return, up_probability, down_probability, signal, strategy_return, cumulative_strategy, cumulative_buy_hold
    FROM prediction_results
    WHERE ticker = ?
    ORDER BY prediction_month DESC
    LIMIT ?
    """,
    (ticker, max(1, int(limit))),
  ).fetchall()
  conn.close()
  out = []
  for r in rows:
    out.append(
      {
        "month": r["prediction_month"],
        "predicted_return": float(r["predicted_return"] or 0.0),
        "actual_return": float(r["actual_return"] or 0.0),
        "up_probability": float(r["up_probability"] or 0.0),
        "down_probability": float(r["down_probability"] or 0.0),
        "signal": r["signal"] or "Neutral",
        "strategy_return": float(r["strategy_return"] or 0.0),
        "cumulative_strategy": float(r["cumulative_strategy"] or 0.0),
        "cumulative_buy_hold": float(r["cumulative_buy_hold"] or 0.0),
      }
    )
  return out


def get_latest_features(ticker: str, limit: int = 10):
  ticker = _safe_ticker(ticker)
  conn = get_conn()
  rows = conn.execute(
    """
    SELECT feature_name, importance, rank_num, prediction_month
    FROM feature_importance
    WHERE ticker = ?
    ORDER BY rank_num ASC, id ASC
    LIMIT ?
    """,
    (ticker, max(1, int(limit))),
  ).fetchall()
  conn.close()
  return [
    {
      "feature_name": r["feature_name"],
      "importance": float(r["importance"] or 0.0),
      "rank": int(r["rank_num"] or 0),
      "prediction_month": r["prediction_month"] or "",
    }
    for r in rows
  ]


def get_admin_data_status():
  conn = get_conn()

  def _count(table_name):
    row = conn.execute(f"SELECT COUNT(*) AS n FROM {table_name}").fetchone()
    return int((row["n"] if row else 0) or 0)

  counts = {
    "ticker_profiles": _count("ticker_profiles"),
    "stock_prices": _count("stock_prices"),
    "stock_valuations": _count("stock_valuations"),
    "stock_financials": _count("stock_financials"),
    "uploaded_files": _count("uploaded_files"),
    "model_runs": _count("model_runs"),
    "prediction_results": _count("prediction_results"),
    "feature_importance": _count("feature_importance"),
    "latest_signals": _count("latest_signals"),
  }
  latest_dates = {
    "price": (_query_one(conn, "SELECT MAX(trade_date) AS v FROM stock_prices") or {}).get("v") or "",
    "valuation": (_query_one(conn, "SELECT MAX(valuation_date) AS v FROM stock_valuations") or {}).get("v") or "",
    "financial": (_query_one(conn, "SELECT MAX(report_date) AS v FROM stock_financials") or {}).get("v") or "",
    "upload": (_query_one(conn, "SELECT MAX(uploaded_at) AS v FROM uploaded_files") or {}).get("v") or "",
    "model_run": (_query_one(conn, "SELECT MAX(run_time) AS v FROM model_runs") or {}).get("v") or "",
  }
  conn.close()
  return {"counts": counts, "latest_dates": latest_dates}


def get_ticker_admin_status(ticker: str):
  ticker = _safe_ticker(ticker)
  conn = get_conn()
  file_rows = conn.execute(
    """
    SELECT file_type, MAX(uploaded_at) AS latest_upload
    FROM uploaded_files
    WHERE ticker = ? AND file_status = 'imported'
    GROUP BY file_type
    """,
    (ticker,),
  ).fetchall()
  imported_types = {str(r["file_type"] or "") for r in file_rows}
  latest_import = ""
  for r in file_rows:
    v = str(r["latest_upload"] or "")
    if v > latest_import:
      latest_import = v
  latest_run = _query_one(conn, "SELECT run_time, status FROM model_runs WHERE ticker = ? ORDER BY run_time DESC LIMIT 1", (ticker,))
  sig = _query_one(conn, "SELECT updated_at, latest_month, signal FROM latest_signals WHERE ticker = ? LIMIT 1", (ticker,))
  conn.close()
  return {
    "ticker": ticker,
    "missing_file_types": sorted(list(REQUIRED_FILE_TYPES - imported_types)),
    "uploaded_file_types": sorted(list(imported_types)),
    "is_uploaded_complete": REQUIRED_FILE_TYPES.issubset(imported_types),
    "is_imported": bool(imported_types),
    "is_trained": bool(latest_run and latest_run.get("status") == "success"),
    "has_latest_signal": bool(sig),
    "latest_import_time": latest_import,
    "latest_train_time": (latest_run or {}).get("run_time") or "",
    "latest_train_status": (latest_run or {}).get("status") or "",
    "latest_signal_time": (sig or {}).get("updated_at") or "",
    "latest_signal_month": (sig or {}).get("latest_month") or "",
    "latest_signal": (sig or {}).get("signal") or "",
  }


def list_upload_history(limit: int = 100, ticker: str = ""):
  conn = get_conn()
  if ticker:
    rows = conn.execute(
      """
      SELECT id, ticker, file_name, file_type, upload_source, file_status, uploaded_at, imported_at, notes
      FROM uploaded_files
      WHERE ticker = ?
      ORDER BY uploaded_at DESC, id DESC
      LIMIT ?
      """,
      (_safe_ticker(ticker), max(1, int(limit))),
    ).fetchall()
  else:
    rows = conn.execute(
      """
      SELECT id, ticker, file_name, file_type, upload_source, file_status, uploaded_at, imported_at, notes
      FROM uploaded_files
      ORDER BY uploaded_at DESC, id DESC
      LIMIT ?
      """,
      (max(1, int(limit)),),
    ).fetchall()
  conn.close()
  return [dict(r) for r in rows]


def list_model_runs(limit: int = 100, ticker: str = ""):
  conn = get_conn()
  if ticker:
    rows = conn.execute(
      """
      SELECT id, ticker, run_time, model_version, train_start, train_end, sample_count, feature_count, status, notes
      FROM model_runs
      WHERE ticker = ?
      ORDER BY run_time DESC, id DESC
      LIMIT ?
      """,
      (_safe_ticker(ticker), max(1, int(limit))),
    ).fetchall()
  else:
    rows = conn.execute(
      """
      SELECT id, ticker, run_time, model_version, train_start, train_end, sample_count, feature_count, status, notes
      FROM model_runs
      ORDER BY run_time DESC, id DESC
      LIMIT ?
      """,
      (max(1, int(limit)),),
    ).fetchall()
  conn.close()
  return [dict(r) for r in rows]


def get_stock_form_rows(form_name: str, limit: int = 500):
  key = str(form_name or "").strip().lower()
  conn = get_conn()
  table_map = {
    "ticker_profiles": ("SELECT * FROM ticker_profiles ORDER BY ticker ASC LIMIT ?", (max(1, int(limit)),)),
    "stock_prices": ("SELECT * FROM stock_prices ORDER BY trade_date DESC, id DESC LIMIT ?", (max(1, int(limit)),)),
    "stock_valuations": ("SELECT * FROM stock_valuations ORDER BY valuation_date DESC, id DESC LIMIT ?", (max(1, int(limit)),)),
    "stock_financials": ("SELECT * FROM stock_financials ORDER BY report_date DESC, id DESC LIMIT ?", (max(1, int(limit)),)),
    "uploaded_files": ("SELECT * FROM uploaded_files ORDER BY uploaded_at DESC, id DESC LIMIT ?", (max(1, int(limit)),)),
    "model_runs": ("SELECT * FROM model_runs ORDER BY run_time DESC, id DESC LIMIT ?", (max(1, int(limit)),)),
    "prediction_results": ("SELECT * FROM prediction_results ORDER BY prediction_month DESC, id DESC LIMIT ?", (max(1, int(limit)),)),
    "feature_importance": ("SELECT * FROM feature_importance ORDER BY created_at DESC, id DESC LIMIT ?", (max(1, int(limit)),)),
    "latest_signals": ("SELECT * FROM latest_signals ORDER BY updated_at DESC, id DESC LIMIT ?", (max(1, int(limit)),)),
  }
  if key not in table_map:
    conn.close()
    return []
  sql, params = table_map[key]
  rows = conn.execute(sql, params).fetchall()
  conn.close()
  return [dict(r) for r in rows]
