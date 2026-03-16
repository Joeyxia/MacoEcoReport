#!/usr/bin/env python3
import json

try:
  from .db import get_conn
except ImportError:
  from db import get_conn


LEVEL_ORDER = {"green": 0, "yellow": 1, "orange": 2, "red": 3}


def _query_all(conn, sql, params=()):
  return [dict(r) for r in conn.execute(sql, params).fetchall()]


def _indicator_map(snapshot):
  out = {}
  for item in (snapshot or {}).get("keyIndicatorsSnapshot") or []:
    label = str(item.get("label") or "").strip()
    if label:
      out[label] = item.get("value")
  return out


def _safe_float(v):
  try:
    return float(v)
  except Exception:
    return None


def build_alert_snapshots(as_of_date, snapshot):
  indicators = _indicator_map(snapshot)
  raw = []
  vix = _safe_float(indicators.get("VIX"))
  hy = _safe_float(indicators.get("HY_OAS"))
  yc = _safe_float(indicators.get("YC_10Y3M"))
  wti = _safe_float(indicators.get("WTI"))
  cpi = _safe_float(indicators.get("CORE_CPI_YOY"))
  unrate = _safe_float(indicators.get("UNRATE"))
  rules = [
    ("A_VIX_SPIKE", "D07", "volatility_spike", vix, "VIX > 30 red; > 24 orange; > 18 yellow"),
    ("A_HY_STRESS", "D05", "credit_spread_stress", hy, "HY OAS > 600 red; > 500 orange; > 400 yellow"),
    ("A_CURVE_INVERSION", "D01", "curve_inversion", yc, "10Y-3M < -50 orange; < 0 yellow"),
    ("A_OIL_SHOCK", "D11", "oil_shock", wti, "WTI > 100 red; > 90 orange; > 80 yellow"),
    ("A_INFLATION_REACCEL", "D03", "inflation_reaccel", cpi, "Core CPI > 3.5 orange; > 3.0 yellow"),
    ("A_LABOR_SOFTEN", "D04", "labor_softening", unrate, "Unemployment > 6 red; > 5.2 orange; > 4.8 yellow"),
  ]
  for alert_code, dim, title, value, rule in rules:
    level = "green"
    if alert_code == "A_VIX_SPIKE" and value is not None:
      level = "red" if value > 30 else ("orange" if value > 24 else ("yellow" if value > 18 else "green"))
    elif alert_code == "A_HY_STRESS" and value is not None:
      level = "red" if value > 600 else ("orange" if value > 500 else ("yellow" if value > 400 else "green"))
    elif alert_code == "A_CURVE_INVERSION" and value is not None:
      level = "orange" if value < -50 else ("yellow" if value < 0 else "green")
    elif alert_code == "A_OIL_SHOCK" and value is not None:
      level = "red" if value > 100 else ("orange" if value > 90 else ("yellow" if value > 80 else "green"))
    elif alert_code == "A_INFLATION_REACCEL" and value is not None:
      level = "orange" if value > 3.5 else ("yellow" if value > 3.0 else "green")
    elif alert_code == "A_LABOR_SOFTEN" and value is not None:
      level = "red" if value > 6 else ("orange" if value > 5.2 else ("yellow" if value > 4.8 else "green"))
    raw.append({
      "as_of_date": str(as_of_date or "").strip(),
      "dimension_code": dim,
      "alert_code": alert_code,
      "alert_level": level,
      "alert_title": title,
      "trigger_value": "" if value is None else str(value),
      "threshold_rule": rule,
      "commentary": f"{title} currently at {value}" if value is not None else f"{title} unavailable",
      "payload_json": json.dumps({"value": value}, ensure_ascii=False),
    })
  return raw


def save_alert_snapshots(as_of_date, snapshot):
  rows = build_alert_snapshots(as_of_date, snapshot)
  conn = get_conn()
  try:
    conn.execute("DELETE FROM alert_snapshots WHERE as_of_date = ?", (str(as_of_date or "").strip(),))
    for row in rows:
      conn.execute(
        """
        INSERT INTO alert_snapshots(as_of_date, dimension_code, alert_code, alert_level, alert_title, trigger_value, threshold_rule, commentary, payload_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
          row["as_of_date"],
          row["dimension_code"],
          row["alert_code"],
          row["alert_level"],
          row["alert_title"],
          row["trigger_value"],
          row["threshold_rule"],
          row["commentary"],
          row["payload_json"],
        ),
      )
    conn.commit()
    return rows
  finally:
    conn.close()


def get_latest_alerts(level=""):
  conn = get_conn()
  try:
    latest = conn.execute("SELECT MAX(as_of_date) AS d FROM alert_snapshots").fetchone()
    as_of = str(latest["d"] if latest and latest["d"] else "")
    if not as_of:
      return []
    if level:
      rows = _query_all(
        conn,
        """
        SELECT as_of_date, dimension_code, alert_code, alert_level, alert_title, trigger_value, threshold_rule, commentary, payload_json, created_at
        FROM alert_snapshots
        WHERE as_of_date = ? AND lower(alert_level) = ?
        ORDER BY created_at DESC, id DESC
        """,
        (as_of, str(level or "").strip().lower()),
      )
    else:
      rows = _query_all(
        conn,
        """
        SELECT as_of_date, dimension_code, alert_code, alert_level, alert_title, trigger_value, threshold_rule, commentary, payload_json, created_at
        FROM alert_snapshots
        WHERE as_of_date = ?
        ORDER BY created_at DESC, id DESC
        """,
        (as_of,),
      )
    rows.sort(key=lambda x: LEVEL_ORDER.get(str(x.get("alert_level") or "").lower(), 0), reverse=True)
    return rows
  finally:
    conn.close()
