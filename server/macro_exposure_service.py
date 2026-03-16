#!/usr/bin/env python3
import json

try:
  from .db import get_conn, now_iso
  from .stock_service import get_latest_prediction_payload, list_tickers
except ImportError:
  from db import get_conn, now_iso
  from stock_service import get_latest_prediction_payload, list_tickers


EXPOSURE_KEYS = [
  "interest_rate_sensitivity",
  "growth_sensitivity",
  "inflation_sensitivity",
  "oil_sensitivity",
  "credit_sensitivity",
  "usd_sensitivity",
  "geopolitics_sensitivity",
  "volatility_sensitivity",
]


def _query_one(conn, sql, params=()):
  row = conn.execute(sql, params).fetchone()
  return dict(row) if row else None


def _query_all(conn, sql, params=()):
  return [dict(r) for r in conn.execute(sql, params).fetchall()]


def _norm_ticker(ticker):
  return str(ticker or "").strip().upper()


def _sector_defaults(sector, industry=""):
  s = str(sector or "").lower()
  i = str(industry or "").lower()
  if "technology" in s or "software" in i or "semiconductor" in i:
    return {
      "interest_rate_sensitivity": "high",
      "growth_sensitivity": "high",
      "inflation_sensitivity": "medium",
      "oil_sensitivity": "low",
      "credit_sensitivity": "medium",
      "usd_sensitivity": "medium",
      "geopolitics_sensitivity": "medium",
      "volatility_sensitivity": "high",
    }
  if "energy" in s:
    return {
      "interest_rate_sensitivity": "low",
      "growth_sensitivity": "medium",
      "inflation_sensitivity": "high",
      "oil_sensitivity": "high",
      "credit_sensitivity": "medium",
      "usd_sensitivity": "medium",
      "geopolitics_sensitivity": "high",
      "volatility_sensitivity": "medium",
    }
  if "financial" in s:
    return {
      "interest_rate_sensitivity": "high",
      "growth_sensitivity": "medium",
      "inflation_sensitivity": "medium",
      "oil_sensitivity": "low",
      "credit_sensitivity": "high",
      "usd_sensitivity": "medium",
      "geopolitics_sensitivity": "low",
      "volatility_sensitivity": "medium",
    }
  return {
    "interest_rate_sensitivity": "medium",
    "growth_sensitivity": "medium",
    "inflation_sensitivity": "medium",
    "oil_sensitivity": "low",
    "credit_sensitivity": "medium",
    "usd_sensitivity": "medium",
    "geopolitics_sensitivity": "low",
    "volatility_sensitivity": "medium",
  }


def ensure_macro_exposure(ticker):
  ticker = _norm_ticker(ticker)
  conn = get_conn()
  try:
    row = _query_one(conn, "SELECT * FROM stock_macro_exposures WHERE ticker = ?", (ticker,))
    if row:
      try:
        row["payload_json"] = json.loads(row.get("payload_json") or "{}")
      except Exception:
        row["payload_json"] = {}
      return row
    profile = _query_one(conn, "SELECT sector, industry FROM ticker_profiles WHERE ticker = ?", (ticker,)) or {}
    vals = _sector_defaults(profile.get("sector"), profile.get("industry"))
    ts = now_iso()
    conn.execute(
      """
      INSERT INTO stock_macro_exposures(
        ticker, interest_rate_sensitivity, growth_sensitivity, inflation_sensitivity, oil_sensitivity, credit_sensitivity, usd_sensitivity, geopolitics_sensitivity, volatility_sensitivity, payload_json, updated_at, created_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      """,
      (
        ticker,
        vals["interest_rate_sensitivity"],
        vals["growth_sensitivity"],
        vals["inflation_sensitivity"],
        vals["oil_sensitivity"],
        vals["credit_sensitivity"],
        vals["usd_sensitivity"],
        vals["geopolitics_sensitivity"],
        vals["volatility_sensitivity"],
        json.dumps({"seeded_from": "sector_defaults"}, ensure_ascii=False),
        ts,
        ts,
      ),
    )
    conn.commit()
    return ensure_macro_exposure(ticker)
  finally:
    conn.close()


def get_macro_exposure(ticker):
  return ensure_macro_exposure(ticker)


def get_macro_signal_latest(ticker):
  ticker = _norm_ticker(ticker)
  conn = get_conn()
  try:
    row = _query_one(
      conn,
      """
      SELECT as_of_date, ticker, regime_code, macro_risk_score, signal, action_bias, explanation_short, explanation_long, payload_json, created_at
      FROM stock_macro_signals
      WHERE ticker = ?
      ORDER BY as_of_date DESC, id DESC
      LIMIT 1
      """,
      (ticker,),
    )
    if not row:
      return None
    try:
      row["payload_json"] = json.loads(row.get("payload_json") or "{}")
    except Exception:
      row["payload_json"] = {}
    return row
  finally:
    conn.close()


def get_macro_signal_history(ticker, limit=60):
  ticker = _norm_ticker(ticker)
  conn = get_conn()
  try:
    rows = _query_all(
      conn,
      """
      SELECT as_of_date, ticker, regime_code, macro_risk_score, signal, action_bias, explanation_short, explanation_long, payload_json, created_at
      FROM stock_macro_signals
      WHERE ticker = ?
      ORDER BY as_of_date DESC, id DESC
      LIMIT ?
      """,
      (ticker, max(1, min(int(limit or 60), 300))),
    )
    for row in rows:
      try:
        row["payload_json"] = json.loads(row.get("payload_json") or "{}")
      except Exception:
        row["payload_json"] = {}
    return rows
  finally:
    conn.close()


def generate_stock_macro_signals(as_of_date, regime, overlay, action_bias):
  reg = str((regime or {}).get("regime_code") or "")
  oil = str((overlay or {}).get("oil_shock_scenario") or "S0")
  bias = str((action_bias or {}).get("overall_bias") or "hold")
  tickers = [str(x.get("ticker") or "").strip().upper() for x in (list_tickers() or []) if str(x.get("ticker") or "").strip()]
  out = []
  conn = get_conn()
  try:
    ts = now_iso()
    for ticker in tickers:
      exposure = ensure_macro_exposure(ticker)
      latest = get_latest_prediction_payload(ticker) or {}
      risk_score = 45.0
      for key in EXPOSURE_KEYS:
        val = str(exposure.get(key) or "").lower()
        risk_score += 6 if val == "high" else (3 if val == "medium" else 0)
      if reg == "credit_stress_risk_off":
        risk_score += 15
      elif reg == "recession_policy_easing":
        risk_score += 8
      elif reg == "liquidity_repair_growth_stable":
        risk_score -= 8
      if oil in {"S2", "S3"} and str(exposure.get("oil_sensitivity") or "").lower() in {"high", "medium"}:
        risk_score += 8
      if str(exposure.get("volatility_sensitivity") or "").lower() == "high":
        risk_score += 5
      if str(latest.get("signal") or "").upper() == "BULLISH":
        risk_score -= 6
      elif str(latest.get("signal") or "").upper() == "BEARISH":
        risk_score += 6
      risk_score = max(0, min(100, round(risk_score, 2)))

      signal = "defensive"
      if bias == "increase" and risk_score <= 45:
        signal = "favorable"
      elif bias in {"hold", "watch_to_add"} and risk_score <= 58:
        signal = "neutral"
      elif bias in {"reduce", "avoid_new_adds"} or risk_score >= 68:
        signal = "risk_off"
      action = bias
      short = f"{ticker} macro risk score {risk_score}, regime {reg}, action {action}."
      long = (
        f"Ticker {ticker} is evaluated against the latest regime ({reg}), geopolitical overlay ({oil}) "
        f"and stock macro exposure map. Combined with latest equity signal {latest.get('signal') or 'Neutral'}, "
        f"the system sets macro risk score to {risk_score} and recommends action bias '{action}'."
      )
      payload = {
        "latest_signal": latest.get("signal") or "",
        "predicted_return": latest.get("predicted_return"),
        "up_probability": latest.get("up_probability"),
        "overlay": oil,
        "bias": bias,
        "exposure": {k: exposure.get(k) for k in EXPOSURE_KEYS},
      }
      conn.execute(
        """
        INSERT INTO stock_macro_signals(as_of_date, ticker, regime_code, macro_risk_score, signal, action_bias, explanation_short, explanation_long, payload_json, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(as_of_date, ticker) DO UPDATE SET
          regime_code=excluded.regime_code,
          macro_risk_score=excluded.macro_risk_score,
          signal=excluded.signal,
          action_bias=excluded.action_bias,
          explanation_short=excluded.explanation_short,
          explanation_long=excluded.explanation_long,
          payload_json=excluded.payload_json
        """,
        (
          str(as_of_date or "").strip(),
          ticker,
          reg,
          risk_score,
          signal,
          action,
          short,
          long,
          json.dumps(payload, ensure_ascii=False),
          ts,
        ),
      )
      out.append({
        "as_of_date": str(as_of_date or "").strip(),
        "ticker": ticker,
        "regime_code": reg,
        "macro_risk_score": risk_score,
        "signal": signal,
        "action_bias": action,
        "explanation_short": short,
        "explanation_long": long,
        "payload_json": payload,
      })
    conn.commit()
    return out
  finally:
    conn.close()
