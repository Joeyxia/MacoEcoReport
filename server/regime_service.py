#!/usr/bin/env python3
import json

try:
  from .db import get_conn, now_iso
except ImportError:
  from db import get_conn, now_iso


REGIME_LABELS = {
  "liquidity_repair_growth_stable": "Liquidity Repair / Growth Stable",
  "growth_slowdown_credit_stable": "Growth Slowdown / Credit Stable",
  "inflation_reacceleration_energy_shock": "Inflation Reacceleration / Energy Shock",
  "credit_stress_risk_off": "Credit Stress / Risk Off",
  "recession_policy_easing": "Recession / Policy Easing",
}


def _query_one(conn, sql, params=()):
  row = conn.execute(sql, params).fetchone()
  return dict(row) if row else None


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


def infer_regime(snapshot):
  indicators = _indicator_map(snapshot)
  dims = {str(x.get("id") or ""): float(x.get("score") or 0) for x in ((snapshot or {}).get("dimensions") or [])}
  total_score = float((snapshot or {}).get("totalScore") or 0)
  vix = _safe_float(indicators.get("VIX"))
  hy = _safe_float(indicators.get("HY_OAS"))
  wti = _safe_float(indicators.get("WTI"))
  unrate = _safe_float(indicators.get("UNRATE"))
  yc = _safe_float(indicators.get("YC_10Y3M"))
  inflation = _safe_float(indicators.get("CORE_CPI_YOY"))

  regime_code = "growth_slowdown_credit_stable"
  confidence = 0.66
  drivers = []

  if (hy is not None and hy > 550) or (vix is not None and vix > 30) or dims.get("D10", 100) < 45:
    regime_code = "credit_stress_risk_off"
    confidence = 0.84
    drivers = ["HY OAS elevated", "VIX elevated", "financial conditions deteriorating"]
  elif (wti is not None and wti > 95) or (inflation is not None and inflation > 3.5):
    regime_code = "inflation_reacceleration_energy_shock"
    confidence = 0.77
    drivers = ["energy pressure", "inflation reacceleration"]
  elif total_score < 42 or ((unrate is not None and unrate > 5.8) and (yc is not None and yc < 0)):
    regime_code = "recession_policy_easing"
    confidence = 0.74
    drivers = ["growth deterioration", "policy easing likely"]
  elif total_score >= 68 and dims.get("D01", 0) >= 52 and dims.get("D02", 0) >= 60 and (hy is None or hy < 420):
    regime_code = "liquidity_repair_growth_stable"
    confidence = 0.71
    drivers = ["growth stable", "liquidity backdrop improving"]
  else:
    drivers = ["growth slowing but credit not yet broken", "watch labor and credit spread path"]

  return {
    "regime_code": regime_code,
    "regime_label": REGIME_LABELS.get(regime_code, regime_code),
    "confidence": confidence,
    "score": total_score,
    "score_delta": 0.0,
    "drivers": drivers,
  }


def upsert_regime_snapshot(as_of_date, snapshot):
  inferred = infer_regime(snapshot)
  conn = get_conn()
  try:
    ts = now_iso()
    prev = _query_one(
      conn,
      "SELECT score FROM regime_snapshots WHERE as_of_date < ? ORDER BY as_of_date DESC, id DESC LIMIT 1",
      (str(as_of_date or "").strip(),),
    )
    score_delta = round(float(inferred["score"] or 0) - float((prev or {}).get("score") or 0), 2) if prev else 0.0
    conn.execute(
      """
      INSERT INTO regime_snapshots(as_of_date, regime_code, regime_label, confidence, score, score_delta, drivers_json, created_at, updated_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(as_of_date, regime_code) DO UPDATE SET
        regime_label=excluded.regime_label,
        confidence=excluded.confidence,
        score=excluded.score,
        score_delta=excluded.score_delta,
        drivers_json=excluded.drivers_json,
        updated_at=excluded.updated_at
      """,
      (
        str(as_of_date or "").strip(),
        inferred["regime_code"],
        inferred["regime_label"],
        inferred["confidence"],
        inferred["score"],
        score_delta,
        json.dumps(inferred["drivers"], ensure_ascii=False),
        ts,
        ts,
      ),
    )
    conn.commit()
    inferred["as_of_date"] = str(as_of_date or "").strip()
    inferred["score_delta"] = score_delta
    return inferred
  finally:
    conn.close()


def get_latest_regime():
  conn = get_conn()
  try:
    row = _query_one(
      conn,
      """
      SELECT as_of_date, regime_code, regime_label, confidence, score, score_delta, drivers_json, created_at, updated_at
      FROM regime_snapshots
      ORDER BY as_of_date DESC, id DESC
      LIMIT 1
      """,
    )
    if not row:
      return None
    try:
      row["drivers_json"] = json.loads(row.get("drivers_json") or "[]")
    except Exception:
      row["drivers_json"] = []
    return row
  finally:
    conn.close()


def get_regime_history(days=90):
  conn = get_conn()
  try:
    rows = _query_all(
      conn,
      """
      SELECT as_of_date, regime_code, regime_label, confidence, score, score_delta, drivers_json
      FROM regime_snapshots
      ORDER BY as_of_date DESC, id DESC
      LIMIT ?
      """,
      (max(1, min(int(days or 90), 365)),),
    )
    for row in rows:
      try:
        row["drivers_json"] = json.loads(row.get("drivers_json") or "[]")
      except Exception:
        row["drivers_json"] = []
    return rows
  finally:
    conn.close()


def upsert_transmission_snapshot(as_of_date, snapshot, regime=None):
  indicators = _indicator_map(snapshot)
  dims = {str(x.get("id") or ""): float(x.get("score") or 0) for x in ((snapshot or {}).get("dimensions") or [])}
  vix = _safe_float(indicators.get("VIX")) or 0
  hy = _safe_float(indicators.get("HY_OAS")) or 0
  yc = _safe_float(indicators.get("YC_10Y3M")) or 0
  wti = _safe_float(indicators.get("WTI")) or 0
  btc = _safe_float(indicators.get("BTC")) or 0
  regime_code = str((regime or {}).get("regime_code") or "")

  rates_bias = "bull_steepening" if yc > 0 else "defensive_duration"
  equities_bias = "risk_on" if regime_code == "liquidity_repair_growth_stable" and vix < 22 else "selective"
  credit_bias = "tightening" if hy < 420 else ("stress" if hy > 550 else "neutral")
  usd_bias = "firm" if dims.get("D08", 50) < 50 else "stable"
  commodities_bias = "inflation_hedge" if wti > 90 else "neutral"
  crypto_bias = "beta_positive" if btc > 0 and vix < 25 else "fragile"
  sectors = {
    "favored": ["quality", "defensives"] if credit_bias == "stress" else ["platform tech", "select cyclicals"],
    "avoided": ["small caps", "deep cyclicals"] if credit_bias == "stress" else ["rate sensitive laggards"],
  }
  asset_classes = {
    "rates": rates_bias,
    "equities": equities_bias,
    "credit": credit_bias,
    "usd": usd_bias,
    "commodities": commodities_bias,
    "crypto": crypto_bias,
  }
  conn = get_conn()
  try:
    ts = now_iso()
    conn.execute(
      """
      INSERT INTO market_transmission_snapshots(as_of_date, rates_bias, equities_bias, credit_bias, usd_bias, commodities_bias, crypto_bias, sectors_json, asset_classes_json, created_at, updated_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(as_of_date) DO UPDATE SET
        rates_bias=excluded.rates_bias,
        equities_bias=excluded.equities_bias,
        credit_bias=excluded.credit_bias,
        usd_bias=excluded.usd_bias,
        commodities_bias=excluded.commodities_bias,
        crypto_bias=excluded.crypto_bias,
        sectors_json=excluded.sectors_json,
        asset_classes_json=excluded.asset_classes_json,
        updated_at=excluded.updated_at
      """,
      (
        str(as_of_date or "").strip(),
        rates_bias,
        equities_bias,
        credit_bias,
        usd_bias,
        commodities_bias,
        crypto_bias,
        json.dumps(sectors, ensure_ascii=False),
        json.dumps(asset_classes, ensure_ascii=False),
        ts,
        ts,
      ),
    )
    conn.commit()
    return {
      "as_of_date": str(as_of_date or "").strip(),
      "rates_bias": rates_bias,
      "equities_bias": equities_bias,
      "credit_bias": credit_bias,
      "usd_bias": usd_bias,
      "commodities_bias": commodities_bias,
      "crypto_bias": crypto_bias,
      "sectors": sectors,
      "asset_classes": asset_classes,
    }
  finally:
    conn.close()


def get_latest_transmission():
  conn = get_conn()
  try:
    row = _query_one(
      conn,
      """
      SELECT as_of_date, rates_bias, equities_bias, credit_bias, usd_bias, commodities_bias, crypto_bias, sectors_json, asset_classes_json
      FROM market_transmission_snapshots
      ORDER BY as_of_date DESC, id DESC
      LIMIT 1
      """,
    )
    if not row:
      return None
    for key in ("sectors_json", "asset_classes_json"):
      try:
        row[key] = json.loads(row.get(key) or "{}")
      except Exception:
        row[key] = {}
    return row
  finally:
    conn.close()


def upsert_action_bias(as_of_date, snapshot, regime=None, overlay=None):
  regime_code = str((regime or {}).get("regime_code") or "")
  oil_scenario = str((overlay or {}).get("oil_shock_scenario") or "S0")
  total_score = float((snapshot or {}).get("totalScore") or 0)

  overall = "hold"
  favored_styles = ["quality"]
  avoided_styles = []
  favored_sectors = ["software", "large cap quality"]
  avoided_sectors = ["high leverage cyclicals"]
  summary = "Balanced positioning."

  if regime_code == "credit_stress_risk_off":
    overall = "reduce"
    favored_styles = ["defensive", "quality"]
    avoided_styles = ["small cap beta", "cyclicals"]
    favored_sectors = ["healthcare", "utilities"]
    avoided_sectors = ["financials", "lower quality cyclicals"]
    summary = "Credit stress argues for de-risking and higher quality."
  elif regime_code == "recession_policy_easing":
    overall = "watch_to_add"
    favored_styles = ["duration", "quality growth"]
    avoided_styles = ["levered value"]
    favored_sectors = ["software", "duration assets"]
    avoided_sectors = ["deep cyclicals"]
    summary = "Watchlist mode: wait for policy easing to transmit before adding risk."
  elif regime_code == "liquidity_repair_growth_stable" and oil_scenario in {"S0", "S1"} and total_score >= 65:
    overall = "increase"
    favored_styles = ["quality growth", "select beta"]
    avoided_styles = ["crowded defensives"]
    favored_sectors = ["platform tech", "semis"]
    avoided_sectors = ["bond proxies"]
    summary = "Liquidity and growth backdrop support selective risk adding."
  elif oil_scenario in {"S2", "S3"}:
    overall = "avoid_new_adds"
    favored_styles = ["energy hedge", "quality"]
    avoided_styles = ["consumer cyclicals"]
    favored_sectors = ["energy", "defensives"]
    avoided_sectors = ["transport", "retail"]
    summary = "Energy shock scenario argues against new risk additions."

  conn = get_conn()
  try:
    ts = now_iso()
    conn.execute(
      """
      INSERT INTO daily_action_biases(as_of_date, overall_bias, favored_styles_json, avoided_styles_json, favored_sectors_json, avoided_sectors_json, summary, created_at, updated_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(as_of_date) DO UPDATE SET
        overall_bias=excluded.overall_bias,
        favored_styles_json=excluded.favored_styles_json,
        avoided_styles_json=excluded.avoided_styles_json,
        favored_sectors_json=excluded.favored_sectors_json,
        avoided_sectors_json=excluded.avoided_sectors_json,
        summary=excluded.summary,
        updated_at=excluded.updated_at
      """,
      (
        str(as_of_date or "").strip(),
        overall,
        json.dumps(favored_styles, ensure_ascii=False),
        json.dumps(avoided_styles, ensure_ascii=False),
        json.dumps(favored_sectors, ensure_ascii=False),
        json.dumps(avoided_sectors, ensure_ascii=False),
        summary,
        ts,
        ts,
      ),
    )
    conn.commit()
    return {
      "as_of_date": str(as_of_date or "").strip(),
      "overall_bias": overall,
      "favored_styles": favored_styles,
      "avoided_styles": avoided_styles,
      "favored_sectors": favored_sectors,
      "avoided_sectors": avoided_sectors,
      "summary": summary,
    }
  finally:
    conn.close()


def get_latest_action_bias():
  conn = get_conn()
  try:
    row = _query_one(
      conn,
      """
      SELECT as_of_date, overall_bias, favored_styles_json, avoided_styles_json, favored_sectors_json, avoided_sectors_json, summary
      FROM daily_action_biases
      ORDER BY as_of_date DESC, id DESC
      LIMIT 1
      """,
    )
    if not row:
      return None
    for key in ("favored_styles_json", "avoided_styles_json", "favored_sectors_json", "avoided_sectors_json"):
      try:
        row[key] = json.loads(row.get(key) or "[]")
      except Exception:
        row[key] = []
    return row
  finally:
    conn.close()
