#!/usr/bin/env python3
import json

try:
  from .db import get_conn, now_iso
except ImportError:
  from db import get_conn, now_iso


def _query_one(conn, sql, params=()):
  row = conn.execute(sql, params).fetchone()
  return dict(row) if row else None


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


def infer_overlay(snapshot):
  indicators = _indicator_map(snapshot)
  wti = _safe_float(indicators.get("WTI")) or 0
  vix = _safe_float(indicators.get("VIX")) or 0
  dims = {str(x.get("id") or ""): float(x.get("score") or 0) for x in ((snapshot or {}).get("dimensions") or [])}
  d11 = dims.get("D11", 50)

  conflict_level = "low"
  supply_disruption_level = "low"
  shipping_risk_level = "low"
  oil_shock_scenario = "S0"
  inflation_impact = "contained"
  risk_asset_impact = "limited"
  credit_impact = "limited"
  summary = "No major geopolitical shock."

  if wti > 100 or d11 > 92:
    conflict_level = "high"
    supply_disruption_level = "high"
    shipping_risk_level = "high"
    oil_shock_scenario = "S3"
    inflation_impact = "broad upside pressure"
    risk_asset_impact = "broad risk-off"
    credit_impact = "spread widening likely"
    summary = "Systemic energy shock risk is elevated."
  elif wti > 90 or d11 > 85:
    conflict_level = "medium"
    supply_disruption_level = "medium"
    shipping_risk_level = "medium"
    oil_shock_scenario = "S2"
    inflation_impact = "headline inflation pressure"
    risk_asset_impact = "cyclical sectors vulnerable"
    credit_impact = "selective widening"
    summary = "Local supply disruption and oil pressure need monitoring."
  elif vix > 24:
    conflict_level = "elevated"
    shipping_risk_level = "medium"
    oil_shock_scenario = "S1"
    inflation_impact = "limited"
    risk_asset_impact = "sentiment shock"
    credit_impact = "mild"
    summary = "Geopolitical newsflow is hitting sentiment more than fundamentals."

  return {
    "conflict_level": conflict_level,
    "supply_disruption_level": supply_disruption_level,
    "shipping_risk_level": shipping_risk_level,
    "oil_shock_scenario": oil_shock_scenario,
    "inflation_impact": inflation_impact,
    "risk_asset_impact": risk_asset_impact,
    "credit_impact": credit_impact,
    "summary": summary,
    "payload_json": {
      "wti": wti,
      "vix": vix,
      "dimension_d11_score": d11,
    },
  }


def upsert_overlay(as_of_date, snapshot):
  inferred = infer_overlay(snapshot)
  conn = get_conn()
  try:
    ts = now_iso()
    conn.execute(
      """
      INSERT INTO geopolitical_overlays(as_of_date, conflict_level, supply_disruption_level, shipping_risk_level, oil_shock_scenario, inflation_impact, risk_asset_impact, credit_impact, summary, payload_json, created_at, updated_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(as_of_date) DO UPDATE SET
        conflict_level=excluded.conflict_level,
        supply_disruption_level=excluded.supply_disruption_level,
        shipping_risk_level=excluded.shipping_risk_level,
        oil_shock_scenario=excluded.oil_shock_scenario,
        inflation_impact=excluded.inflation_impact,
        risk_asset_impact=excluded.risk_asset_impact,
        credit_impact=excluded.credit_impact,
        summary=excluded.summary,
        payload_json=excluded.payload_json,
        updated_at=excluded.updated_at
      """,
      (
        str(as_of_date or "").strip(),
        inferred["conflict_level"],
        inferred["supply_disruption_level"],
        inferred["shipping_risk_level"],
        inferred["oil_shock_scenario"],
        inferred["inflation_impact"],
        inferred["risk_asset_impact"],
        inferred["credit_impact"],
        inferred["summary"],
        json.dumps(inferred["payload_json"], ensure_ascii=False),
        ts,
        ts,
      ),
    )
    conn.commit()
    inferred["as_of_date"] = str(as_of_date or "").strip()
    return inferred
  finally:
    conn.close()


def get_latest_overlay():
  conn = get_conn()
  try:
    row = _query_one(
      conn,
      """
      SELECT as_of_date, conflict_level, supply_disruption_level, shipping_risk_level, oil_shock_scenario, inflation_impact, risk_asset_impact, credit_impact, summary, payload_json
      FROM geopolitical_overlays
      ORDER BY as_of_date DESC, id DESC
      LIMIT 1
      """,
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
