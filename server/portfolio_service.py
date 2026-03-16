#!/usr/bin/env python3
import json

try:
  from .db import get_conn, now_iso
  from .macro_exposure_service import get_macro_exposure, get_macro_signal_latest
except ImportError:
  from db import get_conn, now_iso
  from macro_exposure_service import get_macro_exposure, get_macro_signal_latest


def _query_one(conn, sql, params=()):
  row = conn.execute(sql, params).fetchone()
  return dict(row) if row else None


def _query_all(conn, sql, params=()):
  return [dict(r) for r in conn.execute(sql, params).fetchall()]


def list_watchlists(user_email=""):
  conn = get_conn()
  try:
    if user_email:
      rows = _query_all(
        conn,
        "SELECT id, user_email, list_name, status, created_at, updated_at FROM portfolio_watchlists WHERE lower(user_email)=? ORDER BY updated_at DESC, id DESC",
        (str(user_email or "").strip().lower(),),
      )
    else:
      rows = _query_all(
        conn,
        "SELECT id, user_email, list_name, status, created_at, updated_at FROM portfolio_watchlists ORDER BY updated_at DESC, id DESC",
      )
    return rows
  finally:
    conn.close()


def create_watchlist(user_email, list_name):
  conn = get_conn()
  try:
    ts = now_iso()
    conn.execute(
      """
      INSERT INTO portfolio_watchlists(user_email, list_name, status, created_at, updated_at)
      VALUES (?, ?, 'active', ?, ?)
      ON CONFLICT(user_email, list_name) DO UPDATE SET
        status='active',
        updated_at=excluded.updated_at
      """,
      (str(user_email or "").strip().lower(), str(list_name or "").strip(), ts, ts),
    )
    conn.commit()
    return _query_one(
      conn,
      "SELECT id, user_email, list_name, status, created_at, updated_at FROM portfolio_watchlists WHERE lower(user_email)=? AND list_name=?",
      (str(user_email or "").strip().lower(), str(list_name or "").strip()),
    )
  finally:
    conn.close()


def list_positions(watchlist_id):
  conn = get_conn()
  try:
    rows = _query_all(
      conn,
      """
      SELECT id, watchlist_id, ticker, quantity, cost_basis, market_value, note, created_at, updated_at
      FROM portfolio_positions
      WHERE watchlist_id = ?
      ORDER BY updated_at DESC, id DESC
      """,
      (int(watchlist_id),),
    )
    out = []
    for row in rows:
      ticker = str(row.get("ticker") or "").upper()
      row["macro_exposure"] = get_macro_exposure(ticker)
      row["macro_signal"] = get_macro_signal_latest(ticker)
      out.append(row)
    return out
  finally:
    conn.close()


def add_position(watchlist_id, ticker, quantity=0, cost_basis=None, note=""):
  conn = get_conn()
  try:
    ts = now_iso()
    ticker = str(ticker or "").strip().upper()
    conn.execute(
      """
      INSERT INTO portfolio_positions(watchlist_id, ticker, quantity, cost_basis, market_value, note, created_at, updated_at)
      VALUES (?, ?, ?, ?, NULL, ?, ?, ?)
      ON CONFLICT(watchlist_id, ticker) DO UPDATE SET
        quantity=excluded.quantity,
        cost_basis=excluded.cost_basis,
        note=excluded.note,
        updated_at=excluded.updated_at
      """,
      (int(watchlist_id), ticker, float(quantity or 0), cost_basis, str(note or ""), ts, ts),
    )
    conn.commit()
    return _query_one(
      conn,
      "SELECT id, watchlist_id, ticker, quantity, cost_basis, market_value, note, created_at, updated_at FROM portfolio_positions WHERE watchlist_id=? AND ticker=?",
      (int(watchlist_id), ticker),
    )
  finally:
    conn.close()


def build_portfolio_risk_summary(user_email, watchlist_id=None):
  watchlists = list_watchlists(user_email)
  if watchlist_id is not None:
    wid = int(watchlist_id)
    watchlists = [x for x in watchlists if int(x.get("id") or 0) == wid]
  all_positions = []
  for wl in watchlists:
    all_positions.extend(list_positions(wl["id"]))
  if not all_positions:
    return {
      "user_email": str(user_email or "").strip().lower(),
      "watchlists": watchlists,
      "positions": [],
      "summary": {
        "count": 0,
        "average_macro_risk_score": 0,
        "top_risk_positions": [],
        "top_benefit_positions": [],
        "advice": "No positions yet. Add holdings to generate macro + stock combined risk advice.",
      },
    }
  scored = []
  for pos in all_positions:
    signal = pos.get("macro_signal") or {}
    score = float(signal.get("macro_risk_score") or 0)
    scored.append({
      "ticker": pos.get("ticker"),
      "macro_risk_score": score,
      "action_bias": signal.get("action_bias") or "",
      "signal": signal.get("signal") or "",
      "quantity": pos.get("quantity"),
      "note": pos.get("note") or "",
    })
  scored_sorted = sorted(scored, key=lambda x: x["macro_risk_score"], reverse=True)
  avg = round(sum(x["macro_risk_score"] for x in scored) / len(scored), 2)
  high_risk = [x for x in scored if float(x.get("macro_risk_score") or 0) >= 70]
  risk_off = [x for x in scored if str(x.get("signal") or "").lower() == "risk_off"]
  action_count = {}
  for x in scored:
    action = str(x.get("action_bias") or "").strip().lower() or "unknown"
    action_count[action] = action_count.get(action, 0) + 1
  dominant_action = sorted(action_count.items(), key=lambda kv: kv[1], reverse=True)[0][0] if action_count else "neutral"
  top_risk = scored_sorted[:5]
  top_benefit = list(reversed(scored_sorted[-5:]))
  advice = (
    f"Composite macro-stock risk score is {avg}. "
    f"High-risk names (>=70): {len(high_risk)}. "
    f"Risk-off signals: {len(risk_off)}. "
    f"Dominant action bias: {dominant_action}. "
    f"Trim concentration in {', '.join(x['ticker'] for x in top_risk[:3]) or '--'}, "
    f"and stagger adds toward {', '.join(x['ticker'] for x in top_benefit[:3]) or '--'} only after risk signal stabilizes."
  )
  return {
    "user_email": str(user_email or "").strip().lower(),
    "watchlists": watchlists,
    "positions": scored,
    "summary": {
      "count": len(scored),
      "average_macro_risk_score": avg,
      "top_risk_positions": top_risk,
      "top_benefit_positions": top_benefit,
      "high_risk_count": len(high_risk),
      "risk_off_count": len(risk_off),
      "dominant_action_bias": dominant_action,
      "advice": advice,
    },
  }


def report_portfolio_impact(report_date, user_email=""):
  summary = build_portfolio_risk_summary(user_email)
  top_risk = summary["summary"]["top_risk_positions"]
  top_benefit = summary["summary"]["top_benefit_positions"]
  return {
    "report_date": str(report_date or "").strip(),
    "user_email": str(user_email or "").strip().lower(),
    "average_macro_risk_score": summary["summary"]["average_macro_risk_score"],
    "top_risk_positions": top_risk,
    "top_benefit_positions": top_benefit,
    "summary_text": (
      f"Average macro risk score {summary['summary']['average_macro_risk_score']}. "
      f"Top risk: {', '.join(x['ticker'] for x in top_risk[:3]) or '--'}. "
      f"Potential beneficiaries: {', '.join(x['ticker'] for x in top_benefit[:3]) or '--'}."
    ),
  }
