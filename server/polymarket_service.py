#!/usr/bin/env python3
import json
import math
import os
import random
from datetime import datetime, timedelta, timezone

try:
  from .db import get_conn, now_iso
  from .polymarket_client import PolymarketLiveClient
except ImportError:
  from db import get_conn, now_iso
  from polymarket_client import PolymarketLiveClient

DEFAULT_RISK_TEMPLATE = {
  "max_daily_notional": 500.0,
  "max_order_notional": 50.0,
  "max_market_exposure": 120.0,
  "max_slippage_bps": 20.0,
  "max_open_orders": 3,
  "auto_trading": 0,
  "min_expected_edge_bps": 40.0,
  "min_model_confidence": 0.65,
  "min_orderbook_depth": 300.0,
  "order_cooldown_sec": 30,
  "max_daily_realized_loss": 80.0,
  "max_consecutive_failed_orders": 5,
  "max_reject_ratio_pct_10m": 40.0,
  "halt_on_api_degraded": 1,
  "default_order_type": "GTC",
  "post_only": 1,
  "allow_taker": 0,
  "cancel_stale_after_sec": 120,
  "paper_mode": 1,
}


def _utc_now():
  return datetime.now(timezone.utc)


def _fmt(dt: datetime):
  return dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _clamp(v, lo, hi):
  return max(lo, min(hi, v))


def _best_price(levels, side="ask"):
  if not levels:
    return None
  vals = []
  for lv in levels:
    try:
      vals.append(float(lv.get("price")))
    except Exception:
      continue
  if not vals:
    return None
  return min(vals) if side == "ask" else max(vals)


def _vwap(levels, qty):
  remain = float(qty or 0)
  if remain <= 0:
    return None
  total_notional = 0.0
  total_qty = 0.0
  for lv in levels or []:
    p = float(lv.get("price") or 0)
    s = float(lv.get("size") or 0)
    if p <= 0 or s <= 0:
      continue
    take = min(remain, s)
    total_notional += take * p
    total_qty += take
    remain -= take
    if remain <= 1e-12:
      break
  if total_qty <= 0:
    return None
  return total_notional / total_qty


def _calc_depth(levels, max_levels=5):
  depth = 0.0
  for lv in (levels or [])[:max_levels]:
    try:
      depth += float(lv.get("size") or 0)
    except Exception:
      pass
  return round(depth, 6)


def _fetchone(conn, sql, params=()):
  cur = conn.execute(sql, params)
  row = cur.fetchone()
  return dict(row) if row else None


def _fetchall(conn, sql, params=()):
  cur = conn.execute(sql, params)
  return [dict(r) for r in cur.fetchall()]


def _num(v, default=0.0):
  try:
    return float(v)
  except Exception:
    return float(default)


def _int(v, default=0):
  try:
    return int(float(v))
  except Exception:
    return int(default)


def _bool_int(v, default=0):
  if isinstance(v, bool):
    return 1 if v else 0
  s = str("" if v is None else v).strip().lower()
  if s in {"1", "true", "yes", "y", "on"}:
    return 1
  if s in {"0", "false", "no", "n", "off"}:
    return 0
  return 1 if int(default or 0) else 0


def _sanitize_risk_payload(payload):
  src = payload or {}
  out = dict(DEFAULT_RISK_TEMPLATE)
  out["max_daily_notional"] = max(0.0, _num(src.get("max_daily_notional", out["max_daily_notional"])))
  out["max_order_notional"] = max(0.0, _num(src.get("max_order_notional", out["max_order_notional"])))
  out["max_market_exposure"] = max(0.0, _num(src.get("max_market_exposure", out["max_market_exposure"])))
  out["max_slippage_bps"] = max(0.0, _num(src.get("max_slippage_bps", out["max_slippage_bps"])))
  out["max_open_orders"] = max(0, _int(src.get("max_open_orders", out["max_open_orders"])))
  out["auto_trading"] = _bool_int(src.get("auto_trading", out["auto_trading"]), out["auto_trading"])
  out["min_expected_edge_bps"] = max(0.0, _num(src.get("min_expected_edge_bps", out["min_expected_edge_bps"])))
  out["min_model_confidence"] = max(0.0, min(1.0, _num(src.get("min_model_confidence", out["min_model_confidence"]))))
  out["min_orderbook_depth"] = max(0.0, _num(src.get("min_orderbook_depth", out["min_orderbook_depth"])))
  out["order_cooldown_sec"] = max(0, _int(src.get("order_cooldown_sec", out["order_cooldown_sec"])))
  out["max_daily_realized_loss"] = max(0.0, _num(src.get("max_daily_realized_loss", out["max_daily_realized_loss"])))
  out["max_consecutive_failed_orders"] = max(0, _int(src.get("max_consecutive_failed_orders", out["max_consecutive_failed_orders"])))
  out["max_reject_ratio_pct_10m"] = max(0.0, min(100.0, _num(src.get("max_reject_ratio_pct_10m", out["max_reject_ratio_pct_10m"]))))
  out["halt_on_api_degraded"] = _bool_int(src.get("halt_on_api_degraded", out["halt_on_api_degraded"]), out["halt_on_api_degraded"])
  order_type = str(src.get("default_order_type", out["default_order_type"]) or "GTC").strip().upper()
  out["default_order_type"] = order_type if order_type in {"GTC", "IOC", "FOK"} else "GTC"
  out["post_only"] = _bool_int(src.get("post_only", out["post_only"]), out["post_only"])
  out["allow_taker"] = _bool_int(src.get("allow_taker", out["allow_taker"]), out["allow_taker"])
  out["cancel_stale_after_sec"] = max(0, _int(src.get("cancel_stale_after_sec", out["cancel_stale_after_sec"])))
  out["paper_mode"] = _bool_int(src.get("paper_mode", out["paper_mode"]), out["paper_mode"])
  return out


def _write_audit(conn, actor, object_type, object_id, action, result, metadata):
  conn.execute(
    """
    INSERT INTO audit_logs(actor, object_type, object_id, action, result, metadata_json, created_at)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    """,
    (
      str(actor or "system"),
      str(object_type or ""),
      str(object_id or ""),
      str(action or ""),
      str(result or ""),
      json.dumps(metadata or {}, ensure_ascii=False),
      now_iso(),
    ),
  )


def ensure_demo_data():
  conn = get_conn()
  try:
    cnt = _fetchone(conn, "SELECT COUNT(1) AS c FROM markets")
    if int((cnt or {}).get("c") or 0) > 0:
      return {"ok": True, "seeded": False}

    ts = now_iso()
    markets = [
      {
        "id": "mkt-fed-cut-2026-q3",
        "event_id": "evt-fed-2026",
        "title": "Will Fed cut rates by Sep 2026?",
        "category": "macro",
        "resolution_source": "Polymarket",
        "end_time": "2026-09-30T23:59:59Z",
        "enable_order_book": 1,
        "status": "active",
      },
      {
        "id": "mkt-us-recession-2026",
        "event_id": "evt-us-gdp-2026",
        "title": "US recession by end of 2026?",
        "category": "macro",
        "resolution_source": "Polymarket",
        "end_time": "2026-12-31T23:59:59Z",
        "enable_order_book": 1,
        "status": "active",
      },
    ]
    for m in markets:
      conn.execute(
        """
        INSERT OR REPLACE INTO markets(
          id, event_id, title, category, resolution_source, end_time, enable_order_book, status, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
          m["id"], m["event_id"], m["title"], m["category"], m["resolution_source"],
          m["end_time"], m["enable_order_book"], m["status"], ts, ts,
        ),
      )
      for side in ("YES", "NO"):
        token_id = f'{m["id"]}:{side}'
        conn.execute(
          """
          INSERT OR REPLACE INTO outcomes(id, market_id, label, token_id, side, settlement_rule, created_at, updated_at)
          VALUES (?, ?, ?, ?, ?, ?, ?, ?)
          """,
          (token_id, m["id"], side, token_id, side, "binary", ts, ts),
        )

    # Seed orderbooks with slight inefficiency so scanner can find opportunity.
    for m in markets:
      yes = f'{m["id"]}:YES'
      no = f'{m["id"]}:NO'
      y_asks = [{"price": 0.47, "size": 1200}, {"price": 0.49, "size": 1800}]
      y_bids = [{"price": 0.45, "size": 900}, {"price": 0.44, "size": 1600}]
      n_asks = [{"price": 0.50, "size": 1000}, {"price": 0.52, "size": 1700}]
      n_bids = [{"price": 0.48, "size": 800}, {"price": 0.47, "size": 1500}]
      for asset, bids, asks in ((yes, y_bids, y_asks), (no, n_bids, n_asks)):
        conn.execute(
          """
          INSERT INTO orderbook_snapshots(
            ts, asset_id, best_bid, best_ask, bid_levels_json, ask_levels_json, depth_1, depth_5
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
          """,
          (
            now_iso(),
            asset,
            _best_price(bids, "bid"),
            _best_price(asks, "ask"),
            json.dumps(bids, ensure_ascii=False),
            json.dumps(asks, ensure_ascii=False),
            _calc_depth(bids, max_levels=1) + _calc_depth(asks, max_levels=1),
            _calc_depth(bids, max_levels=5) + _calc_depth(asks, max_levels=5),
          ),
        )
    conn.commit()
    return {"ok": True, "seeded": True, "markets": len(markets)}
  finally:
    conn.close()


def account_connect(user_id, account_type, signer_address, funder_address, signature_type):
  conn = get_conn()
  try:
    account_id = f"acct-{str(user_id or 'user')}-{abs(hash(str(signer_address or 'na'))) % 1000000}"
    ts = now_iso()
    conn.execute(
      """
      INSERT OR REPLACE INTO trading_accounts(
        id, user_id, account_type, signer_address, funder_address, signature_type, status, created_at, updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, 'connected', ?, ?)
      """,
      (account_id, str(user_id or "default"), account_type, signer_address, funder_address, signature_type, ts, ts),
    )
    tpl = dict(DEFAULT_RISK_TEMPLATE)
    conn.execute(
      """
      INSERT INTO risk_limits(
        account_id, max_daily_notional, max_order_notional, max_market_exposure, max_slippage_bps, max_open_orders, auto_trading,
        min_expected_edge_bps, min_model_confidence, min_orderbook_depth, order_cooldown_sec,
        max_daily_realized_loss, max_consecutive_failed_orders, max_reject_ratio_pct_10m, halt_on_api_degraded,
        default_order_type, post_only, allow_taker, cancel_stale_after_sec, paper_mode,
        created_at, updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(account_id) DO NOTHING
      """,
      (
        account_id,
        tpl["max_daily_notional"],
        tpl["max_order_notional"],
        tpl["max_market_exposure"],
        tpl["max_slippage_bps"],
        tpl["max_open_orders"],
        tpl["auto_trading"],
        tpl["min_expected_edge_bps"],
        tpl["min_model_confidence"],
        tpl["min_orderbook_depth"],
        tpl["order_cooldown_sec"],
        tpl["max_daily_realized_loss"],
        tpl["max_consecutive_failed_orders"],
        tpl["max_reject_ratio_pct_10m"],
        tpl["halt_on_api_degraded"],
        tpl["default_order_type"],
        tpl["post_only"],
        tpl["allow_taker"],
        tpl["cancel_stale_after_sec"],
        tpl["paper_mode"],
        ts,
        ts,
      ),
    )
    conn.execute(
      """
      INSERT INTO account_balances(account_id, asset_symbol, available_balance, locked_balance, snapshot_at, created_at, updated_at)
      VALUES (?, 'USDC', 5000, 0, ?, ?, ?)
      """,
      (account_id, ts, ts, ts),
    )
    conn.execute(
      """
      INSERT INTO authorization_logs(account_id, action_type, result, metadata_json, created_at)
      VALUES (?, 'connect', 'ok', ?, ?)
      """,
      (account_id, json.dumps({"account_type": account_type}, ensure_ascii=False), ts),
    )
    _write_audit(conn, user_id, "account", account_id, "connect", "ok", {"account_type": account_type})
    conn.commit()
    return {"ok": True, "account_id": account_id, "status": "connected"}
  finally:
    conn.close()


def derive_credentials(account_id):
  conn = get_conn()
  try:
    acc = _fetchone(conn, "SELECT id, signer_address, funder_address, signature_type FROM trading_accounts WHERE id=?", (account_id,))
    if not acc:
      return {"ok": False, "error": "account_not_found"}
    ts = now_iso()
    key_id = f"pmk-{abs(hash(account_id + ts)) % 100000000}"
    live_client = PolymarketLiveClient()
    private_key = str(os.environ.get("POLYMARKET_PRIVATE_KEY", "")).strip()
    live = live_client.derive_api_credentials(
      signer_private_key=private_key,
      funder_address=str(acc.get("funder_address") or ""),
      signature_type=str(acc.get("signature_type") or ""),
    )
    use_live = bool(live.get("ok"))
    api_key_enc = f"enc:{live.get('api_key')}" if use_live else f"enc:{key_id}"
    secret_enc = f"enc:{live.get('api_secret')}" if use_live else f"enc:s-{key_id}"
    passphrase_enc = f"enc:{live.get('api_passphrase')}" if use_live else f"enc:p-{key_id}"
    key_version = "live-v1" if use_live else "v1"
    conn.execute(
      """
      INSERT INTO polymarket_api_credentials(
        account_id, api_key_enc, secret_enc, passphrase_enc, key_version, status, created_at, updated_at, last_verified_at
      ) VALUES (?, ?, ?, ?, 'v1', 'active', ?, ?, ?)
      ON CONFLICT(account_id) DO UPDATE SET
        api_key_enc=excluded.api_key_enc,
        secret_enc=excluded.secret_enc,
        passphrase_enc=excluded.passphrase_enc,
        key_version=excluded.key_version,
        status='active',
        updated_at=excluded.updated_at,
        last_verified_at=excluded.last_verified_at
      """,
      (account_id, api_key_enc, secret_enc, passphrase_enc, ts, ts, ts),
    )
    conn.execute("UPDATE polymarket_api_credentials SET key_version=? WHERE account_id=?", (key_version, account_id))
    conn.execute(
      """
      INSERT INTO authorization_logs(account_id, action_type, result, metadata_json, created_at)
      VALUES (?, 'derive_credentials', 'ok', ?, ?)
      """,
      (
        account_id,
        json.dumps(
          {
            "key_version": key_version,
            "live_enabled": bool(live_client.enabled),
            "live_used": bool(use_live),
            "live_error": str(live.get("error") or ""),
          },
          ensure_ascii=False,
        ),
        ts,
      ),
    )
    _write_audit(conn, "system", "credentials", account_id, "derive", "ok", {"key_version": key_version, "live_used": bool(use_live)})
    conn.commit()
    return {
      "ok": True,
      "credential_status": "active",
      "last_verified_at": ts,
      "key_version": key_version,
      "live_used": bool(use_live),
      "live_error": str(live.get("error") or ""),
    }
  finally:
    conn.close()


def get_account_status(account_id):
  conn = get_conn()
  try:
    acc = _fetchone(conn, "SELECT * FROM trading_accounts WHERE id=?", (account_id,))
    if not acc:
      return {"ok": False, "error": "account_not_found"}
    balances = _fetchall(
      conn,
      """
      SELECT asset_symbol, available_balance, locked_balance, snapshot_at
      FROM account_balances WHERE account_id=?
      ORDER BY id DESC LIMIT 10
      """,
      (account_id,),
    )
    positions = _fetchall(
      conn,
      """
      SELECT market_id, token_id, qty, avg_cost, mark_price, unrealized_pnl, updated_at
      FROM account_positions WHERE account_id=?
      ORDER BY updated_at DESC LIMIT 30
      """,
      (account_id,),
    )
    cred = _fetchone(
      conn,
      "SELECT status, key_version, last_verified_at FROM polymarket_api_credentials WHERE account_id=?",
      (account_id,),
    ) or {}
    risk = get_risk_limits(account_id)
    return {
      "ok": True,
      "account_id": account_id,
      "balances": balances,
      "positions": positions,
      "approvals": {
        "wallet_connected": acc.get("status") == "connected",
        "credentials_active": cred.get("status") == "active",
      },
      "health": {
        "sync_delay_sec": random.randint(0, 3),
        "account_status": acc.get("status"),
      },
      "limits": risk,
    }
  finally:
    conn.close()


def set_auto_trading(account_id, enabled: bool):
  conn = get_conn()
  try:
    ts = now_iso()
    conn.execute(
      """
      UPDATE risk_limits
      SET auto_trading=?, updated_at=?
      WHERE account_id=?
      """,
      (1 if enabled else 0, ts, account_id),
    )
    conn.execute(
      """
      INSERT INTO authorization_logs(account_id, action_type, result, metadata_json, created_at)
      VALUES (?, ?, 'ok', ?, ?)
      """,
      (
        account_id,
        "enable_auto_trading" if enabled else "disable_auto_trading",
        json.dumps({"enabled": bool(enabled)}, ensure_ascii=False),
        ts,
      ),
    )
    _write_audit(conn, "operator", "account", account_id, "set_auto_trading", "ok", {"enabled": bool(enabled)})
    conn.commit()
    limits = get_risk_limits(account_id)
    return {"ok": True, "current_limits": limits}
  finally:
    conn.close()


def list_trading_accounts(user_id=""):
  conn = get_conn()
  try:
    if user_id:
      rows = _fetchall(
        conn,
        """
        SELECT id, user_id, account_type, signer_address, funder_address, signature_type, status, created_at, updated_at
        FROM trading_accounts
        WHERE user_id=?
        ORDER BY updated_at DESC
        """,
        (user_id,),
      )
    else:
      rows = _fetchall(
        conn,
        """
        SELECT id, user_id, account_type, signer_address, funder_address, signature_type, status, created_at, updated_at
        FROM trading_accounts
        ORDER BY updated_at DESC
        """,
      )
    return rows
  finally:
    conn.close()


def get_risk_limits(account_id):
  conn = get_conn()
  try:
    row = _fetchone(conn, "SELECT * FROM risk_limits WHERE account_id=?", (account_id,))
    if not row:
      return dict(DEFAULT_RISK_TEMPLATE)
    out = dict(DEFAULT_RISK_TEMPLATE)
    out.update({k: row.get(k) for k in out.keys() if k in row})
    out["auto_trading"] = _bool_int(out.get("auto_trading"), 0)
    out["halt_on_api_degraded"] = _bool_int(out.get("halt_on_api_degraded"), 1)
    out["post_only"] = _bool_int(out.get("post_only"), 1)
    out["allow_taker"] = _bool_int(out.get("allow_taker"), 0)
    out["paper_mode"] = _bool_int(out.get("paper_mode"), 1)
    return out
  finally:
    conn.close()


def upsert_risk_limits(account_id, payload, actor="operator"):
  conn = get_conn()
  try:
    lim = _sanitize_risk_payload(payload or {})
    ts = now_iso()
    conn.execute(
      """
      INSERT INTO risk_limits(
        account_id, max_daily_notional, max_order_notional, max_market_exposure, max_slippage_bps, max_open_orders, auto_trading,
        min_expected_edge_bps, min_model_confidence, min_orderbook_depth, order_cooldown_sec,
        max_daily_realized_loss, max_consecutive_failed_orders, max_reject_ratio_pct_10m, halt_on_api_degraded,
        default_order_type, post_only, allow_taker, cancel_stale_after_sec, paper_mode,
        created_at, updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(account_id) DO UPDATE SET
        max_daily_notional=excluded.max_daily_notional,
        max_order_notional=excluded.max_order_notional,
        max_market_exposure=excluded.max_market_exposure,
        max_slippage_bps=excluded.max_slippage_bps,
        max_open_orders=excluded.max_open_orders,
        auto_trading=excluded.auto_trading,
        min_expected_edge_bps=excluded.min_expected_edge_bps,
        min_model_confidence=excluded.min_model_confidence,
        min_orderbook_depth=excluded.min_orderbook_depth,
        order_cooldown_sec=excluded.order_cooldown_sec,
        max_daily_realized_loss=excluded.max_daily_realized_loss,
        max_consecutive_failed_orders=excluded.max_consecutive_failed_orders,
        max_reject_ratio_pct_10m=excluded.max_reject_ratio_pct_10m,
        halt_on_api_degraded=excluded.halt_on_api_degraded,
        default_order_type=excluded.default_order_type,
        post_only=excluded.post_only,
        allow_taker=excluded.allow_taker,
        cancel_stale_after_sec=excluded.cancel_stale_after_sec,
        paper_mode=excluded.paper_mode,
        updated_at=excluded.updated_at
      """,
      (
        account_id,
        lim["max_daily_notional"],
        lim["max_order_notional"],
        lim["max_market_exposure"],
        lim["max_slippage_bps"],
        lim["max_open_orders"],
        lim["auto_trading"],
        lim["min_expected_edge_bps"],
        lim["min_model_confidence"],
        lim["min_orderbook_depth"],
        lim["order_cooldown_sec"],
        lim["max_daily_realized_loss"],
        lim["max_consecutive_failed_orders"],
        lim["max_reject_ratio_pct_10m"],
        lim["halt_on_api_degraded"],
        lim["default_order_type"],
        lim["post_only"],
        lim["allow_taker"],
        lim["cancel_stale_after_sec"],
        lim["paper_mode"],
        ts,
        ts,
      ),
    )
    _write_audit(conn, actor, "risk_limits", account_id, "upsert", "ok", lim)
    conn.commit()
    return {"ok": True, "account_id": account_id, "limits": lim}
  finally:
    conn.close()


def emergency_cancel_all(account_id):
  conn = get_conn()
  try:
    ts = now_iso()
    cur = conn.execute(
      """
      UPDATE executions
      SET status='cancelled', updated_at=?
      WHERE account_id=? AND status IN ('planned', 'submitted', 'partial')
      """,
      (ts, account_id),
    )
    cancelled = int(cur.rowcount or 0)
    conn.execute(
      """
      INSERT INTO authorization_logs(account_id, action_type, result, metadata_json, created_at)
      VALUES (?, 'emergency_cancel_all', 'ok', ?, ?)
      """,
      (account_id, json.dumps({"cancelled_count": cancelled}, ensure_ascii=False), ts),
    )
    _write_audit(conn, "operator", "execution", account_id, "emergency_cancel_all", "ok", {"cancelled_count": cancelled})
    conn.commit()
    return {"ok": True, "cancelled_count": cancelled}
  finally:
    conn.close()


def _latest_book(conn, asset_id):
  row = _fetchone(
    conn,
    """
    SELECT asset_id, best_bid, best_ask, bid_levels_json, ask_levels_json, ts
    FROM orderbook_snapshots
    WHERE asset_id=?
    ORDER BY id DESC
    LIMIT 1
    """,
    (asset_id,),
  )
  if not row:
    return None
  try:
    row["bid_levels"] = json.loads(row.get("bid_levels_json") or "[]")
  except Exception:
    row["bid_levels"] = []
  try:
    row["ask_levels"] = json.loads(row.get("ask_levels_json") or "[]")
  except Exception:
    row["ask_levels"] = []
  return row


def scan_opportunities(limit=50):
  conn = get_conn()
  try:
    ensure_demo_data()
    markets = _fetchall(conn, "SELECT * FROM markets WHERE status='active' ORDER BY updated_at DESC LIMIT 200")
    results = []
    ts = now_iso()
    for m in markets:
      yes = _latest_book(conn, f"{m['id']}:YES")
      no = _latest_book(conn, f"{m['id']}:NO")
      if not yes or not no:
        continue
      ask_sum = (float(yes.get("best_ask") or 0) + float(no.get("best_ask") or 0))
      bid_sum = (float(yes.get("best_bid") or 0) + float(no.get("best_bid") or 0))
      strategy_type = None
      theory_profit = 0.0
      legs = []
      if ask_sum > 0 and ask_sum < 0.995:
        strategy_type = "single_market_buy_both"
        theory_profit = 1.0 - ask_sum
        legs = [
          {"asset_id": yes["asset_id"], "side": "BUY", "qty": 100, "limit_price": yes.get("best_ask")},
          {"asset_id": no["asset_id"], "side": "BUY", "qty": 100, "limit_price": no.get("best_ask")},
        ]
      elif bid_sum > 1.005:
        strategy_type = "single_market_sell_both"
        theory_profit = bid_sum - 1.0
        legs = [
          {"asset_id": yes["asset_id"], "side": "SELL", "qty": 100, "limit_price": yes.get("best_bid")},
          {"asset_id": no["asset_id"], "side": "SELL", "qty": 100, "limit_price": no.get("best_bid")},
        ]
      if not strategy_type:
        continue
      confidence = _clamp(0.5 + theory_profit * 4, 0.5, 0.99)
      sim = estimate_simulation(legs)
      vwap_profit = round(max(-1, theory_profit - (sim.get("slippage_bps") or 0) / 10000), 6)
      opp_id = f"opp-{m['id']}-{int(datetime.now().timestamp())}"
      conn.execute(
        """
        INSERT INTO arbitrage_opportunities(
          id, strategy_type, discovered_at, theory_profit, vwap_profit, confidence, legs_json, status, market_id, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, 'open', ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
          theory_profit=excluded.theory_profit,
          vwap_profit=excluded.vwap_profit,
          confidence=excluded.confidence,
          legs_json=excluded.legs_json,
          status='open',
          updated_at=excluded.updated_at
        """,
        (
          opp_id, strategy_type, ts, round(theory_profit, 6), vwap_profit, round(confidence, 4),
          json.dumps(legs, ensure_ascii=False), m["id"], ts, ts,
        ),
      )
      results.append({
        "id": opp_id,
        "market_id": m["id"],
        "market_title": m["title"],
        "strategy_type": strategy_type,
        "theory_profit": round(theory_profit, 6),
        "vwap_profit": vwap_profit,
        "confidence": round(confidence, 4),
        "simulation": sim,
        "legs": legs,
      })
    conn.commit()
    results.sort(key=lambda x: x.get("vwap_profit", 0), reverse=True)
    return results[: max(1, min(int(limit or 50), 300))]
  finally:
    conn.close()


def estimate_simulation(legs):
  conn = get_conn()
  try:
    worst_slippage_bps = 0.0
    leg_results = []
    for lg in legs or []:
      asset_id = str(lg.get("asset_id") or "")
      side = str(lg.get("side") or "BUY").upper()
      qty = float(lg.get("qty") or 0)
      book = _latest_book(conn, asset_id)
      if not book:
        continue
      levels = book.get("ask_levels") if side == "BUY" else book.get("bid_levels")
      best = float(book.get("best_ask") if side == "BUY" else book.get("best_bid") or 0)
      vwap = _vwap(levels, qty) or best
      if best > 0:
        slippage_bps = abs(vwap - best) / best * 10000
      else:
        slippage_bps = 0.0
      worst_slippage_bps = max(worst_slippage_bps, slippage_bps)
      leg_results.append({
        "asset_id": asset_id,
        "side": side,
        "qty": qty,
        "best_price": round(best, 6),
        "vwap_price": round(vwap, 6),
        "slippage_bps": round(slippage_bps, 2),
      })
    return {
      "vwap_profit_adj": round(-worst_slippage_bps / 10000, 6),
      "slippage_bps": round(worst_slippage_bps, 2),
      "fill_risk": _clamp(round(worst_slippage_bps / 120, 4), 0.0, 0.99),
      "legs": leg_results,
    }
  finally:
    conn.close()


def get_opportunities(limit=50, status="open"):
  conn = get_conn()
  try:
    rows = _fetchall(
      conn,
      """
      SELECT id, strategy_type, discovered_at, theory_profit, vwap_profit, confidence, legs_json, status, market_id
      FROM arbitrage_opportunities
      WHERE status=?
      ORDER BY discovered_at DESC
      LIMIT ?
      """,
      (status, max(1, min(int(limit or 50), 300))),
    )
    out = []
    for r in rows:
      try:
        legs = json.loads(r.get("legs_json") or "[]")
      except Exception:
        legs = []
      m = _fetchone(conn, "SELECT id, title FROM markets WHERE id=?", (r.get("market_id"),)) or {}
      out.append({
        **r,
        "related_market": m,
        "legs": legs,
      })
    return out
  finally:
    conn.close()


def get_opportunity_detail(opportunity_id):
  conn = get_conn()
  try:
    r = _fetchone(
      conn,
      """
      SELECT id, strategy_type, discovered_at, theory_profit, vwap_profit, confidence, legs_json, status, market_id
      FROM arbitrage_opportunities WHERE id=?
      """,
      (opportunity_id,),
    )
    if not r:
      return None
    try:
      legs = json.loads(r.get("legs_json") or "[]")
    except Exception:
      legs = []
    sim = estimate_simulation(legs)
    m = _fetchone(conn, "SELECT id, event_id, title, category, end_time, status FROM markets WHERE id=?", (r.get("market_id"),))
    return {
      "opportunity": {**r, "legs": legs},
      "simulation": sim,
      "related_markets": [m] if m else [],
    }
  finally:
    conn.close()


def risk_overview(account_id=None):
  conn = get_conn()
  try:
    where = ""
    params = ()
    if account_id:
      where = "WHERE account_id=?"
      params = (account_id,)
    limits = _fetchall(
      conn,
      f"""
      SELECT account_id, max_daily_notional, max_market_exposure, max_slippage_bps, max_open_orders, auto_trading, updated_at
      FROM risk_limits
      {where}
      ORDER BY updated_at DESC
      """,
      params,
    )
    open_exec = _fetchall(
      conn,
      f"""
      SELECT account_id, COUNT(1) AS open_orders
      FROM executions
      WHERE status IN ('planned', 'submitted', 'partial')
      {"AND account_id=?" if account_id else ""}
      GROUP BY account_id
      """,
      ((account_id,) if account_id else ()),
    )
    open_map = {r["account_id"]: int(r.get("open_orders") or 0) for r in open_exec}
    alerts = []
    for l in limits:
      acct = l["account_id"]
      max_open = int(l.get("max_open_orders") or 0)
      curr_open = int(open_map.get(acct) or 0)
      if curr_open > max_open > 0:
        alerts.append({
          "account_id": acct,
          "type": "max_open_orders_breach",
          "severity": "high",
          "message": f"open_orders {curr_open} > max_open_orders {max_open}",
        })
      if int(l.get("auto_trading") or 0) == 0:
        alerts.append({
          "account_id": acct,
          "type": "auto_trading_disabled",
          "severity": "info",
          "message": "auto trading is disabled",
        })
    return {"limits": limits, "breaches": alerts, "alerts": alerts}
  finally:
    conn.close()


def evaluate_and_execute(account_id, opportunity_id, actor="system"):
  conn = get_conn()
  try:
    opp = _fetchone(conn, "SELECT * FROM arbitrage_opportunities WHERE id=?", (opportunity_id,))
    if not opp:
      return {"ok": False, "error": "opportunity_not_found"}
    acc = _fetchone(conn, "SELECT * FROM trading_accounts WHERE id=?", (account_id,))
    if not acc:
      return {"ok": False, "error": "account_not_found"}
    lim = _fetchone(conn, "SELECT * FROM risk_limits WHERE account_id=?", (account_id,))
    if not lim:
      return {"ok": False, "error": "risk_limit_not_found"}
    lim = {**dict(DEFAULT_RISK_TEMPLATE), **(lim or {})}
    if int(lim.get("auto_trading") or 0) != 1:
      return {"ok": False, "error": "auto_trading_disabled"}
    try:
      legs = json.loads(opp.get("legs_json") or "[]")
    except Exception:
      legs = []
    sim = estimate_simulation(legs)
    if float(sim.get("slippage_bps") or 0) > float(lim.get("max_slippage_bps") or 0):
      return {"ok": False, "error": "slippage_limit_exceeded", "simulation": sim}
    expected_profit = float(opp.get("vwap_profit") or 0)
    edge_bps = expected_profit * 10000
    if edge_bps < float(lim.get("min_expected_edge_bps") or 0):
      return {"ok": False, "error": "edge_below_threshold", "edge_bps": round(edge_bps, 3)}
    if float(opp.get("confidence") or 0) < float(lim.get("min_model_confidence") or 0):
      return {"ok": False, "error": "confidence_below_threshold", "confidence": float(opp.get("confidence") or 0)}
    if float(sim.get("fill_risk") or 0) > 0.95 and int(lim.get("halt_on_api_degraded") or 0) == 1:
      return {"ok": False, "error": "api_or_liquidity_degraded", "simulation": sim}

    ts = now_iso()
    exe_id = f"exe-{abs(hash(opportunity_id + account_id + ts)) % 1000000000}"
    realized_profit = round(expected_profit * random.uniform(0.85, 1.05), 6)
    latency = random.randint(120, 960)
    conn.execute(
      """
      INSERT INTO executions(
        id, arb_id, account_id, status, expected_profit, realized_profit, latency_ms, abort_reason, created_at, updated_at
      ) VALUES (?, ?, ?, 'filled', ?, ?, ?, '', ?, ?)
      """,
      (exe_id, opportunity_id, account_id, expected_profit, realized_profit, latency, ts, ts),
    )
    for idx, lg in enumerate(legs):
      conn.execute(
        """
        INSERT INTO execution_legs(
          execution_id, leg_index, market_id, token_id, side, qty, limit_price, fill_qty, avg_fill_price, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
          exe_id,
          idx,
          (lg.get("asset_id") or "").split(":")[0],
          lg.get("asset_id"),
          lg.get("side"),
          lg.get("qty"),
          lg.get("limit_price"),
          lg.get("qty"),
          lg.get("limit_price"),
          ts,
          ts,
        ),
      )
    conn.execute("UPDATE arbitrage_opportunities SET status='executed', updated_at=? WHERE id=?", (ts, opportunity_id))
    _write_audit(
      conn,
      actor,
      "execution",
      exe_id,
      "place_order",
      "ok",
      {"account_id": account_id, "opportunity_id": opportunity_id, "expected_profit": expected_profit},
    )
    conn.commit()
    return {
      "ok": True,
      "execution_id": exe_id,
      "status": "filled",
      "expected_profit": expected_profit,
      "realized_profit": realized_profit,
      "latency_ms": latency,
      "simulation": sim,
    }
  finally:
    conn.close()


def toggle_strategy(strategy_id, enabled=True):
  conn = get_conn()
  try:
    ts = now_iso()
    conn.execute(
      """
      INSERT INTO strategy_configs(strategy_id, enabled, params_json, updated_at, created_at)
      VALUES (?, ?, '{}', ?, ?)
      ON CONFLICT(strategy_id) DO UPDATE SET
        enabled=excluded.enabled,
        updated_at=excluded.updated_at
      """,
      (strategy_id, 1 if enabled else 0, ts, ts),
    )
    _write_audit(conn, "operator", "strategy", strategy_id, "toggle", "ok", {"enabled": bool(enabled)})
    conn.commit()
    return {"ok": True, "status": "enabled" if enabled else "disabled"}
  finally:
    conn.close()


def list_executions(account_id="", limit=80):
  conn = get_conn()
  try:
    if account_id:
      rows = _fetchall(
        conn,
        """
        SELECT id, arb_id, account_id, status, expected_profit, realized_profit, latency_ms, abort_reason, created_at
        FROM executions WHERE account_id=?
        ORDER BY created_at DESC LIMIT ?
        """,
        (account_id, max(1, min(int(limit or 80), 300))),
      )
    else:
      rows = _fetchall(
        conn,
        """
        SELECT id, arb_id, account_id, status, expected_profit, realized_profit, latency_ms, abort_reason, created_at
        FROM executions
        ORDER BY created_at DESC LIMIT ?
        """,
        (max(1, min(int(limit or 80), 300)),),
      )
    return rows
  finally:
    conn.close()


def replay_summary(days=7):
  conn = get_conn()
  try:
    since = _fmt(_utc_now() - timedelta(days=max(1, int(days or 7))))
    rows = _fetchall(
      conn,
      """
      SELECT status, COUNT(1) AS n, SUM(realized_profit) AS pnl
      FROM executions
      WHERE created_at>=?
      GROUP BY status
      """,
      (since,),
    )
    total = {
      "executions": int(sum(int(r.get("n") or 0) for r in rows)),
      "pnl": round(sum(float(r.get("pnl") or 0) for r in rows), 6),
    }
    return {"since": since, "groups": rows, "summary": total}
  finally:
    conn.close()
