#!/usr/bin/env python3
import json
import math
import os
import random
import hashlib
import time
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


def _filter_live_markets(raw_items):
  filtered = []
  for m in raw_items or []:
    if not isinstance(m, dict):
      continue
    if not bool(m.get("active")):
      continue
    if bool(m.get("closed")):
      continue
    if not bool(m.get("accepting_orders")):
      continue
    toks = m.get("tokens") or []
    if not isinstance(toks, list) or len(toks) < 2:
      continue
    filtered.append(m)
  return filtered


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


def _unwrap_enc(v):
  s = str(v or "")
  return s[4:] if s.startswith("enc:") else s


def _env_signer_address():
  pk = str(os.environ.get("POLYMARKET_PRIVATE_KEY", "")).strip()
  if not pk:
    return ""
  try:
    from eth_account import Account  # type: ignore
    return str(Account.from_key(pk).address or "").strip().lower()
  except Exception:
    return ""


def _get_live_credential_bundle(conn, account_id):
  acc = _fetchone(conn, "SELECT id, signer_address, funder_address, signature_type FROM trading_accounts WHERE id=?", (account_id,)) or {}
  cred = _fetchone(
    conn,
    "SELECT api_key_enc, secret_enc, passphrase_enc, status, key_version FROM polymarket_api_credentials WHERE account_id=?",
    (account_id,),
  ) or {}
  return {
    "account": acc,
    "credential": cred,
    "api_key": _unwrap_enc(cred.get("api_key_enc")),
    "api_secret": _unwrap_enc(cred.get("secret_enc")),
    "api_passphrase": _unwrap_enc(cred.get("passphrase_enc")),
  }


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
    live_client = PolymarketLiveClient()
    env_signer = _env_signer_address()
    in_signer = str(signer_address or "").strip().lower()
    # Live mode is bound to server signer key; reject mismatched signer to avoid "wrong wallet" confusion.
    if live_client.enabled and env_signer and in_signer and in_signer != env_signer:
      return {"ok": False, "error": "signer_mismatch_with_server_key", "expected_signer": env_signer}

    signer_norm = str(signer_address or "").strip().lower()
    stable = hashlib.sha1(signer_norm.encode("utf-8")).hexdigest()[:10]
    account_id = f"acct-{str(user_id or 'user')}-{stable}"
    ts = now_iso()
    existing = _fetchone(conn, "SELECT display_name FROM trading_accounts WHERE id=?", (account_id,)) or {}
    display_name = str(existing.get("display_name") or "").strip()
    if not display_name:
      n = int((_fetchone(conn, "SELECT COUNT(1) AS c FROM trading_accounts WHERE user_id=?", (str(user_id or "default"),)) or {}).get("c") or 0) + 1
      display_name = f"Account {n}"
    conn.execute(
      """
      INSERT OR REPLACE INTO trading_accounts(
        id, user_id, account_type, display_name, signer_address, funder_address, signature_type, status, created_at, updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, 'connected', ?, ?)
      """,
      (account_id, str(user_id or "default"), account_type, display_name, signer_address, funder_address, signature_type, ts, ts),
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
    env_signer = _env_signer_address()
    if live_client.enabled and env_signer and str(acc.get("signer_address") or "").strip().lower() != env_signer:
      return {"ok": False, "error": "signer_mismatch_with_server_key", "expected_signer": env_signer}

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
    out = {
      "ok": True,
      "account_id": account_id,
      "account": {
        "user_id": acc.get("user_id"),
        "signer_address": acc.get("signer_address"),
        "funder_address": acc.get("funder_address"),
        "signature_type": acc.get("signature_type"),
      },
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
    live_client = PolymarketLiveClient()
    if live_client.enabled:
      env_signer = _env_signer_address()
      out["health"]["env_signer"] = env_signer
      out["health"]["signer_match"] = bool(
        env_signer and str(acc.get("signer_address") or "").strip().lower() == env_signer
      )
      bundle = _get_live_credential_bundle(conn, account_id)
      if str((bundle.get("credential") or {}).get("status") or "") == "active":
        if env_signer and str((bundle.get("account") or {}).get("signer_address") or "").strip().lower() != env_signer:
          out["health"]["live_balance_ok"] = False
          out["health"]["live_balance_error"] = "signer_mismatch_with_server_key"
          return out
        private_key = str(os.environ.get("POLYMARKET_PRIVATE_KEY", "")).strip()
        bal = live_client.get_balance(
          signer_private_key=private_key,
          funder_address=str((bundle.get("account") or {}).get("funder_address") or ""),
          signature_type=str((bundle.get("account") or {}).get("signature_type") or "eoa"),
          api_key=str(bundle.get("api_key") or ""),
          api_secret=str(bundle.get("api_secret") or ""),
          api_passphrase=str(bundle.get("api_passphrase") or ""),
        )
        if bal.get("ok"):
          bd = bal.get("balance") or {}
          avail = float(bal.get("balance_num") or 0)
          if avail <= 0:
            try:
              avail = float(str((bd.get("balance") or "0")).strip() or 0)
            except Exception:
              avail = 0.0
          out["balances"] = [{
            "asset_symbol": "USDC",
            "available_balance": round(avail, 6),
            "locked_balance": 0,
            "snapshot_at": now_iso(),
            "source": "polymarket_live",
          }]
          out["health"]["live_balance_ok"] = True
        else:
          out["health"]["live_balance_ok"] = False
          out["health"]["live_balance_error"] = str(bal.get("error") or "")
    return out
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


def disconnect_account(account_id, actor="operator"):
  conn = get_conn()
  try:
    ts = now_iso()
    row = _fetchone(conn, "SELECT id, status FROM trading_accounts WHERE id=?", (account_id,))
    if not row:
      return {"ok": False, "error": "account_not_found"}
    conn.execute(
      "UPDATE trading_accounts SET status='disconnected', updated_at=? WHERE id=?",
      (ts, account_id),
    )
    conn.execute(
      "UPDATE risk_limits SET auto_trading=0, updated_at=? WHERE account_id=?",
      (ts, account_id),
    )
    conn.execute(
      """
      INSERT INTO authorization_logs(account_id, action_type, result, metadata_json, created_at)
      VALUES (?, 'disconnect_account', 'ok', ?, ?)
      """,
      (account_id, json.dumps({"previous_status": row.get("status")}, ensure_ascii=False), ts),
    )
    _write_audit(conn, actor, "account", account_id, "disconnect", "ok", {"previous_status": row.get("status")})
    conn.commit()
    return {"ok": True, "account_id": account_id, "status": "disconnected"}
  finally:
    conn.close()


def rename_account(account_id, display_name, actor="operator"):
  conn = get_conn()
  try:
    row = _fetchone(conn, "SELECT id FROM trading_accounts WHERE id=?", (account_id,))
    if not row:
      return {"ok": False, "error": "account_not_found"}
    name = str(display_name or "").strip()
    if not name:
      return {"ok": False, "error": "display_name_required"}
    if len(name) > 64:
      return {"ok": False, "error": "display_name_too_long"}
    ts = now_iso()
    conn.execute("UPDATE trading_accounts SET display_name=?, updated_at=? WHERE id=?", (name, ts, account_id))
    _write_audit(conn, actor, "account", account_id, "rename", "ok", {"display_name": name})
    conn.commit()
    return {"ok": True, "account_id": account_id, "display_name": name}
  finally:
    conn.close()


def list_trading_accounts(user_id=""):
  conn = get_conn()
  try:
    if user_id:
      rows = _fetchall(
        conn,
        """
        SELECT id, user_id, account_type, display_name, signer_address, funder_address, signature_type, status, created_at, updated_at
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
        SELECT id, user_id, account_type, display_name, signer_address, funder_address, signature_type, status, created_at, updated_at
        FROM trading_accounts
        ORDER BY updated_at DESC
        """,
      )
    # Deduplicate same signer accounts (legacy random-id bug) and prefer real proxy/api-wallet config.
    def _rank(r):
      sig = str(r.get("signature_type") or "").strip().lower()
      signer = str(r.get("signer_address") or "").strip().lower()
      funder = str(r.get("funder_address") or "").strip().lower()
      cred = _fetchone(
        conn,
        "SELECT status, key_version, last_verified_at FROM polymarket_api_credentials WHERE account_id=?",
        (str(r.get("id") or ""),),
      ) or {}
      connected = 1 if str(r.get("status") or "") == "connected" else 0
      is_proxy = 1 if sig in {"proxy", "poly_proxy", "1"} else 0
      has_api_wallet = 1 if funder and signer and funder != signer else 0
      live_cred = 1 if str(cred.get("key_version") or "").startswith("live-") else 0
      active_cred = 1 if str(cred.get("status") or "") == "active" else 0
      updated = str(r.get("updated_at") or "")
      return (connected, is_proxy, has_api_wallet, live_cred, active_cred, updated)

    # Hide legacy duplicates: same signer keeps only the best configured record.
    grouped = {}
    for r in rows:
      signer = str(r.get("signer_address") or "").strip().lower()
      key = (str(r.get("user_id") or "").strip().lower(), signer)
      old = grouped.get(key)
      if not old or _rank(r) > _rank(old):
        grouped[key] = r
    out = sorted(grouped.values(), key=lambda x: _rank(x), reverse=True)
    return out
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


def scan_opportunities(limit=50, include_near_miss=False, near_limit=10):
  conn = get_conn()
  try:
    live_client = PolymarketLiveClient()
    markets = []
    live_mode = False
    start_ts = time.time()
    try:
      max_scan_sec = max(2.0, float(os.environ.get("POLYMARKET_SCAN_MAX_SEC", "8")))
    except Exception:
      max_scan_sec = 8.0
    try:
      scan_market_cap = max(6, min(int(os.environ.get("POLYMARKET_SCAN_MARKET_CAP", "20")), 80))
    except Exception:
      scan_market_cap = 20
    try:
      fee_bps_per_leg = max(0.0, float(os.environ.get("POLYMARKET_FEE_BPS_PER_LEG", "20")))
    except Exception:
      fee_bps_per_leg = 20.0
    try:
      fee_bps_maker = max(0.0, float(os.environ.get("POLYMARKET_FEE_BPS_MAKER", "0")))
    except Exception:
      fee_bps_maker = 0.0
    try:
      fee_bps_taker = max(0.0, float(os.environ.get("POLYMARKET_FEE_BPS_TAKER", "20")))
    except Exception:
      fee_bps_taker = 20.0
    exec_role = str(os.environ.get("POLYMARKET_EXEC_ROLE", "taker") or "taker").strip().lower()
    fee_bps_exec = fee_bps_maker if exec_role == "maker" else fee_bps_taker
    try:
      min_net_edge_bps = max(0.0, float(os.environ.get("POLYMARKET_MIN_NET_EDGE_BPS", "10")))
    except Exception:
      min_net_edge_bps = 10.0
    try:
      min_expected_net_bps = max(0.0, float(os.environ.get("POLYMARKET_MIN_EXPECTED_NET_BPS", "8")))
    except Exception:
      min_expected_net_bps = 8.0
    adaptive_enabled = str(os.environ.get("POLYMARKET_ADAPTIVE_THRESH_ENABLED", "1")).strip().lower() in {"1", "true", "yes", "on"}
    try:
      adaptive_floor_bps = max(0.0, float(os.environ.get("POLYMARKET_ADAPTIVE_FLOOR_BPS", "1.5")))
    except Exception:
      adaptive_floor_bps = 1.5
    try:
      adaptive_cap_bps = max(adaptive_floor_bps, float(os.environ.get("POLYMARKET_ADAPTIVE_CAP_BPS", "14")))
    except Exception:
      adaptive_cap_bps = 14.0
    try:
      min_pair_depth = max(0.0, float(os.environ.get("POLYMARKET_MIN_PAIR_DEPTH", "150")))
    except Exception:
      min_pair_depth = 150.0
    try:
      latency_ms = max(0.0, float(os.environ.get("POLYMARKET_EXEC_LATENCY_MS", "800")))
    except Exception:
      latency_ms = 800.0
    try:
      latency_bps_per_500ms = max(0.0, float(os.environ.get("POLYMARKET_LATENCY_BPS_PER_500MS", "1.5")))
    except Exception:
      latency_bps_per_500ms = 1.5
    try:
      base_fill_prob = _clamp(float(os.environ.get("POLYMARKET_BASE_FILL_PROB", "0.82")), 0.1, 0.99)
    except Exception:
      base_fill_prob = 0.82
    try:
      fill_depth_ref = max(1.0, float(os.environ.get("POLYMARKET_FILL_DEPTH_REF", "300")))
    except Exception:
      fill_depth_ref = 300.0
    try:
      fill_slippage_ref_bps = max(1.0, float(os.environ.get("POLYMARKET_FILL_SLIPPAGE_REF_BPS", "20")))
    except Exception:
      fill_slippage_ref_bps = 20.0
    cross_enabled = str(os.environ.get("POLYMARKET_CROSS_MARKET_ENABLED", "1")).strip().lower() in {"1", "true", "yes", "on"}
    try:
      cross_pair_cap = max(10, min(int(os.environ.get("POLYMARKET_CROSS_PAIR_CAP", "80")), 300))
    except Exception:
      cross_pair_cap = 80
    try:
      cross_min_spread_bps = max(5.0, float(os.environ.get("POLYMARKET_CROSS_MIN_SPREAD_BPS", "60")))
    except Exception:
      cross_min_spread_bps = 60.0
    try:
      cross_min_expected_bps = max(2.0, float(os.environ.get("POLYMARKET_CROSS_MIN_EXPECTED_BPS", "6")))
    except Exception:
      cross_min_expected_bps = 6.0
    try:
      cross_near_quota = max(0, int(os.environ.get("POLYMARKET_CROSS_NEAR_QUOTA", "4")))
    except Exception:
      cross_near_quota = 4

    def _kw(question):
      s = str(question or "").lower()
      for ch in ",.:;!?()[]{}\"'`/\\|-_":
        s = s.replace(ch, " ")
      stop = {"the", "will", "is", "are", "for", "and", "with", "this", "that", "from", "into", "after", "before", "over", "under", "to", "of", "in", "on", "a", "an"}
      out = []
      for w in s.split():
        if len(w) < 4 or w in stop:
          continue
        out.append(w)
      return set(out)

    def _hash_like_title(t):
      s = str(t or "").strip().lower()
      if not s:
        return True
      if s.startswith("0x") and len(s) >= 18:
        return True
      if " " not in s and len(s) >= 20:
        return True
      return False
    if live_client.enabled:
      lm = live_client.fetch_markets(max_markets=180, max_pages=3)
      if lm.get("ok"):
        # Keep scan responsive on public HTTP: bounded market slice + bounded runtime.
        markets = _filter_live_markets(lm.get("items") or [])[:scan_market_cap]
        live_mode = bool(markets)
    if not markets and not live_client.enabled:
      # fallback for development/demo only when live mode is disabled
      ensure_demo_data()
      markets = _fetchall(conn, "SELECT * FROM markets WHERE status='active' ORDER BY updated_at DESC LIMIT 200")
    results = []
    near_miss = []
    cross_pool = []
    ts = now_iso()
    max_results = max(1, min(int(limit or 50), 300))
    for m in markets:
      if (time.time() - start_ts) > max_scan_sec:
        break
      if live_mode:
        toks = m.get("tokens") or []
        if not isinstance(toks, list) or len(toks) < 2:
          continue
        t0 = str((toks[0] or {}).get("token_id") or "")
        t1 = str((toks[1] or {}).get("token_id") or "")
        b0 = live_client.fetch_order_book(t0)
        b1 = live_client.fetch_order_book(t1)
        if not b0.get("ok") or not b1.get("ok"):
          continue
        yes = {
          "asset_id": t0,
          "best_bid": b0.get("best_bid"),
          "best_ask": b0.get("best_ask"),
          "bid_levels": b0.get("bids") or [],
          "ask_levels": b0.get("asks") or [],
        }
        no = {
          "asset_id": t1,
          "best_bid": b1.get("best_bid"),
          "best_ask": b1.get("best_ask"),
          "bid_levels": b1.get("bids") or [],
          "ask_levels": b1.get("asks") or [],
        }
      else:
        yes = _latest_book(conn, f"{m['id']}:YES")
        no = _latest_book(conn, f"{m['id']}:NO")
      if not yes or not no:
        continue
      qtxt = str((m.get("question") if live_mode else m.get("title")) or "")
      y_bid = float(yes.get("best_bid") or 0.0)
      y_ask = float(yes.get("best_ask") or 0.0)
      if y_bid > 0 and y_ask > 0:
        cross_pool.append({
          "market_id": str((m.get("condition_id") if live_mode else m.get("id")) or ""),
          "market_title": qtxt,
          "category": str(m.get("category") or "unknown"),
          "yes_token": str(yes.get("asset_id") or ""),
          "yes_bid": y_bid,
          "yes_ask": y_ask,
          "yes_mid": (y_bid + y_ask) / 2.0,
          "yes_depth": _calc_depth(yes.get("bid_levels") or [], max_levels=5),
          "kw": _kw(qtxt),
          "source": "polymarket_live" if live_mode else "demo_seed",
        })
      ask_sum = (float(yes.get("best_ask") or 0) + float(no.get("best_ask") or 0))
      bid_sum = (float(yes.get("best_bid") or 0) + float(no.get("best_bid") or 0))
      strategy_type = None
      theory_profit = 0.0
      legs = []
      candidate_only = False
      arb_gap_bps = 0.0
      if ask_sum > 0 and ask_sum < 0.995:
        strategy_type = "single_market_buy_both"
        theory_profit = 1.0 - ask_sum
        legs = [
          {"asset_id": yes["asset_id"], "side": "BUY", "qty": 1, "limit_price": yes.get("best_ask")},
          {"asset_id": no["asset_id"], "side": "BUY", "qty": 1, "limit_price": no.get("best_ask")},
        ]
      elif bid_sum > 1.005:
        strategy_type = "single_market_sell_both"
        theory_profit = bid_sum - 1.0
        legs = [
          {"asset_id": yes["asset_id"], "side": "SELL", "qty": 1, "limit_price": yes.get("best_bid")},
          {"asset_id": no["asset_id"], "side": "SELL", "qty": 1, "limit_price": no.get("best_bid")},
        ]
      elif include_near_miss:
        # Extended candidate scan: emit nearest non-arb structures for diagnostics.
        buy_gap = abs(ask_sum - 0.995) if ask_sum > 0 else 999.0
        sell_gap = abs(1.005 - bid_sum) if bid_sum > 0 else 999.0
        if buy_gap <= sell_gap and ask_sum > 0:
          strategy_type = "single_market_buy_both_candidate"
          theory_profit = 1.0 - ask_sum
          arb_gap_bps = max(0.0, ask_sum - 0.995) * 10000.0
          legs = [
            {"asset_id": yes["asset_id"], "side": "BUY", "qty": 1, "limit_price": yes.get("best_ask")},
            {"asset_id": no["asset_id"], "side": "BUY", "qty": 1, "limit_price": no.get("best_ask")},
          ]
          candidate_only = True
        elif bid_sum > 0:
          strategy_type = "single_market_sell_both_candidate"
          theory_profit = bid_sum - 1.0
          arb_gap_bps = max(0.0, 1.005 - bid_sum) * 10000.0
          legs = [
            {"asset_id": yes["asset_id"], "side": "SELL", "qty": 1, "limit_price": yes.get("best_bid")},
            {"asset_id": no["asset_id"], "side": "SELL", "qty": 1, "limit_price": no.get("best_bid")},
          ]
          candidate_only = True
      if not strategy_type:
        continue
      # Require minimum usable depth on both legs.
      if strategy_type == "single_market_buy_both":
        d_yes = _calc_depth(yes.get("ask_levels") or [], max_levels=5)
        d_no = _calc_depth(no.get("ask_levels") or [], max_levels=5)
        notional_ref = max(0.0, ask_sum)
      else:
        d_yes = _calc_depth(yes.get("bid_levels") or [], max_levels=5)
        d_no = _calc_depth(no.get("bid_levels") or [], max_levels=5)
        notional_ref = max(0.0, bid_sum)
      pair_depth = min(d_yes, d_no)
      # Microstructure proxies for adaptive thresholding.
      y_bid = float(yes.get("best_bid") or 0.0)
      y_ask = float(yes.get("best_ask") or 0.0)
      n_bid = float(no.get("best_bid") or 0.0)
      n_ask = float(no.get("best_ask") or 0.0)
      y_mid = (y_bid + y_ask) / 2.0 if (y_bid > 0 and y_ask > 0) else 0.0
      n_mid = (n_bid + n_ask) / 2.0 if (n_bid > 0 and n_ask > 0) else 0.0
      y_spread_bps = ((y_ask - y_bid) / y_mid * 10000.0) if y_mid > 0 else 999.0
      n_spread_bps = ((n_ask - n_bid) / n_mid * 10000.0) if n_mid > 0 else 999.0
      pair_spread_bps = (max(0.0, y_spread_bps) + max(0.0, n_spread_bps)) / 2.0

      sim = estimate_simulation(legs)
      slippage_bps = float(sim.get("slippage_bps") or 0.0)
      effective_fee_bps = fee_bps_exec if fee_bps_exec > 0 else fee_bps_per_leg
      fee_cost = notional_ref * (effective_fee_bps / 10000.0) * 2.0
      net_profit = round(theory_profit - (slippage_bps / 10000.0) - fee_cost, 6)
      net_edge_bps = round(net_profit * 10000.0, 2)
      dyn_min_net_bps = min_net_edge_bps
      dyn_min_expected_bps = min_expected_net_bps
      if adaptive_enabled:
        spread_up = max(0.0, (pair_spread_bps - 35.0) / 25.0)  # wider spread => stricter
        depth_down = max(0.0, (pair_depth / max(min_pair_depth, 1.0)) - 1.0) * 0.8  # deeper book => looser
        dyn_min_net_bps = _clamp(min_net_edge_bps + spread_up - depth_down, max(1.0, adaptive_floor_bps), adaptive_cap_bps)
        dyn_min_expected_bps = _clamp(min_expected_net_bps + spread_up - depth_down, adaptive_floor_bps, adaptive_cap_bps)
      depth_factor = _clamp(pair_depth / fill_depth_ref, 0.35, 1.2)
      slip_factor = _clamp(1.0 - (slippage_bps / fill_slippage_ref_bps) * 0.5, 0.3, 1.0)
      fill_probability = _clamp(base_fill_prob * depth_factor * slip_factor, 0.1, 0.99)
      latency_penalty_bps = (latency_ms / 500.0) * latency_bps_per_500ms
      expected_net_bps = round(net_edge_bps * fill_probability - latency_penalty_bps, 2)
      market_id = str((m.get("condition_id") if live_mode else m.get("id")) or "")
      market_title = str((m.get("question") if live_mode else m.get("title")) or market_id)
      fail_reason = ""
      if candidate_only:
        fail_reason = "raw_structure_not_arb"
      elif pair_depth < min_pair_depth:
        fail_reason = "depth_too_low"
      elif net_edge_bps < dyn_min_net_bps:
        fail_reason = "net_edge_below_threshold"
      elif expected_net_bps < dyn_min_expected_bps:
        fail_reason = "expected_net_below_threshold"
      if fail_reason:
        if include_near_miss:
          near_miss.append({
            "market_id": market_id,
            "market_title": market_title,
            "strategy_type": strategy_type,
            "theory_profit": round(theory_profit, 6),
            "net_profit": net_profit,
            "net_edge_bps": net_edge_bps,
            "expected_net_bps": expected_net_bps,
            "fill_probability": round(fill_probability, 4),
            "pair_depth": round(pair_depth, 4),
            "reason": fail_reason,
            "gap_bps": round(
              arb_gap_bps if fail_reason == "raw_structure_not_arb"
              else (min_pair_depth - pair_depth) if fail_reason == "depth_too_low"
              else (dyn_min_net_bps - net_edge_bps) if fail_reason == "net_edge_below_threshold"
              else (dyn_min_expected_bps - expected_net_bps),
              2
            ),
            "source": "polymarket_live" if live_mode else "demo_seed",
          })
        continue
      expected_net_profit = round(expected_net_bps / 10000.0, 6)
      confidence = _clamp(0.45 + (max(0.0, expected_net_profit) * 12), 0.45, 0.99)
      opp_id = f"opp-{market_id}-{int(datetime.now().timestamp())}"
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
          opp_id, strategy_type, ts, round(theory_profit, 6), expected_net_profit, round(confidence, 4),
          json.dumps(legs, ensure_ascii=False), market_id, ts, ts,
        ),
      )
      results.append({
        "id": opp_id,
        "market_id": market_id,
        "market_title": market_title,
        "strategy_type": strategy_type,
        "theory_profit": round(theory_profit, 6),
        "vwap_profit": expected_net_profit,
        "net_profit": net_profit,
        "net_edge_bps": net_edge_bps,
        "expected_net_profit": expected_net_profit,
        "expected_net_bps": expected_net_bps,
        "fill_probability": round(fill_probability, 4),
        "cost_model": {
          "fee_bps_per_leg": effective_fee_bps,
          "slippage_bps": round(slippage_bps, 2),
          "latency_penalty_bps": round(latency_penalty_bps, 2),
          "pair_depth": round(pair_depth, 4),
          "min_pair_depth": min_pair_depth,
          "min_net_edge_bps": round(dyn_min_net_bps, 2),
          "min_expected_net_bps": round(dyn_min_expected_bps, 2),
          "pair_spread_bps": round(pair_spread_bps, 2),
          "adaptive_enabled": adaptive_enabled,
          "adaptive_floor_bps": adaptive_floor_bps,
          "adaptive_cap_bps": adaptive_cap_bps,
          "execution_role": exec_role,
          "base_fill_prob": round(base_fill_prob, 4),
        },
        "confidence": round(confidence, 4),
        "simulation": sim,
        "legs": legs,
        "source": "polymarket_live" if live_mode else "demo_seed",
      })
      if len(results) >= max_results:
        break
    if cross_enabled and len(results) < max_results and len(cross_pool) >= 2:
      cross_count = 0
      for i in range(len(cross_pool)):
        a = cross_pool[i]
        for j in range(i + 1, len(cross_pool)):
          if cross_count >= cross_pair_cap or len(results) >= max_results:
            break
          b = cross_pool[j]
          if a["market_id"] == b["market_id"]:
            continue
          inter = len((a.get("kw") or set()) & (b.get("kw") or set()))
          if not (_hash_like_title(a.get("market_title")) or _hash_like_title(b.get("market_title"))):
            if inter < 2:
              continue
          spread = abs(float(a.get("yes_mid") or 0.0) - float(b.get("yes_mid") or 0.0))
          spread_bps = spread * 10000.0
          if spread_bps < 1:
            continue
          high = a if float(a["yes_mid"]) >= float(b["yes_mid"]) else b
          low = b if high is a else a
          # candidate: buy lower-mid YES, sell higher-mid YES
          sim_slip = max(0.0, (100.0 / max(1.0, float(low.get("yes_depth") or 1.0))) * 8.0)
          fee_bps = (fee_bps_exec if fee_bps_exec > 0 else fee_bps_per_leg) * 2.0
          net_edge_bps = spread_bps - fee_bps - sim_slip
          fill_prob = _clamp(base_fill_prob * _clamp(min(float(low.get("yes_depth") or 0.0), float(high.get("yes_depth") or 0.0)) / fill_depth_ref, 0.35, 1.1), 0.1, 0.95)
          expected_bps = net_edge_bps * fill_prob - ((latency_ms / 500.0) * latency_bps_per_500ms)
          cross_id = f"opp-xm-{low['market_id'][:8]}-{high['market_id'][:8]}-{int(datetime.now().timestamp())}"
          legs = [
            {"asset_id": low["yes_token"], "side": "BUY", "qty": 1, "limit_price": low["yes_ask"]},
            {"asset_id": high["yes_token"], "side": "SELL", "qty": 1, "limit_price": high["yes_bid"]},
          ]
          title = f"{low['market_title']}  <->  {high['market_title']}"
          if spread_bps >= cross_min_spread_bps and expected_bps >= cross_min_expected_bps:
            expected_net_profit = round(expected_bps / 10000.0, 6)
            confidence = _clamp(0.42 + max(0.0, expected_net_profit) * 10, 0.42, 0.92)
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
                cross_id, "cross_market_yes_spread", ts, round(spread, 6), expected_net_profit, round(confidence, 4),
                json.dumps(legs, ensure_ascii=False), f"{low['market_id']}|{high['market_id']}", ts, ts,
              ),
            )
            results.append({
              "id": cross_id,
              "market_id": f"{low['market_id']}|{high['market_id']}",
              "market_title": title,
              "strategy_type": "cross_market_yes_spread",
              "theory_profit": round(spread, 6),
              "vwap_profit": round(expected_bps / 10000.0, 6),
              "net_profit": round(net_edge_bps / 10000.0, 6),
              "expected_net_profit": round(expected_bps / 10000.0, 6),
              "expected_net_bps": round(expected_bps, 2),
              "fill_probability": round(fill_prob, 4),
              "confidence": round(confidence, 4),
              "legs": legs,
              "source": low.get("source") or "polymarket_live",
            })
          elif include_near_miss:
            reason = "cross_spread_below_threshold" if spread_bps < cross_min_spread_bps else "cross_expected_below_threshold"
            gap = (cross_min_spread_bps - spread_bps) if reason == "cross_spread_below_threshold" else (cross_min_expected_bps - expected_bps)
            near_miss.append({
              "market_id": f"{low['market_id']}|{high['market_id']}",
              "market_title": title,
              "strategy_type": "cross_market_yes_spread_candidate",
              "theory_profit": round(spread, 6),
              "expected_net_bps": round(expected_bps, 2),
              "fill_probability": round(fill_prob, 4),
              "reason": reason,
              "gap_bps": round(max(0.0, gap), 2),
              "source": low.get("source") or "polymarket_live",
            })
          cross_count += 1
        if cross_count >= cross_pair_cap or len(results) >= max_results:
          break
    conn.commit()
    results.sort(key=lambda x: x.get("vwap_profit", 0), reverse=True)
    if include_near_miss:
      near_miss.sort(key=lambda x: float(x.get("gap_bps") or 0.0))
      near_limit_n = max(1, min(int(near_limit or 10), 50))
      cross_nm = [x for x in near_miss if str(x.get("strategy_type") or "").startswith("cross_market_")]
      non_cross_nm = [x for x in near_miss if not str(x.get("strategy_type") or "").startswith("cross_market_")]
      q = max(0, min(cross_near_quota, near_limit_n))
      out_nm = []
      out_nm.extend(cross_nm[:q])
      remain = near_limit_n - len(out_nm)
      out_nm.extend(non_cross_nm[:remain])
      if len(out_nm) < near_limit_n:
        used = {id(x) for x in out_nm}
        for x in cross_nm[q:]:
          if len(out_nm) >= near_limit_n:
            break
          if id(x) not in used:
            out_nm.append(x)
      return {
        "items": results[: max_results],
        "near_miss": out_nm,
        "meta": {
          "min_pair_depth": min_pair_depth,
          "min_net_edge_bps": min_net_edge_bps,
          "min_expected_net_bps": min_expected_net_bps,
          "adaptive_enabled": adaptive_enabled,
          "adaptive_floor_bps": adaptive_floor_bps,
          "adaptive_cap_bps": adaptive_cap_bps,
          "cross_near_quota": q,
        },
      }
    return results[: max_results]
  finally:
    conn.close()


def get_live_market_diagnostics():
  live_client = PolymarketLiveClient()
  if not live_client.enabled:
    return {
      "ok": True,
      "live_enabled": False,
      "raw_market_count": 0,
      "tradable_market_count": 0,
      "source": "disabled",
    }
  lm = live_client.fetch_markets(max_markets=500, max_pages=6)
  if not lm.get("ok"):
    return {
      "ok": False,
      "live_enabled": True,
      "error": str(lm.get("error") or "fetch_markets_failed"),
      "detail": str(lm.get("detail") or "")[:240],
      "raw_market_count": 0,
      "tradable_market_count": 0,
    }
  raw = lm.get("items") or []
  filtered = _filter_live_markets(raw)
  return {
    "ok": True,
    "live_enabled": True,
    "raw_market_count": len(raw),
    "tradable_market_count": len(filtered),
    "source": "polymarket_live",
  }


def estimate_simulation(legs):
  conn = get_conn()
  try:
    worst_slippage_bps = 0.0
    leg_results = []
    live_client = PolymarketLiveClient()
    for lg in legs or []:
      asset_id = str(lg.get("asset_id") or "")
      side = str(lg.get("side") or "BUY").upper()
      qty = float(lg.get("qty") or 0)
      book = _latest_book(conn, asset_id)
      if not book and live_client.enabled and asset_id:
        lb = live_client.fetch_order_book(asset_id)
        if lb.get("ok"):
          book = {
            "best_bid": lb.get("best_bid"),
            "best_ask": lb.get("best_ask"),
            "bid_levels": lb.get("bids") or [],
            "ask_levels": lb.get("asks") or [],
          }
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
    latency = random.randint(120, 960)
    is_paper = int(lim.get("paper_mode") or 1) == 1
    if is_paper:
      realized_profit = round(expected_profit * random.uniform(0.85, 1.05), 6)
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
        {"account_id": account_id, "opportunity_id": opportunity_id, "expected_profit": expected_profit, "mode": "paper"},
      )
      conn.commit()
      return {
        "ok": True,
        "execution_id": exe_id,
        "status": "filled",
        "mode": "paper",
        "expected_profit": expected_profit,
        "realized_profit": realized_profit,
        "latency_ms": latency,
        "simulation": sim,
      }

    live_client = PolymarketLiveClient()
    if not live_client.enabled:
      return {"ok": False, "error": "live_disabled"}
    if not live_client.allow_live_orders:
      return {"ok": False, "error": "live_orders_disabled"}
    env_signer = _env_signer_address()
    if env_signer and str(acc.get("signer_address") or "").strip().lower() != env_signer:
      return {"ok": False, "error": "signer_mismatch_with_server_key", "expected_signer": env_signer}

    bundle = _get_live_credential_bundle(conn, account_id)
    if str((bundle.get("credential") or {}).get("status") or "") != "active":
      return {"ok": False, "error": "credential_not_active"}
    private_key = str(os.environ.get("POLYMARKET_PRIVATE_KEY", "")).strip()
    if not private_key:
      return {"ok": False, "error": "missing_private_key"}

    live_orders = []
    nlegs = max(1, len(legs))
    max_order_notional = float(lim.get("max_order_notional") or 0)
    for idx, lg in enumerate(legs):
      px = float(lg.get("limit_price") or 0)
      side = str(lg.get("side") or "BUY").upper()
      token_id = str(lg.get("asset_id") or "")
      if px <= 0 or not token_id:
        return {"ok": False, "error": "invalid_leg_data", "leg_index": idx}
      qty_by_risk = max_order_notional / (max(px, 1e-9) * nlegs) if max_order_notional > 0 else float(lg.get("qty") or 0)
      qty = float(lg.get("qty") or 0)
      if qty_by_risk > 0:
        qty = min(qty, qty_by_risk) if qty > 0 else qty_by_risk
      qty = max(0.01, round(qty, 4))
      p = live_client.place_limit_order(
        signer_private_key=private_key,
        funder_address=str((bundle.get("account") or {}).get("funder_address") or ""),
        signature_type=str((bundle.get("account") or {}).get("signature_type") or "eoa"),
        api_key=str(bundle.get("api_key") or ""),
        api_secret=str(bundle.get("api_secret") or ""),
        api_passphrase=str(bundle.get("api_passphrase") or ""),
        token_id=token_id,
        side=side,
        price=px,
        size=qty,
        post_only=bool(int(lim.get("post_only") or 0) == 1),
        order_type=str(lim.get("default_order_type") or "GTC"),
      )
      if not p.get("ok"):
        return {"ok": False, "error": p.get("error") or "place_order_failed", "detail": p.get("detail"), "leg_index": idx}
      live_orders.append({
        "token_id": token_id,
        "side": side,
        "qty": qty,
        "limit_price": px,
        "order_id": p.get("order_id"),
      })

    conn.execute(
      """
      INSERT INTO executions(
        id, arb_id, account_id, status, expected_profit, realized_profit, latency_ms, abort_reason, created_at, updated_at
      ) VALUES (?, ?, ?, 'submitted', ?, 0, ?, '', ?, ?)
      """,
      (exe_id, opportunity_id, account_id, expected_profit, latency, ts, ts),
    )
    for idx, lg in enumerate(live_orders):
      conn.execute(
        """
        INSERT INTO execution_legs(
          execution_id, leg_index, market_id, token_id, side, qty, limit_price, fill_qty, avg_fill_price, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, 0, 0, ?, ?)
        """,
        (
          exe_id,
          idx,
          str(lg.get("token_id") or "").split(":")[0],
          lg.get("token_id"),
          lg.get("side"),
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
      {"account_id": account_id, "opportunity_id": opportunity_id, "expected_profit": expected_profit, "mode": "live", "orders": live_orders},
    )
    conn.commit()
    return {
      "ok": True,
      "execution_id": exe_id,
      "status": "submitted",
      "mode": "live",
      "expected_profit": expected_profit,
      "realized_profit": 0,
      "latency_ms": latency,
      "simulation": sim,
      "orders": live_orders,
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


def rebuild_pnl_ledger(account_id, actor="system", lookback_days=365):
  conn = get_conn()
  try:
    now = now_iso()
    since = _fmt(_utc_now() - timedelta(days=max(1, int(lookback_days or 365))))
    acc = _fetchone(conn, "SELECT id, user_id FROM trading_accounts WHERE id=?", (account_id,))
    if not acc:
      return {"ok": False, "error": "account_not_found"}

    # Rebuild derived trade ledger entries from execution_legs.
    conn.execute(
      "DELETE FROM ledger_entries WHERE account_id=? AND source_tag='execution_derived'",
      (account_id,),
    )
    legs = _fetchall(
      conn,
      """
      SELECT
        e.id AS execution_id,
        e.arb_id,
        e.created_at AS occurred_at,
        e.status AS execution_status,
        e.expected_profit,
        e.realized_profit,
        l.leg_index,
        l.market_id,
        l.token_id,
        l.side,
        l.qty,
        l.avg_fill_price,
        l.limit_price
      FROM executions e
      LEFT JOIN execution_legs l ON l.execution_id=e.id
      WHERE e.account_id=? AND e.created_at>=?
      ORDER BY e.created_at ASC, l.leg_index ASC
      """,
      (account_id, since),
    )
    inserted = 0
    for lg in legs:
      if lg.get("leg_index") is None:
        continue
      qty = float(lg.get("qty") or 0.0)
      px = float(lg.get("avg_fill_price") or lg.get("limit_price") or 0.0)
      side = str(lg.get("side") or "").upper()
      if qty <= 0 or px <= 0 or side not in {"BUY", "SELL"}:
        continue
      usdc_delta = -(qty * px) if side == "BUY" else (qty * px)
      token_delta = qty if side == "BUY" else -qty
      strategy_row = _fetchone(conn, "SELECT strategy_type FROM arbitrage_opportunities WHERE id=?", (str(lg.get("arb_id") or ""),))
      strategy_type = str((strategy_row or {}).get("strategy_type") or "")
      fill_id = f"{lg.get('execution_id')}:{lg.get('leg_index')}"
      activity_type = "TRADE_BUY" if side == "BUY" else "TRADE_SELL"
      conn.execute(
        """
        INSERT INTO ledger_entries(
          account_id, execution_id, execution_leg_index, activity_type, market_id, token_id, strategy_type,
          usdc_delta, token_delta, fee_delta, rebate_delta, reward_delta, tx_hash, fill_id, occurred_at, source_tag,
          metadata_json, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0, 0, '', ?, ?, 'execution_derived', ?, ?, ?)
        """,
        (
          account_id,
          str(lg.get("execution_id") or ""),
          int(lg.get("leg_index") or 0),
          activity_type,
          str(lg.get("market_id") or ""),
          str(lg.get("token_id") or ""),
          strategy_type,
          round(usdc_delta, 8),
          round(token_delta, 8),
          fill_id,
          str(lg.get("occurred_at") or now),
          json.dumps({
            "qty": qty,
            "price": px,
            "execution_status": str(lg.get("execution_status") or ""),
            "expected_profit": float(lg.get("expected_profit") or 0.0),
            "realized_profit": float(lg.get("realized_profit") or 0.0),
          }, ensure_ascii=False),
          now,
          now,
        ),
      )
      inserted += 1

    # Mark snapshot from current account_positions.
    conn.execute("DELETE FROM position_marks WHERE account_id=? AND ts>=?", (account_id, since))
    pos_rows = _fetchall(
      conn,
      """
      SELECT market_id, token_id, qty, mark_price, unrealized_pnl, updated_at
      FROM account_positions
      WHERE account_id=?
      """,
      (account_id,),
    )
    for p in pos_rows:
      conn.execute(
        """
        INSERT INTO position_marks(account_id, market_id, token_id, mark_price, mark_source, qty, unrealized_pnl, ts, created_at)
        VALUES (?, ?, ?, ?, 'account_positions', ?, ?, ?, ?)
        """,
        (
          account_id,
          str(p.get("market_id") or ""),
          str(p.get("token_id") or ""),
          float(p.get("mark_price") or 0.0),
          float(p.get("qty") or 0.0),
          float(p.get("unrealized_pnl") or 0.0),
          now,
          now,
        ),
      )

    # Snapshot totals.
    ex = _fetchone(
      conn,
      """
      SELECT
        COALESCE(SUM(CASE WHEN status='filled' THEN realized_profit ELSE 0 END),0) AS realized_exec,
        COUNT(1) AS n_exec
      FROM executions
      WHERE account_id=? AND created_at>=?
      """,
      (account_id, since),
    ) or {}
    led = _fetchone(
      conn,
      """
      SELECT
        COALESCE(SUM(fee_delta),0) AS fee_total,
        COALESCE(SUM(rebate_delta),0) AS rebate_total,
        COALESCE(SUM(reward_delta),0) AS reward_total
      FROM ledger_entries
      WHERE account_id=? AND occurred_at>=?
      """,
      (account_id, since),
    ) or {}
    pos = _fetchone(
      conn,
      "SELECT COALESCE(SUM(unrealized_pnl),0) AS unrealized_total FROM account_positions WHERE account_id=?",
      (account_id,),
    ) or {}
    realized = float(ex.get("realized_exec") or 0.0)
    fee_total = float(led.get("fee_total") or 0.0)
    rebate_total = float(led.get("rebate_total") or 0.0)
    reward_total = float(led.get("reward_total") or 0.0)
    unrealized = float(pos.get("unrealized_total") or 0.0)
    total = realized + unrealized + rebate_total + reward_total - abs(fee_total)
    conn.execute(
      """
      INSERT INTO pnl_snapshots(
        account_id, ts, realized_pnl, unrealized_pnl, fee_total, rebate_total, reward_total, total_pnl, calc_version, metadata_json, created_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'v1', ?, ?)
      """,
      (
        account_id,
        now,
        round(realized, 8),
        round(unrealized, 8),
        round(fee_total, 8),
        round(rebate_total, 8),
        round(reward_total, 8),
        round(total, 8),
        json.dumps({"source": "rebuild_pnl_ledger", "lookback_days": int(lookback_days)}, ensure_ascii=False),
        now,
      ),
    )

    # Reconciliation.
    positions_total = realized + unrealized
    diff_pos = total - positions_total
    status = "ok" if abs(diff_pos) <= 1.0 else ("warn" if abs(diff_pos) <= 5.0 else "critical")
    conn.execute(
      """
      INSERT INTO reconciliation_results(
        account_id, ts, internal_total_pnl, positions_total_pnl, leaderboard_total_pnl,
        diff_internal_vs_positions, diff_internal_vs_leaderboard, status, notes, metadata_json, created_at
      ) VALUES (?, ?, ?, ?, NULL, ?, NULL, ?, ?, ?, ?)
      """,
      (
        account_id,
        now,
        round(total, 8),
        round(positions_total, 8),
        round(diff_pos, 8),
        status,
        "leaderboard reference not integrated",
        json.dumps({"threshold_abs_diff": 1.0}, ensure_ascii=False),
        now,
      ),
    )

    # Strategy attribution from executions + opportunity strategy_type.
    conn.execute("DELETE FROM strategy_attribution WHERE account_id=?", (account_id,))
    strat_rows = _fetchall(
      conn,
      """
      SELECT
        COALESCE(o.strategy_type, 'unknown') AS strategy_type,
        COALESCE(SUM(ABS(e.expected_profit)),0) AS volume,
        COALESCE(SUM(CASE WHEN e.status='filled' THEN e.realized_profit ELSE 0 END),0) AS realized_pnl,
        COUNT(1) AS n_trades
      FROM executions e
      LEFT JOIN arbitrage_opportunities o ON o.id=e.arb_id
      WHERE e.account_id=? AND e.created_at>=?
      GROUP BY COALESCE(o.strategy_type, 'unknown')
      ORDER BY realized_pnl DESC
      """,
      (account_id, since),
    )
    for idx, r in enumerate(strat_rows, start=1):
      st = str(r.get("strategy_type") or "unknown")
      conn.execute(
        """
        INSERT INTO strategy_attribution(
          account_id, strategy_id, strategy_type, volume, realized_pnl, unrealized_pnl, n_trades, ts, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, 0, ?, ?, ?, ?)
        """,
        (
          account_id,
          f"attr-{account_id}-{idx}",
          st,
          float(r.get("volume") or 0.0),
          float(r.get("realized_pnl") or 0.0),
          int(r.get("n_trades") or 0),
          now,
          now,
          now,
        ),
      )

    _write_audit(conn, actor, "pnl", account_id, "rebuild", "ok", {"ledger_entries": inserted, "strategies": len(strat_rows)})
    conn.commit()
    return {
      "ok": True,
      "account_id": account_id,
      "inserted_ledger_entries": inserted,
      "strategies": len(strat_rows),
      "snapshot_ts": now,
    }
  finally:
    conn.close()


def get_pnl_overview(account_id, days=30):
  conn = get_conn()
  try:
    since = _fmt(_utc_now() - timedelta(days=max(1, int(days or 30))))
    latest = _fetchone(
      conn,
      """
      SELECT ts, realized_pnl, unrealized_pnl, fee_total, rebate_total, reward_total, total_pnl, calc_version
      FROM pnl_snapshots
      WHERE account_id=?
      ORDER BY ts DESC, id DESC
      LIMIT 1
      """,
      (account_id,),
    ) or {}
    if not latest:
      # soft fallback from executions/positions
      ex = _fetchone(conn, "SELECT COALESCE(SUM(CASE WHEN status='filled' THEN realized_profit ELSE 0 END),0) AS rp FROM executions WHERE account_id=?", (account_id,)) or {}
      pos = _fetchone(conn, "SELECT COALESCE(SUM(unrealized_pnl),0) AS up FROM account_positions WHERE account_id=?", (account_id,)) or {}
      realized = float(ex.get("rp") or 0.0)
      unrealized = float(pos.get("up") or 0.0)
      latest = {
        "ts": now_iso(),
        "realized_pnl": round(realized, 8),
        "unrealized_pnl": round(unrealized, 8),
        "fee_total": 0.0,
        "rebate_total": 0.0,
        "reward_total": 0.0,
        "total_pnl": round(realized + unrealized, 8),
        "calc_version": "fallback",
      }
    series = _fetchall(
      conn,
      """
      SELECT substr(ts, 1, 10) AS d, COALESCE(SUM(total_pnl),0) AS total_pnl
      FROM pnl_snapshots
      WHERE account_id=? AND ts>=?
      GROUP BY substr(ts, 1, 10)
      ORDER BY d ASC
      """,
      (account_id, since),
    )
    strat = _fetchall(
      conn,
      """
      SELECT strategy_type, volume, realized_pnl, unrealized_pnl, n_trades, ts
      FROM strategy_attribution
      WHERE account_id=?
      ORDER BY realized_pnl DESC, ts DESC
      LIMIT 20
      """,
      (account_id,),
    )
    return {"ok": True, "account_id": account_id, "as_of": latest.get("ts"), "overview": latest, "series": series, "strategy_attribution": strat}
  finally:
    conn.close()


def get_pnl_reconciliation(account_id):
  conn = get_conn()
  try:
    row = _fetchone(
      conn,
      """
      SELECT ts, internal_total_pnl, positions_total_pnl, leaderboard_total_pnl,
             diff_internal_vs_positions, diff_internal_vs_leaderboard, status, notes, metadata_json
      FROM reconciliation_results
      WHERE account_id=?
      ORDER BY ts DESC, id DESC
      LIMIT 1
      """,
      (account_id,),
    )
    if not row:
      return {"ok": True, "account_id": account_id, "status": "empty", "message": "no reconciliation snapshot"}
    return {"ok": True, "account_id": account_id, "reconciliation": row}
  finally:
    conn.close()


def list_ledger_entries(account_id, start_at="", end_at="", market_id="", strategy_type="", limit=200):
  conn = get_conn()
  try:
    qs = ["account_id=?"]
    params = [account_id]
    if start_at:
      qs.append("occurred_at>=?")
      params.append(str(start_at))
    if end_at:
      qs.append("occurred_at<=?")
      params.append(str(end_at))
    if market_id:
      qs.append("market_id=?")
      params.append(str(market_id))
    if strategy_type:
      qs.append("strategy_type=?")
      params.append(str(strategy_type))
    params.append(max(1, min(int(limit or 200), 2000)))
    where = " AND ".join(qs)
    rows = _fetchall(
      conn,
      f"""
      SELECT id, account_id, execution_id, execution_leg_index, activity_type, market_id, token_id, strategy_type,
             usdc_delta, token_delta, fee_delta, rebate_delta, reward_delta, tx_hash, fill_id, occurred_at, source_tag, metadata_json
      FROM ledger_entries
      WHERE {where}
      ORDER BY occurred_at DESC, id DESC
      LIMIT ?
      """,
      tuple(params),
    )
    return rows
  finally:
    conn.close()
