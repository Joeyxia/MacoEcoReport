#!/usr/bin/env python3
import os
import time


def _signature_type_to_int(raw):
  s = str(raw or "").strip().lower()
  if s in {"0", "eoa"}:
    return 0
  if s in {"1", "poly_proxy", "proxy"}:
    return 1
  if s in {"2", "gnosis_safe", "safe"}:
    return 2
  return 0


class PolymarketLiveClient:
  def __init__(self):
    self.enabled = str(os.environ.get("POLYMARKET_LIVE_ENABLED", "0")).strip().lower() in {"1", "true", "yes", "on"}
    self.allow_live_orders = str(os.environ.get("POLYMARKET_ALLOW_LIVE_ORDERS", "0")).strip().lower() in {"1", "true", "yes", "on"}
    self.host = str(os.environ.get("POLYMARKET_CLOB_HOST", "https://clob.polymarket.com")).strip()
    self.chain_id = int(os.environ.get("POLYMARKET_CHAIN_ID", "137"))

  def _new_client(self, signer_private_key: str, funder_address: str, signature_type):
    from py_clob_client.client import ClobClient
    sig_type = _signature_type_to_int(signature_type)
    return ClobClient(
      host=self.host,
      key=signer_private_key,
      chain_id=self.chain_id,
      signature_type=sig_type,
      funder=funder_address,
    )

  def derive_api_credentials(self, signer_private_key: str, funder_address: str, signature_type):
    if not self.enabled:
      return {"ok": False, "error": "live_disabled"}
    if not signer_private_key:
      return {"ok": False, "error": "missing_private_key"}
    if not funder_address:
      return {"ok": False, "error": "missing_funder_address"}

    try:
      from py_clob_client.clob_types import ApiCreds  # noqa: F401
    except Exception as e:
      return {"ok": False, "error": "py_clob_client_missing", "detail": str(e)[:240]}

    try:
      sig_type = _signature_type_to_int(signature_type)
      client = self._new_client(signer_private_key, funder_address, signature_type)
      creds = client.create_or_derive_api_creds()
      api_key = str(getattr(creds, "api_key", "") or creds.get("api_key") or "")
      secret = str(getattr(creds, "api_secret", "") or creds.get("api_secret") or creds.get("secret") or "")
      passphrase = str(getattr(creds, "api_passphrase", "") or creds.get("api_passphrase") or creds.get("passphrase") or "")
      if not api_key or not secret or not passphrase:
        return {"ok": False, "error": "empty_creds"}
      return {
        "ok": True,
        "api_key": api_key,
        "api_secret": secret,
        "api_passphrase": passphrase,
        "signature_type": sig_type,
        "host": self.host,
      }
    except Exception as e:
      return {"ok": False, "error": "derive_failed", "detail": str(e)[:400]}

  def _authed_client(self, signer_private_key: str, funder_address: str, signature_type, api_key: str, api_secret: str, api_passphrase: str):
    from py_clob_client.clob_types import ApiCreds
    client = self._new_client(signer_private_key, funder_address, signature_type)
    client.set_api_creds(ApiCreds(api_key=api_key, api_secret=api_secret, api_passphrase=api_passphrase))
    return client

  def get_balance(self, signer_private_key: str, funder_address: str, signature_type, api_key: str, api_secret: str, api_passphrase: str):
    if not self.enabled:
      return {"ok": False, "error": "live_disabled"}
    try:
      from py_clob_client.clob_types import BalanceAllowanceParams, AssetType
      client = self._authed_client(signer_private_key, funder_address, signature_type, api_key, api_secret, api_passphrase)
      p = BalanceAllowanceParams(asset_type=AssetType.COLLATERAL, token_id="", signature_type=_signature_type_to_int(signature_type))
      out = client.get_balance_allowance(p)
      return {"ok": True, "balance": out}
    except Exception as e:
      return {"ok": False, "error": "balance_failed", "detail": str(e)[:320]}

  def list_open_orders(self, signer_private_key: str, funder_address: str, signature_type, api_key: str, api_secret: str, api_passphrase: str):
    if not self.enabled:
      return {"ok": False, "error": "live_disabled"}
    try:
      client = self._authed_client(signer_private_key, funder_address, signature_type, api_key, api_secret, api_passphrase)
      rows = client.get_orders()
      return {"ok": True, "items": rows if isinstance(rows, list) else []}
    except Exception as e:
      return {"ok": False, "error": "list_orders_failed", "detail": str(e)[:320]}

  def fetch_markets(self, max_markets=200, max_pages=4):
    if not self.enabled:
      return {"ok": False, "error": "live_disabled"}
    try:
      # read-only market endpoints; auth is not required but ClobClient is shared.
      signer_private_key = str(os.environ.get("POLYMARKET_PRIVATE_KEY", "")).strip()
      funder_address = str(os.environ.get("POLYMARKET_FUNDER_ADDRESS", "")).strip()
      signature_type = str(os.environ.get("POLYMARKET_SIGNATURE_TYPE", "eoa")).strip()
      client = self._new_client(signer_private_key, funder_address, signature_type)
      out = []
      cursor = "MA=="
      pages = 0
      while pages < max(1, int(max_pages)):
        rsp = client.get_markets(next_cursor=cursor)
        data = rsp.get("data") if isinstance(rsp, dict) else []
        if not isinstance(data, list) or not data:
          break
        out.extend(data)
        cursor = str((rsp or {}).get("next_cursor") or "")
        pages += 1
        if len(out) >= max(1, int(max_markets)):
          break
        if not cursor:
          break
      return {"ok": True, "items": out[: max(1, int(max_markets))]}
    except Exception as e:
      return {"ok": False, "error": "fetch_markets_failed", "detail": str(e)[:320]}

  def fetch_order_book(self, token_id: str):
    if not self.enabled:
      return {"ok": False, "error": "live_disabled"}
    if not token_id:
      return {"ok": False, "error": "missing_token_id"}
    try:
      signer_private_key = str(os.environ.get("POLYMARKET_PRIVATE_KEY", "")).strip()
      funder_address = str(os.environ.get("POLYMARKET_FUNDER_ADDRESS", "")).strip()
      signature_type = str(os.environ.get("POLYMARKET_SIGNATURE_TYPE", "eoa")).strip()
      client = self._new_client(signer_private_key, funder_address, signature_type)
      ob = client.get_order_book(token_id)
      bids = [{"price": float(x.price), "size": float(x.size)} for x in (getattr(ob, "bids", []) or [])]
      asks = [{"price": float(x.price), "size": float(x.size)} for x in (getattr(ob, "asks", []) or [])]
      return {
        "ok": True,
        "token_id": token_id,
        "best_bid": (bids[0]["price"] if bids else None),
        "best_ask": (asks[0]["price"] if asks else None),
        "bids": bids[:20],
        "asks": asks[:20],
        "ts": str(getattr(ob, "timestamp", "") or int(time.time())),
      }
    except Exception as e:
      return {"ok": False, "error": "fetch_book_failed", "detail": str(e)[:320]}

  def place_limit_order(
    self,
    signer_private_key: str,
    funder_address: str,
    signature_type,
    api_key: str,
    api_secret: str,
    api_passphrase: str,
    token_id: str,
    side: str,
    price: float,
    size: float,
    post_only: bool = False,
    order_type: str = "GTC",
  ):
    if not self.enabled:
      return {"ok": False, "error": "live_disabled"}
    if not self.allow_live_orders:
      return {"ok": False, "error": "live_orders_disabled"}
    try:
      from py_clob_client.clob_types import OrderArgs
      client = self._authed_client(signer_private_key, funder_address, signature_type, api_key, api_secret, api_passphrase)
      args = OrderArgs(
        token_id=str(token_id),
        price=float(price),
        size=float(size),
        side=str(side).upper(),
      )
      order = client.create_order(args)
      posted = client.post_order(order, orderType=str(order_type or "GTC").upper(), post_only=bool(post_only))
      oid = None
      if isinstance(posted, dict):
        oid = posted.get("orderID") or posted.get("order_id") or posted.get("id")
      return {"ok": True, "order_id": oid, "raw": posted}
    except Exception as e:
      return {"ok": False, "error": "place_order_failed", "detail": str(e)[:400]}
