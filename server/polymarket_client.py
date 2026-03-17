#!/usr/bin/env python3
import os


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
    self.host = str(os.environ.get("POLYMARKET_CLOB_HOST", "https://clob.polymarket.com")).strip()
    self.chain_id = int(os.environ.get("POLYMARKET_CHAIN_ID", "137"))

  def derive_api_credentials(self, signer_private_key: str, funder_address: str, signature_type):
    if not self.enabled:
      return {"ok": False, "error": "live_disabled"}
    if not signer_private_key:
      return {"ok": False, "error": "missing_private_key"}
    if not funder_address:
      return {"ok": False, "error": "missing_funder_address"}

    try:
      from py_clob_client.client import ClobClient
    except Exception as e:
      return {"ok": False, "error": "py_clob_client_missing", "detail": str(e)[:240]}

    sig_type = _signature_type_to_int(signature_type)
    try:
      client = ClobClient(
        host=self.host,
        key=signer_private_key,
        chain_id=self.chain_id,
        signature_type=sig_type,
        funder=funder_address,
      )
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
