#!/usr/bin/env python3
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]
SUBSCRIBERS_PATH = ROOT / "data" / "subscribers.json"
EMAIL_RE = re.compile(r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", re.I)


def api_call(url: str, method: str = "GET", payload=None):
  token = os.environ.get("GITHUB_TOKEN", "")
  headers = {
    "Accept": "application/vnd.github+json",
    "User-Agent": "macro-monitor-bot",
  }
  if token:
    headers["Authorization"] = f"Bearer {token}"
  data = None
  if payload is not None:
    data = json.dumps(payload).encode("utf-8")
    headers["Content-Type"] = "application/json"
  req = Request(url, data=data, headers=headers, method=method)
  with urlopen(req, timeout=30) as resp:
    return json.loads(resp.read().decode("utf-8"))


def load_subscribers():
  if not SUBSCRIBERS_PATH.exists():
    return {"subscribers": []}
  try:
    payload = json.loads(SUBSCRIBERS_PATH.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
      return {"subscribers": []}
    if not isinstance(payload.get("subscribers"), list):
      payload["subscribers"] = []
    return payload
  except Exception:
    return {"subscribers": []}


def save_subscribers(payload):
  SUBSCRIBERS_PATH.parent.mkdir(parents=True, exist_ok=True)
  SUBSCRIBERS_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def extract_email(*texts):
  for text in texts:
    if not text:
      continue
    hit = EMAIL_RE.search(str(text))
    if hit:
      return hit.group(0).lower()
  return ""


def main():
  repo = os.environ.get("GITHUB_REPOSITORY", "").strip()
  if not repo or "/" not in repo:
    raise SystemExit("GITHUB_REPOSITORY not found")

  owner, name = repo.split("/", 1)
  base = f"https://api.github.com/repos/{owner}/{name}"
  query = urlencode({"state": "open", "labels": "subscription", "per_page": 100})
  issues = api_call(f"{base}/issues?{query}")

  payload = load_subscribers()
  now = datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
  existing = {str(x.get("email", "")).lower(): x for x in payload["subscribers"] if x.get("email")}
  added = 0

  for issue in issues:
    if issue.get("pull_request"):
      continue
    number = issue.get("number")
    email = extract_email(issue.get("title"), issue.get("body"))
    if not email:
      api_call(
        f"{base}/issues/{number}/comments",
        method="POST",
        payload={"body": "Invalid subscription request: no valid email found. Please submit again."},
      )
      api_call(f"{base}/issues/{number}", method="PATCH", payload={"state": "closed"})
      continue

    if email not in existing:
      existing[email] = {"email": email, "createdAt": now, "status": "active"}
      added += 1

    api_call(
      f"{base}/issues/{number}/comments",
      method="POST",
      payload={"body": f"Subscription confirmed for `{email}`. This issue is now closed."},
    )
    api_call(f"{base}/issues/{number}", method="PATCH", payload={"state": "closed"})

  merged = sorted(existing.values(), key=lambda x: x.get("createdAt", ""))
  save_subscribers({"subscribers": merged})
  print(f"processed={len(issues)} added={added} total={len(merged)}")


if __name__ == "__main__":
  try:
    main()
  except HTTPError as e:
    body = e.read().decode("utf-8", errors="ignore")
    raise SystemExit(f"GitHub API error: HTTP {e.code} {body[:400]}")
