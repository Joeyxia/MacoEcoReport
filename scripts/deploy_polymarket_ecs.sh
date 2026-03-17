#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   bash scripts/deploy_polymarket_ecs.sh <repo_dir> <systemd_service>
#
# Example:
#   bash scripts/deploy_polymarket_ecs.sh /root/MacoEcoReport nexo-api

REPO_DIR="${1:-}"
SERVICE_NAME="${2:-}"

if [[ -z "${REPO_DIR}" || -z "${SERVICE_NAME}" ]]; then
  echo "Usage: bash scripts/deploy_polymarket_ecs.sh <repo_dir> <systemd_service>"
  exit 1
fi

if [[ ! -d "${REPO_DIR}" ]]; then
  echo "Repo directory not found: ${REPO_DIR}"
  exit 1
fi

echo "[1/7] Enter repo: ${REPO_DIR}"
cd "${REPO_DIR}"

echo "[2/7] Sync latest code from origin/main"
git fetch origin
git checkout main
git pull --ff-only origin main

echo "[3/7] Install dependencies"
if [[ -x ".venv/bin/pip" ]]; then
  .venv/bin/pip install -r requirements.txt
else
  python3 -m pip install -r requirements.txt
fi

echo "[4/7] Compile-check key polymarket files"
if [[ -x ".venv/bin/python" ]]; then
  PY=".venv/bin/python"
else
  PY="python3"
fi
"${PY}" -m py_compile \
  server/app.py \
  server/db.py \
  server/polymarket_service.py \
  server/polymarket_client.py

echo "[5/7] Restart service: ${SERVICE_NAME}"
systemctl daemon-reload || true
systemctl restart "${SERVICE_NAME}"

echo "[6/7] Service status"
systemctl --no-pager --full status "${SERVICE_NAME}" | sed -n '1,40p'

echo "[7/7] API smoke tests"
echo "Health:"
curl -sS "https://api.nexo.hk/monitor-api/health" || true
echo
echo "Polymarket accounts route (expect 401/403/200, but not 404):"
HTTP_CODE="$(curl -s -o /tmp/polymarket_accounts_check.out -w '%{http_code}' 'https://api.nexo.hk/monitor-api/polymarket/accounts')"
echo "HTTP ${HTTP_CODE}"
if [[ "${HTTP_CODE}" == "404" ]]; then
  echo "ERROR: polymarket route missing after deploy."
  exit 1
fi
echo "Deploy finished."
