# Macro Economic Report Website

Bilingual macro dashboard + daily report system with server and SQLite database.

## What is implemented

1. Server + Database
- Backend: Flask (`server/app.py`)
- Database: SQLite (`data/macro_monitor.db`)
- All core website data is stored in DB: snapshots, sheet rows, daily reports, checks, subscribers, email dispatch logs.

2. Online data check before final report:
- In `Daily Report`, click **Generate Final Report**
- If "Run online data check" is enabled, the app fetches latest data for detectable FRED series and updates Inputs snapshot before generating final report text

3. English / Chinese switch:
- Language toggle button in header on all pages

4. Main page shows all 14 dimensions:
- Dedicated card section sourced from `Dimensions` sheet

5. Daily report archive links:
- Each saved report date is shown as a link in `Daily Report Archive`

6. Detailed indicators support:
- Daily report page shows full detailed `Scores` table
- Separate `indicators.html` page shows all indicator information from `Indicators` sheet

7. GitHub Pages deployment:
- Configure Pages source to `main` branch and `/ (root)` path
- Site is served directly from the static files in this repository

8. Daily production pipeline:
- Run `python3 scripts/daily_refresh.py`
- It will:
  - pull latest available online data
  - update `model.xlsx` Inputs + AsOf date
  - recalculate indicator/dimension scores
  - generate `data/latest_snapshot.json` for dashboard
  - generate `reports/<YYYY-MM-DD>.txt` and `reports/<YYYY-MM-DD>.html`
  - update `reports/index.json` for report links
  - append/update `DailyReports` sheet in `model.xlsx`
  - persist all generated data to SQLite DB (`data/macro_monitor.db`)

9. 09:00 China-time automatic generation:
- GitHub Actions workflow: `.github/workflows/daily-report-and-email.yml`
- Schedule: `0 1 * * *` (UTC), equals `09:00` Asia/Shanghai daily
- It automatically:
  - ingests new subscription requests from GitHub issues label `subscription`
  - runs `scripts/daily_refresh.py`
  - sends report summary + link email to all active subscribers
  - commits updated artifacts back to `main`

10. Email subscription (server + DB):
- Dashboard subscription form posts directly to backend `/api/subscribers`.
- Email is stored in DB table `subscribers` and mirrored in `data/subscribers.json` (workflow path).
- Daily workflow sends summary + report link to active subscribers.

11. Token usage monitoring (including OpenAI conversation usage import):
- Internal pipeline usage is logged into table `monitor_token_usage` by `scripts/daily_refresh.py`.
- You can import OpenAI usage (conversation/completions token stats) into the same table:
```bash
export OPENAI_ADMIN_API_KEY="<your_openai_admin_key>"
python3 scripts/import_openai_usage.py --days 2
```
- Recommended cron (hourly):
```bash
5 * * * * cd /opt/MacoEcoReport && /opt/MacoEcoReport/.venv/bin/python scripts/import_openai_usage.py --days 2 >> /var/log/macro-openai-usage.log 2>&1
```

## Pages

- `index.html`: dashboard
- `daily-report.html`: report editor + online check + archive links + detailed scores
- `indicators.html`: full indicators table
- `glossary.html`: glossary terms

## Data file

- Default workbook: `model.xlsx`
- You can upload another `.xlsx` from the dashboard to replace current model data.

## Run Server Locally

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python3 server/app.py
```

Then open: `http://127.0.0.1:5000`

The frontend will use backend APIs automatically on localhost.

## Deploy Backend (Render Recommended)

1. In Render, create a new **Web Service** from this GitHub repo.
2. Render will detect `render.yaml`:
- Start command: `gunicorn server.app:app --bind 0.0.0.0:$PORT`
- Persistent disk mounted at `/var/data`
- SQLite path env: `MACRO_DB_PATH=/var/data/macro_monitor.db`
3. After deploy, verify:
- `GET https://<your-render-domain>/api/health`
- `POST https://<your-render-domain>/api/subscribers`
4. Update frontend API base meta value if your domain is different from:
- `https://macro-monitor-backend.onrender.com`

## Browser DB Migration

- On first load with backend available, frontend migrates existing browser IndexedDB records to backend DB via `/api/migrate`.
- Migration flag is stored in localStorage key `macro-monitor-db-migrated`.

## Required GitHub Secrets

Set in repository `Settings -> Secrets and variables -> Actions`:

- `RESEND_API_KEY`: API key for Resend email sending
- `RESEND_FROM`: sender address, e.g. `Macro Monitor <noreply@your-domain.com>`

## Run locally

```bash
python3 -m http.server 8080
```

Open: `http://localhost:8080`
