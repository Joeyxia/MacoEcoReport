# Macro Economic Report Website

A static bilingual website for your macro monitoring model, ready for GitHub Pages.

## What is implemented

1. Browser database (`IndexedDB`) stores:
- Parsed model data from Excel (`current model`)
- Daily reports (date-indexed)
- Online data check history

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

## Pages

- `index.html`: dashboard
- `daily-report.html`: report editor + online check + archive links + detailed scores
- `indicators.html`: full indicators table
- `glossary.html`: glossary terms

## Data file

- Default workbook: `model.xlsx`
- You can upload another `.xlsx` from the dashboard to replace current model data.

## Run locally

```bash
python3 -m http.server 8080
```

Open: `http://localhost:8080`
