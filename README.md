# Macro Economic Report Website

Static website for your macro monitoring model with three pages:

- `index.html`: dashboard (main page)
- `daily-report.html`: editable auto-drafted daily report
- `glossary.html`: glossary of key macro model terms
- `model.xlsx`: default workbook auto-loaded on dashboard startup

## Run locally

From this folder:

```bash
python3 -m http.server 8080
```

Then open [http://localhost:8080](http://localhost:8080).

## Connect your Excel model

1. Open the dashboard page (it auto-loads `model.xlsx` by default).
2. Click **Load Model (.xlsx)**.
3. Select your workbook file (`宏观监控模型_14维.xlsx` or updated versions).
4. The dashboard will parse model sheets and refresh.

The parsed model snapshot is stored in browser `localStorage` and reused by the Daily Report page.

## Data shown on dashboard

- Summary: total score, regime status, active alerts, top contributors, and macro drivers.
- Core model tables: `Dimensions`, `Indicators`, `Inputs`, `Scores`, `Alerts`.
- Full workbook explorer: all worksheets, all rows, all columns with tab switch.

## Daily report workflow

1. Go to `daily-report.html`.
2. Click **Regenerate Draft** to produce today's report from current model data.
3. Edit text as needed.
4. Click **Save** (stored in browser) or **Download .txt**.
