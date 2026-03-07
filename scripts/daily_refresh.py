#!/usr/bin/env python3
import csv
import io
import json
import math
import ssl
import sys
from datetime import date, datetime
from pathlib import Path
from urllib.request import Request, urlopen

from openpyxl import load_workbook

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "server"))
from db import init_db, replace_sheet_rows, save_model_snapshot, upsert_daily_report

MODEL_PATH = ROOT / "model.xlsx"
REPORTS_DIR = ROOT / "reports"
DATA_DIR = ROOT / "data"
TODAY = date.today().isoformat()
GENERATED_AT = datetime.utcnow().isoformat(timespec="seconds") + "Z"
CTX = ssl._create_unverified_context()


def fetch_text(url: str) -> str:
    req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urlopen(req, timeout=30, context=CTX) as resp:
        return resp.read().decode("utf-8", errors="ignore")


def fetch_json(url: str):
    return json.loads(fetch_text(url))


def fred_last(series: str):
    txt = fetch_text(f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={series}")
    rows = list(csv.reader(io.StringIO(txt)))
    for d, v in reversed(rows[1:]):
        if v and v != ".":
            return d, float(v)
    raise ValueError(f"No value for {series}")


def fred_yoy(series: str):
    txt = fetch_text(f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={series}")
    rows = list(csv.reader(io.StringIO(txt)))[1:]
    vals = [(d, float(v)) for d, v in rows if v and v != "."]
    if len(vals) < 13:
        raise ValueError(f"Insufficient history for {series}")
    d, v = vals[-1]
    _, v12 = vals[-13]
    return d, (v / v12 - 1) * 100


def first_series_token(raw: str) -> str:
    text = str(raw or "").upper()
    for token in text.replace("&", " ").replace("/", " ").replace(",", " ").split():
        token = token.strip()
        if token and token[0].isalpha() and all(ch.isalnum() or ch == "_" for ch in token):
            return token
    return ""


def clamp(v, lo, hi):
    return max(lo, min(hi, v))


def as_number(v):
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).replace(",", "").strip()
    if not s:
        return None
    try:
        return float(s)
    except Exception:
        return None


def serializable(v):
    if v is None:
        return None
    if isinstance(v, (int, float, str, bool)):
        if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
            return None
        return v
    return str(v)


def sheet_to_dicts(ws):
    header = None
    rows = []
    for r in ws.iter_rows(values_only=True):
        vals = [serializable(x) for x in r]
        if header is None:
            # find first complete header row
            if vals and vals[0] and any(vals[1:]):
                header = vals
            continue
        if not any(v is not None and str(v).strip() != "" for v in vals):
            continue
        row = {}
        for i, h in enumerate(header):
            if h is None:
                continue
            if i < len(vals):
                row[str(h)] = vals[i]
        rows.append(row)
    return rows


def status_from_score(score):
    if score >= 75:
        return "扩张偏热"
    if score >= 60:
        return "温和扩张"
    if score >= 45:
        return "中性脆弱"
    if score >= 30:
        return "防御区"
    return "衰退/危机"


def run():
    wb = load_workbook(MODEL_PATH)
    ws_dim = wb["Dimensions"]
    ws_ind = wb["Indicators"]
    ws_in = wb["Inputs"]

    # load indicators
    indicators = {}
    for r in range(2, ws_ind.max_row + 1):
        code = ws_ind.cell(r, 1).value
        if not code:
            continue
        indicators[str(code)] = {
            "row": r,
            "DimensionID": ws_ind.cell(r, 2).value,
            "IndicatorName": ws_ind.cell(r, 3).value,
            "Source": ws_ind.cell(r, 6).value,
            "SourceURL": ws_ind.cell(r, 7).value,
            "Series": ws_ind.cell(r, 8).value,
            "ScaleType": ws_ind.cell(r, 9).value,
            "Direction": ws_ind.cell(r, 10).value,
            "Best": ws_ind.cell(r, 11).value,
            "Worst": ws_ind.cell(r, 12).value,
            "WorstLow": ws_ind.cell(r, 13).value,
            "TargetLow": ws_ind.cell(r, 14).value,
            "TargetHigh": ws_ind.cell(r, 15).value,
            "WorstHigh": ws_ind.cell(r, 16).value,
            "CapLow": ws_ind.cell(r, 17).value,
            "CapHigh": ws_ind.cell(r, 18).value,
            "WeightWithinDim": ws_ind.cell(r, 19).value,
        }

    input_rows = {}
    for r in range(7, ws_in.max_row + 1):
        code = ws_in.cell(r, 1).value
        if code:
            input_rows[str(code)] = r

    ws_in["B2"] = TODAY

    cache = {}

    def s_last(series):
        if series in cache:
            return cache[series]
        cache[series] = fred_last(series)
        return cache[series]

    updated = []
    failed = []

    for code in indicators:
        try:
            source = str(indicators[code].get("Source") or "").lower()
            series_hint = indicators[code].get("Series")
            d = None
            v = None
            if code == "YC_10Y3M":
                d1, x = s_last("DGS10")
                d2, y = s_last("DGS3MO")
                d = max(d1, d2)
                v = (x - y) * 100
            elif code == "SOFR":
                d, v = s_last("SOFR")
            elif code == "FED_ASSETS":
                d, x = s_last("WALCL")
                v = x / 1_000_000
            elif code == "NET_LIQ_PROXY":
                d1, a = s_last("WALCL")
                d2, tga = s_last("WTREGEN")
                d3, rrp = s_last("RRPONTSYD")
                d = max(d1, d2, d3)
                v = (a / 1000 - tga - rrp) / 1000
            elif code == "GDP_QOQ_SAAR":
                d, v = s_last("A191RL1Q225SBEA")
            elif code == "CLAIMS_4WMA":
                d, v = s_last("IC4WSA")
            elif code == "CORE_CPI_YOY":
                d, v = fred_yoy("CPILFESL")
            elif code == "CORE_PCE_YOY":
                d, v = fred_yoy("PCEPILFE")
            elif code == "BREAKEVEN_5Y5Y":
                d, v = s_last("T5YIFR")
            elif code == "UNRATE":
                d, v = s_last("UNRATE")
            elif code == "WAGE_GROWTH":
                d, v = fred_yoy("CES0500000003")
            elif code == "CC_DELINQ":
                d, v = s_last("DRCCLACBS")
            elif code == "REAL_DISP_INC":
                d, v = fred_yoy("DSPIC96")
            elif code == "HY_OAS":
                d, x = s_last("BAMLH0A0HYM2")
                v = x * 100 if x < 50 else x
            elif code == "MORTGAGE_30Y":
                d, v = s_last("MORTGAGE30US")
            elif code == "HOUSING_STARTS":
                d, x = s_last("HOUST")
                v = x / 1000
            elif code == "VIX":
                d, v = s_last("VIXCLS")
            elif code == "DXY":
                d, v = s_last("DTWEXBGS")
            elif code == "US_JP_10Y_SPREAD":
                d1, us10 = s_last("DGS10")
                d2, jp10 = s_last("IRLTLT01JPM156N")
                d = max(d1, d2)
                v = (us10 - jp10) * 100
            elif code == "FCI":
                d, v = s_last("NFCI")
            elif code == "BANK_LENDING":
                d, v = fred_yoy("TOTBKCR")
            elif code == "TED_SPREAD":
                d, v = s_last("TEDRATE")
            elif code == "WTI":
                d, v = s_last("DCOILWTICO")
            elif code == "BTC":
                j = fetch_json("https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd")
                d = TODAY
                v = float(j["bitcoin"]["usd"])
            elif code == "USDC_MCAP":
                j = fetch_json("https://api.coingecko.com/api/v3/coins/usd-coin")
                d = TODAY
                v = float(j["market_data"]["market_cap"]["usd"]) / 1_000_000_000
            elif "fred" in source:
                token = first_series_token(series_hint)
                if not token:
                    raise ValueError("no FRED series token")
                d, v = s_last(token)
            else:
                raise ValueError("manual/proprietary source required")

            row = input_rows.get(code)
            if row and v is not None:
                ws_in.cell(row, 2).value = round(v, 4)
                ws_in.cell(row, 3).value = d
                ws_in.cell(row, 4).value = TODAY
            updated.append({"code": code, "value": round(v, 6), "date": d})
        except Exception as e:
            failed.append({"code": code, "error": str(e)[:180]})

    # read dimensions
    dimensions = []
    for r in range(2, ws_dim.max_row + 1):
        did = ws_dim.cell(r, 1).value
        if not did:
            continue
        dimensions.append({
            "id": str(did),
            "name": ws_dim.cell(r, 2).value,
            "tier": ws_dim.cell(r, 3).value,
            "weight": as_number(ws_dim.cell(r, 4).value) or 0,
            "definition": ws_dim.cell(r, 5).value,
            "update": ws_dim.cell(r, 6).value,
        })

    # map input values
    input_values = {}
    input_meta = {}
    for code, r in input_rows.items():
        v = as_number(ws_in.cell(r, 2).value)
        value_date = serializable(ws_in.cell(r, 3).value)
        source_date = serializable(ws_in.cell(r, 4).value)
        input_meta[code] = {"value_date": value_date, "source_date": source_date}
        if v is not None:
            input_values[code] = v

    # calculate indicator scores
    indicator_scores = []
    for code, m in indicators.items():
        if code not in input_values:
            continue
        x = input_values[code]
        cap_low = as_number(m["CapLow"])
        cap_high = as_number(m["CapHigh"])
        if cap_low is not None:
            x = max(x, cap_low)
        if cap_high is not None:
            x = min(x, cap_high)

        scale = str(m["ScaleType"] or "").lower()
        direction = str(m["Direction"] or "").lower()
        best = as_number(m["Best"])
        worst = as_number(m["Worst"])
        wl = as_number(m["WorstLow"])
        tl = as_number(m["TargetLow"])
        th = as_number(m["TargetHigh"])
        wh = as_number(m["WorstHigh"])
        w = as_number(m["WeightWithinDim"]) or 0

        score = None
        if "targetband" in scale and None not in (wl, tl, th, wh):
            if tl <= x <= th:
                score = 100
            elif x < tl:
                score = ((x - wl) / (tl - wl)) * 100
            else:
                score = ((wh - x) / (wh - th)) * 100
        elif "higher" in direction and None not in (best, worst) and best != worst:
            score = ((x - worst) / (best - worst)) * 100
        elif "lower" in direction and None not in (best, worst) and worst != best:
            score = ((worst - x) / (worst - best)) * 100

        if score is None or not math.isfinite(score):
            continue
        score = clamp(score, 0, 100)
        indicator_scores.append({
            "IndicatorCode": code,
            "DimensionID": m["DimensionID"],
            "IndicatorName": m["IndicatorName"],
            "LatestValue": round(input_values[code], 4),
            "Score(0-100)": round(score, 2),
            "WeightWithinDim": round(w, 4),
            "WeightedScore": round(score * w, 4),
        })

    # dimension scores
    dim_summary = []
    for d in dimensions:
        rows = [r for r in indicator_scores if str(r["DimensionID"]) == d["id"]]
        wsum = sum(r["WeightWithinDim"] for r in rows)
        wscore = sum(r["WeightedScore"] for r in rows)
        dim_score = wscore / wsum if wsum > 0 else 0
        contrib = dim_score * d["weight"] / 100
        dim_summary.append({
            "id": d["id"],
            "name": d["name"],
            "tier": d["tier"],
            "weight": d["weight"],
            "score": round(dim_score, 2),
            "contribution": round(contrib, 4),
            "definition": d["definition"],
            "update": d["update"],
        })

    total_score = round(sum(x["contribution"] for x in dim_summary), 2)
    status = status_from_score(total_score)

    # alerts based on updated values
    val = input_values
    alerts = [
        {"id": "A01", "level": "RED", "condition": "VIX > 30", "triggered": (val.get("VIX", -999) > 30)},
        {"id": "A02", "level": "RED", "condition": "MOVE > 140", "triggered": (val.get("MOVE", -999) > 140)},
        {"id": "A03", "level": "YELLOW", "condition": "HY OAS > 600bps", "triggered": (val.get("HY_OAS", -999) > 600)},
        {"id": "A04", "level": "YELLOW", "condition": "10Y-3M < -50bps", "triggered": (val.get("YC_10Y3M", 999) < -50)},
        {"id": "A05", "level": "YELLOW", "condition": "失业率 > 6%", "triggered": (val.get("UNRATE", -999) > 6)},
        {"id": "A06", "level": "YELLOW", "condition": "核心PCE > 3.5%", "triggered": (val.get("CORE_PCE_YOY", -999) > 3.5)},
        {"id": "A07", "level": "YELLOW", "condition": "WTI > 100", "triggered": (val.get("WTI", -999) > 100)},
    ]

    ranked = sorted(dim_summary, key=lambda x: x["score"]) 
    weak = ranked[:3]
    strong = list(reversed(ranked[-3:]))

    drivers = [
        {"title": "Primary Support", "text": f"{strong[0]['name']} is the strongest block ({strong[0]['score']})." if strong else ""},
        {"title": "Primary Drag", "text": f"{weak[0]['name']} is the key drag ({weak[0]['score']})." if weak else ""},
        {"title": "Risk Trigger", "text": f"{sum(1 for a in alerts if a['triggered'])} alert(s) currently triggered."},
    ]

    key_watch = [
        {"label": "YC_10Y3M", "value": val.get("YC_10Y3M")},
        {"label": "VIX", "value": val.get("VIX")},
        {"label": "HY_OAS", "value": val.get("HY_OAS")},
        {"label": "CORE_CPI_YOY", "value": val.get("CORE_CPI_YOY")},
        {"label": "UNRATE", "value": val.get("UNRATE")},
        {"label": "WTI", "value": val.get("WTI")},
        {"label": "BTC", "value": val.get("BTC")},
    ]
    key_indicators_snapshot = [
        {"title": "10Y-3M利差", "label": "YC_10Y3M", "value": val.get("YC_10Y3M"), "source": "FRED"},
        {"title": "VIX指数", "label": "VIX", "value": val.get("VIX"), "source": "FRED/CBOE"},
        {"title": "HY OAS", "label": "HY_OAS", "value": val.get("HY_OAS"), "source": "FRED"},
        {"title": "核心CPI同比", "label": "CORE_CPI_YOY", "value": val.get("CORE_CPI_YOY"), "source": "FRED/BLS"},
        {"title": "失业率", "label": "UNRATE", "value": val.get("UNRATE"), "source": "FRED/BLS"},
        {"title": "WTI油价", "label": "WTI", "value": val.get("WTI"), "source": "FRED"},
    ]
    top_dimension_contributors = sorted(
        [{"name": d["name"], "score": d["score"], "contribution": d["contribution"], "id": d["id"]} for d in dim_summary],
        key=lambda x: x["contribution"],
        reverse=True,
    )
    daily_watched_items = [f"{x['label']}: {x['value']}" for x in key_watch if x.get("value") is not None]
    updated_set = {x["code"] for x in updated}
    failed_map = {x["code"]: x.get("error", "") for x in failed}
    indicator_details = []
    for code, m in indicators.items():
        current_value = input_values.get(code)
        verified = code in updated_set
        status = "Verified Online" if verified else "Fallback (latest available in model inputs)"
        indicator_details.append(
            {
                "IndicatorCode": code,
                "IndicatorName": m["IndicatorName"],
                "DimensionID": m["DimensionID"],
                "Source": m["Source"],
                "Series/Code": m["Series"],
                "LatestValue": current_value,
                "ValueDate": input_meta.get(code, {}).get("value_date"),
                "SourceDate": input_meta.get(code, {}).get("source_date"),
                "VerifiedOnline": verified,
                "VerificationStatus": status,
                "VerificationError": failed_map.get(code, ""),
                "GeneratedAt": GENERATED_AT,
            }
        )

    all14_dimensions_detailed = [
        {
            "DimensionID": d["id"],
            "DimensionName": d["name"],
            "Tier": d["tier"],
            "Weight(%)": d["weight"],
            "Definition": d["definition"],
            "Typical Update": d["update"],
            "DimScore": d["score"],
            "WeightedContribution": d["contribution"],
        }
        for d in dim_summary
    ]

    # persist workbook
    wb.save(MODEL_PATH)

    REPORTS_DIR.mkdir(exist_ok=True)
    DATA_DIR.mkdir(exist_ok=True)

    short_summary = (
        f"Macro model updated on {TODAY}. Public-source updater refreshed {len(updated)} indicators; "
        f"{len(failed)} indicators still require manual/proprietary updates. "
        f"Composite score: {total_score} ({status})."
    )

    report_text_lines = [
        f"Macro Daily Report ({TODAY})",
        f"Model As-Of: {TODAY}",
        f"Composite Score: {total_score} ({status})",
        "",
        "Key Indicators To Watch",
    ]
    for item in key_watch:
        if item["value"] is not None:
            report_text_lines.append(f"- {item['label']}: {round(item['value'],4)}")
    report_text_lines += [
        "",
        "Short Summary",
        f"- {short_summary}",
        f"- Data generated at: {GENERATED_AT}",
        "- Weakest dimensions: " + ", ".join([f"{x['id']} {x['name']} ({x['score']})" for x in weak]),
        "- Strongest dimensions: " + ", ".join([f"{x['id']} {x['name']} ({x['score']})" for x in strong]),
    ]
    report_text = "\n".join(report_text_lines) + "\n"

    (REPORTS_DIR / f"{TODAY}.txt").write_text(report_text, encoding="utf-8")

    def render_dim_cards(items):
        parts=[]
        for d in items:
            related=[x for x in indicator_scores if str(x['DimensionID'])==d['id']][:3]
            li=''.join([f"<li><strong>{r['IndicatorCode']}</strong>: {r['LatestValue']} | score {r['Score(0-100)']}</li>" for r in related])
            parts.append(f"<div class='preview-dim-card'><strong>{d['id']} {d['name']}</strong><div>Weight {d['weight']}% | Score {d['score']} | Contribution {d['contribution']}</div><ul>{li}</ul></div>")
        return ''.join(parts)

    # group by tier
    tiers={}
    for d in dim_summary:
        tiers.setdefault(d['tier'] or 'Other',[]).append(d)

    tier_html=''
    for tier,items in tiers.items():
        tier_html += f"<section class='preview-tier'><h2>{tier}</h2>{render_dim_cards(items)}</section>"

    report_html=f"""<!doctype html><html><head><meta charset='utf-8'><meta name='viewport' content='width=device-width,initial-scale=1'><title>Macro Daily Report {TODAY}</title><link rel='stylesheet' href='../styles.css'></head><body><main class='page'><section class='panel preview-header'><h1>Macro Daily Report ({TODAY})</h1><p>Composite Score: {total_score} ({status})</p><p>{short_summary}</p></section><section class='panel preview-section'><h2>Key Indicators To Watch</h2><ul class='preview-list'>{''.join([f"<li>{i['label']}: {round(i['value'],4)}</li>" for i in key_watch if i['value'] is not None])}</ul></section>{tier_html}</main></body></html>"""
    (REPORTS_DIR / f"{TODAY}.html").write_text(report_html, encoding="utf-8")

    ws_daily = wb["DailyReports"] if "DailyReports" in wb.sheetnames else wb.create_sheet("DailyReports")
    if ws_daily.max_row < 1 or ws_daily.cell(1, 1).value != "Date":
        ws_daily.cell(1, 1).value = "Date"
        ws_daily.cell(1, 2).value = "AsOf"
        ws_daily.cell(1, 3).value = "TotalScore"
        ws_daily.cell(1, 4).value = "Status"
        ws_daily.cell(1, 5).value = "Summary"
        ws_daily.cell(1, 6).value = "GeneratedAt"
        ws_daily.cell(1, 7).value = "ReportPath"
    existing_row = None
    for rr in range(2, ws_daily.max_row + 1):
        if str(ws_daily.cell(rr, 1).value or "") == TODAY:
            existing_row = rr
            break
    target_row = existing_row or (ws_daily.max_row + 1)
    ws_daily.cell(target_row, 1).value = TODAY
    ws_daily.cell(target_row, 2).value = TODAY
    ws_daily.cell(target_row, 3).value = total_score
    ws_daily.cell(target_row, 4).value = status
    ws_daily.cell(target_row, 5).value = short_summary
    ws_daily.cell(target_row, 6).value = GENERATED_AT
    ws_daily.cell(target_row, 7).value = f"reports/{TODAY}.html"

    index_path = REPORTS_DIR / "index.json"
    reports = []
    if index_path.exists():
        try:
            reports = json.loads(index_path.read_text(encoding="utf-8")).get("reports", [])
        except Exception:
            reports = []

    today_entry = {
        "date": TODAY,
        "meta": {"score": str(total_score), "status": status},
        "text": report_text,
        "path": f"reports/{TODAY}.html",
        "reportPayload": {
            "topDimensionContributors": top_dimension_contributors,
            "triggerAlerts": alerts,
            "dailyWatchedItems": daily_watched_items,
            "primaryDrivers": drivers,
            "keyIndicatorsSnapshot": key_indicators_snapshot,
            "all14DimensionsDetailed": all14_dimensions_detailed,
            "latestReportSummary": short_summary,
            "indicatorDetails": indicator_details,
            "generatedAt": GENERATED_AT,
        },
    }
    merged = [today_entry] + [r for r in reports if r.get("date") != TODAY]
    index_path.write_text(json.dumps({"reports": merged}, ensure_ascii=False, indent=2), encoding="utf-8")

    snapshot = {
        "asOf": TODAY,
        "reportDate": TODAY,
        "totalScore": total_score,
        "status": status,
        "alerts": alerts,
        "dimensions": [{"name": d["name"], "score": d["score"], "contribution": d["contribution"], "id": d["id"]} for d in dim_summary],
        "drivers": drivers,
        "keyWatch": key_watch,
        "latestReportSummary": short_summary,
        "generatedAt": GENERATED_AT,
        "topDimensionContributors": top_dimension_contributors,
        "triggerAlerts": alerts,
        "dailyWatchedItems": daily_watched_items,
        "primaryDrivers": drivers,
        "keyIndicatorsSnapshot": key_indicators_snapshot,
        "all14DimensionsDetailed": all14_dimensions_detailed,
        "indicatorDetails": indicator_details,
        "tables": {
            "dimensions": sheet_to_dicts(ws_dim),
            "indicators": sheet_to_dicts(ws_ind),
            "inputs": sheet_to_dicts(ws_in),
            "scores": indicator_scores,
            "alerts": alerts,
        },
        "onlineUpdate": {"updated_count": len(updated), "failed_count": len(failed), "updated": updated, "failed": failed}
    }
    (DATA_DIR / "latest_snapshot.json").write_text(json.dumps(snapshot, ensure_ascii=False, indent=2), encoding="utf-8")
    (ROOT / "data_update_log.json").write_text(json.dumps(snapshot["onlineUpdate"], ensure_ascii=False, indent=2), encoding="utf-8")

    init_db()
    save_model_snapshot(snapshot)
    replace_sheet_rows("Dimensions", snapshot["tables"]["dimensions"], TODAY)
    replace_sheet_rows("Indicators", snapshot["tables"]["indicators"], TODAY)
    replace_sheet_rows("Inputs", snapshot["tables"]["inputs"], TODAY)
    replace_sheet_rows("Scores", snapshot["tables"]["scores"], TODAY)
    replace_sheet_rows("Alerts", snapshot["tables"]["alerts"], TODAY)
    replace_sheet_rows(
        "DailyReports",
        [
            {
                "Date": TODAY,
                "AsOf": TODAY,
                "TotalScore": total_score,
                "Status": status,
                "Summary": short_summary,
                "GeneratedAt": GENERATED_AT,
                "ReportPath": f"reports/{TODAY}.html",
            }
        ],
        TODAY,
    )
    upsert_daily_report(
        report_date=TODAY,
        text=report_text,
        meta={"score": total_score, "status": status, "summary": short_summary},
        report_path=f"reports/{TODAY}.html",
        payload=today_entry.get("reportPayload"),
    )

    print(f"DONE as_of={TODAY} updated={len(updated)} failed={len(failed)} total_score={total_score}")


if __name__ == "__main__":
    run()
