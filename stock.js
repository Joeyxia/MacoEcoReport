const STOCK_LANG_KEY = "nexo-stock-lang";
const STOCK_API_BASE = (document.querySelector('meta[name="macro-api-base"]')?.content || "").trim() || "https://api.nexo.hk";
let chartPred = null;
let chartCum = null;
let chartFeat = null;

const stockI18n = {
  en: {
    eyebrow: "Quant Signal Engine",
    title: "Stock Prediction Dashboard",
    desc: "Database-driven prediction, backtest, and feature interpretation.",
    ticker: "Ticker",
    refresh: "Refresh",
    updated: "Updated",
    latestSignal: "Latest Signal",
    predReturn: "Predicted Return",
    upProb: "Up Probability",
    acc: "Direction Accuracy",
    scagr: "Strategy CAGR",
    bcagr: "Buy & Hold CAGR",
    sharpe: "Sharpe Ratio",
    predChart: "Predicted vs Actual Returns",
    cumChart: "Strategy vs Buy & Hold",
    featChart: "Top Features",
    history: "Latest 12 Predictions",
    noData: "No data available",
    month: "Month",
    pred: "Predicted",
    actual: "Actual",
    signal: "Signal",
    strat: "Strategy",
  },
  zh: {
    eyebrow: "量化信号引擎",
    title: "股票预测仪表盘",
    desc: "基于数据库的预测、回测与特征解释。",
    ticker: "股票代码",
    refresh: "刷新",
    updated: "更新时间",
    latestSignal: "最新信号",
    predReturn: "预测收益率",
    upProb: "上涨概率",
    acc: "方向准确率",
    scagr: "策略 CAGR",
    bcagr: "买入持有 CAGR",
    sharpe: "夏普比率",
    predChart: "预测收益率 vs 实际收益率",
    cumChart: "策略累计收益 vs 买入持有",
    featChart: "关键驱动因子",
    history: "最近 12 期预测",
    noData: "暂无可用数据",
    month: "月份",
    pred: "预测",
    actual: "实际",
    signal: "信号",
    strat: "策略收益",
  },
};

function sLang(){
  return localStorage.getItem(STOCK_LANG_KEY) === "zh" ? "zh" : "en";
}

function st(k){
  return (stockI18n[sLang()] || stockI18n.en)[k] || k;
}

function fmtPct(v){
  const n = Number(v || 0);
  return `${(n * 100).toFixed(2)}%`;
}

function fmtNum(v, d = 3){
  return Number(v || 0).toFixed(d);
}

function setText(id, value){
  const el = document.getElementById(id);
  if (el) el.textContent = value;
}

function api(path){
  const p = String(path || "").startsWith("/") ? path : `/${path}`;
  return `${STOCK_API_BASE}${p}`;
}

async function apiGet(path){
  const r = await fetch(api(path), { credentials: "omit" });
  if (!r.ok) return null;
  return r.json();
}

function ensureFooter(){
  if (document.querySelector(".site-footer-note")) return;
  const footer = document.createElement("footer");
  footer.className = "site-footer-note";
  footer.textContent = sLang() === "zh" ? "由 Nexo Marco Intelligence 提供支持" : "Powered by Nexo Marco Intelligence";
  document.body.appendChild(footer);
}

function applyStockI18n(){
  document.documentElement.lang = sLang() === "zh" ? "zh-CN" : "en";
  setText("stock-eyebrow", st("eyebrow"));
  setText("stock-page-title", st("title"));
  setText("stock-page-desc", st("desc"));
  setText("stock-ticker-label", st("ticker"));
  setText("stock-refresh", st("refresh"));
  setText("stock-latest-signal-label", st("latestSignal"));
  setText("kpi_pred_label", st("predReturn"));
  setText("kpi_up_label", st("upProb"));
  setText("kpi_acc_label", st("acc"));
  setText("kpi_scagr_label", st("scagr"));
  setText("kpi_bcagr_label", st("bcagr"));
  setText("kpi_sharpe_label", st("sharpe"));
  setText("chart_pred_title", st("predChart"));
  setText("chart_cum_title", st("cumChart"));
  setText("chart_feat_title", st("featChart"));
  setText("history_title", st("history"));
  const btn = document.getElementById("stock-lang-toggle");
  if (btn) btn.textContent = sLang() === "zh" ? "EN" : "中文";
}

function renderHistory(rows){
  const root = document.getElementById("stock-history-table");
  if (!root) return;
  if (!rows?.length){
    root.innerHTML = `<p class="subtle">${st("noData")}</p>`;
    return;
  }
  const header = [st("month"), st("pred"), st("actual"), st("upProb"), st("signal"), st("strat")];
  const body = rows.slice(0, 12).map((r) => `
    <tr>
      <td>${r.month || ""}</td>
      <td>${fmtPct(r.predicted_return)}</td>
      <td>${fmtPct(r.actual_return)}</td>
      <td>${fmtPct(r.up_probability)}</td>
      <td>${r.signal || ""}</td>
      <td>${fmtPct(r.strategy_return)}</td>
    </tr>
  `).join("");
  root.innerHTML = `
    <table>
      <thead><tr>${header.map((x) => `<th>${x}</th>`).join("")}</tr></thead>
      <tbody>${body}</tbody>
    </table>
  `;
}

function drawCharts(historyRows, featureRows){
  const rows = [...(historyRows || [])].reverse();
  const labels = rows.map((x) => x.month);
  const pred = rows.map((x) => Number(x.predicted_return || 0) * 100);
  const actual = rows.map((x) => Number(x.actual_return || 0) * 100);
  const cumS = rows.map((x) => Number(x.cumulative_strategy || 0) * 100);
  const cumB = rows.map((x) => Number(x.cumulative_buy_hold || 0) * 100);

  if (chartPred) chartPred.destroy();
  if (chartCum) chartCum.destroy();
  if (chartFeat) chartFeat.destroy();

  chartPred = new Chart(document.getElementById("stock-chart-pred"), {
    type: "line",
    data: { labels, datasets: [{ label: st("pred"), data: pred, borderColor: "#0b8d73" }, { label: st("actual"), data: actual, borderColor: "#425a67" }] },
    options: { responsive: true, maintainAspectRatio: false },
  });
  chartCum = new Chart(document.getElementById("stock-chart-cum"), {
    type: "line",
    data: { labels, datasets: [{ label: "Strategy", data: cumS, borderColor: "#0b8d73" }, { label: "Buy&Hold", data: cumB, borderColor: "#425a67" }] },
    options: { responsive: true, maintainAspectRatio: false },
  });

  const fs = featureRows || [];
  chartFeat = new Chart(document.getElementById("stock-chart-feat"), {
    type: "bar",
    data: {
      labels: fs.map((x) => x.feature_name),
      datasets: [{ label: "Importance", data: fs.map((x) => Number(x.importance || 0)), backgroundColor: "#0b8d73" }],
    },
    options: { indexAxis: "y", responsive: true, maintainAspectRatio: false },
  });
}

async function loadTickers(){
  const res = await apiGet("/api/stocks/tickers");
  const list = (res?.tickers || []).map((x) => x.ticker).filter(Boolean);
  const sel = document.getElementById("stock-ticker");
  if (!sel) return [];
  sel.innerHTML = list.map((t) => `<option value="${t}">${t}</option>`).join("");
  if (!list.length) sel.innerHTML = `<option value="PDD">PDD</option>`;
  return list;
}

async function loadTickerData(ticker){
  const [latest, history, features] = await Promise.all([
    apiGet(`/api/stocks/${ticker}/predict/latest`),
    apiGet(`/api/stocks/${ticker}/backtest/history?limit=120`),
    apiGet(`/api/stocks/${ticker}/features/latest?limit=12`),
  ]);
  if (!latest?.ok){
    setText("stock-latest-signal", "--");
    renderHistory([]);
    drawCharts([], []);
    return;
  }
  setText("stock-latest-signal", latest.signal || "--");
  setText("stock-latest-month", latest.latest_month || "--");
  setText("kpi-pred", fmtPct(latest.predicted_return));
  setText("kpi-up", fmtPct(latest.up_probability));
  setText("kpi-acc", fmtPct(latest.model_metrics?.direction_accuracy));
  setText("kpi-scagr", fmtPct(latest.model_metrics?.strategy_cagr));
  setText("kpi-bcagr", fmtPct(latest.model_metrics?.buy_hold_cagr));
  setText("kpi-sharpe", fmtNum(latest.model_metrics?.sharpe_ratio, 2));
  setText("stock-updated-at", `${st("updated")}: ${latest.updated_at || "--"}`);
  const rows = history?.rows || [];
  const feat = features?.rows || latest.top_features || [];
  renderHistory(rows);
  drawCharts(rows, feat);
}

async function initStockPage(){
  applyStockI18n();
  ensureFooter();
  const tickers = await loadTickers();
  const sel = document.getElementById("stock-ticker");
  const ticker = sel?.value || tickers[0] || "PDD";
  await loadTickerData(ticker);
  document.getElementById("stock-refresh")?.addEventListener("click", async () => {
    await loadTickerData(sel?.value || ticker);
  });
  sel?.addEventListener("change", async () => {
    await loadTickerData(sel.value);
  });
  document.getElementById("stock-lang-toggle")?.addEventListener("click", async () => {
    localStorage.setItem(STOCK_LANG_KEY, sLang() === "zh" ? "en" : "zh");
    applyStockI18n();
    ensureFooter();
    await loadTickerData(sel?.value || ticker);
  });
}

document.addEventListener("DOMContentLoaded", initStockPage);
