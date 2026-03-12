const STOCK_LANG_KEY = "nexo-stock-lang";
const STOCK_API_BASE = (document.querySelector('meta[name="macro-api-base"]')?.content || "").trim() || "https://api.nexo.hk";
let chartPred = null;
let chartCum = null;
let chartFeat = null;
let knownTickers = [];

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
    tickerNotFound: "No data for this ticker in database",
    enterTicker: "Enter ticker and click Refresh",
    metricNoData: "No metric interpretation available.",
    signalBullish: "Model bias is positive; trend-following entries can be considered with risk controls.",
    signalBearish: "Model bias is negative; reduce exposure or wait for confirmation.",
    signalNeutral: "Model has no clear directional edge; position sizing should stay conservative.",
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
    tickerNotFound: "数据库中暂无此 ticker 数据",
    enterTicker: "输入 ticker 后点击刷新",
    metricNoData: "暂无可解释的指标数据。",
    signalBullish: "模型偏多，说明趋势信号较强，可考虑顺势配置并控制回撤风险。",
    signalBearish: "模型偏空，说明下行风险较高，建议降低仓位或等待确认信号。",
    signalNeutral: "模型方向性优势不明显，建议以轻仓和风控为主。",
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

function normalizeTicker(v){
  return String(v || "").trim().toUpperCase();
}

function signalClass(signal){
  const s = normalizeTicker(signal);
  if (s === "BULLISH") return "stock-signal-bullish";
  if (s === "BEARISH") return "stock-signal-bearish";
  return "stock-signal-neutral";
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
  if ((document.getElementById("stock-query-status")?.textContent || "").trim() === "--"){
    setText("stock-query-status", st("enterTicker"));
  }
}

function paintSignal(signal){
  const signalEl = document.getElementById("stock-latest-signal");
  if (!signalEl) return;
  signalEl.classList.remove("stock-signal-bullish", "stock-signal-bearish", "stock-signal-neutral");
  signalEl.classList.add(signalClass(signal || "Neutral"));
  signalEl.textContent = signal || "--";
  const s = normalizeTicker(signal || "Neutral");
  if (s === "BULLISH") setText("stock-latest-signal-desc", st("signalBullish"));
  else if (s === "BEARISH") setText("stock-latest-signal-desc", st("signalBearish"));
  else setText("stock-latest-signal-desc", st("signalNeutral"));
}

function explainPredReturn(v){
  const n = Number(v);
  if (!Number.isFinite(n)) return st("metricNoData");
  if (n >= 0.05) return sLang() === "zh" ? "模型预期收益较高，意味着短期上涨空间相对充足，但仍需防范波动回撤。" : "Model expects strong upside; potential return is meaningful, but volatility risk remains.";
  if (n >= 0) return sLang() === "zh" ? "模型预期为小幅正收益，意味着偏多但优势有限，适合分批和风控交易。" : "Model expects modest gains; bias is positive but edge is limited, favor staged entries and risk control.";
  return sLang() === "zh" ? "模型预期为负收益，意味着下行压力更大，建议谨慎或降低仓位。" : "Model expects negative return; downside pressure is higher, so caution or smaller exposure is preferred.";
}

function explainUpProb(v){
  const n = Number(v);
  if (!Number.isFinite(n)) return st("metricNoData");
  if (n >= 0.65) return sLang() === "zh" ? "上涨概率较高，说明模型对上涨方向置信度较强。" : "High probability of upside indicates strong model confidence in upward direction.";
  if (n >= 0.5) return sLang() === "zh" ? "上涨概率略高于中性，方向偏多但优势不大。" : "Upside probability is only slightly above neutral; bullish bias exists but is not strong.";
  return sLang() === "zh" ? "上涨概率低于 50%，说明短期偏空风险更高。" : "Upside probability is below 50%, implying higher short-term bearish risk.";
}

function explainAccuracy(v){
  const n = Number(v);
  if (!Number.isFinite(n)) return st("metricNoData");
  if (n >= 0.6) return sLang() === "zh" ? "历史方向准确率较高，模型在该标的上具有较稳定的判断能力。" : "Historical direction accuracy is strong, suggesting stable signal quality for this ticker.";
  if (n >= 0.52) return sLang() === "zh" ? "方向准确率略优于随机，模型有一定参考价值但不应单独使用。" : "Accuracy is modestly above random; useful as reference but not as a standalone decision tool.";
  return sLang() === "zh" ? "方向准确率较低，模型稳定性偏弱，建议结合更多信息。" : "Direction accuracy is weak; combine with additional signals before making decisions.";
}

function explainStrategyCagr(v){
  const n = Number(v);
  if (!Number.isFinite(n)) return st("metricNoData");
  if (n > 0) return sLang() === "zh" ? "策略 CAGR 为正，说明长期复利表现可观，策略具有可持续性潜力。" : "Positive strategy CAGR implies potentially sustainable long-term compounding performance.";
  return sLang() === "zh" ? "策略 CAGR 为负，说明策略长期收益不足，需要优化或降低使用权重。" : "Negative strategy CAGR suggests weak long-term performance and potential need for strategy adjustments.";
}

function explainBuyHoldCagr(v){
  const n = Number(v);
  if (!Number.isFinite(n)) return st("metricNoData");
  if (n > 0) return sLang() === "zh" ? "买入持有 CAGR 为正，标的本身长期趋势偏强，可作为基准对照。" : "Positive buy-and-hold CAGR indicates the underlying has a constructive long-term trend as a benchmark.";
  return sLang() === "zh" ? "买入持有 CAGR 为负，说明标的长期趋势偏弱，择时和风控更重要。" : "Negative buy-and-hold CAGR indicates weak long-term trend; timing and risk control are more important.";
}

function explainSharpe(v){
  const n = Number(v);
  if (!Number.isFinite(n)) return st("metricNoData");
  if (n >= 1) return sLang() === "zh" ? "夏普比率较高，说明单位风险对应的收益效率较好。" : "Sharpe ratio is strong, indicating efficient return per unit of risk.";
  if (n >= 0.5) return sLang() === "zh" ? "夏普比率中等，收益和波动的匹配度一般，需结合回撤管理。" : "Sharpe ratio is moderate; risk-adjusted return is acceptable but requires drawdown control.";
  return sLang() === "zh" ? "夏普比率偏低，说明风险补偿不足，建议谨慎使用该信号。" : "Sharpe ratio is low, implying limited risk compensation; use the signal cautiously.";
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
  knownTickers = list.length ? list : ["PDD"];
  const dl = document.getElementById("stock-ticker-list");
  if (dl) dl.innerHTML = knownTickers.map((t) => `<option value="${t}"></option>`).join("");
  const input = document.getElementById("stock-ticker-input");
  if (input && !String(input.value || "").trim()) input.value = knownTickers[0] || "PDD";
  return knownTickers;
}

async function loadTickerData(ticker){
  const input = document.getElementById("stock-ticker-input");
  const q = normalizeTicker(ticker || input?.value || "");
  if (!q){
    setText("stock-query-status", st("enterTicker"));
    return;
  }
  if (input) input.value = q;
  const [latest, history, features] = await Promise.all([
    apiGet(`/api/stocks/${q}/predict/latest`),
    apiGet(`/api/stocks/${q}/backtest/history?limit=120`),
    apiGet(`/api/stocks/${q}/features/latest?limit=12`),
  ]);
  if (!latest?.ok){
    setText("stock-query-status", `${st("tickerNotFound")}: ${q}`);
    paintSignal("Neutral");
    setText("stock-latest-month", "--");
    setText("kpi-pred", "--");
    setText("kpi-up", "--");
    setText("kpi-acc", "--");
    setText("kpi-scagr", "--");
    setText("kpi-bcagr", "--");
    setText("kpi-sharpe", "--");
    setText("stock-updated-at", `${st("updated")}: --`);
    setText("kpi-pred-desc", st("metricNoData"));
    setText("kpi-up-desc", st("metricNoData"));
    setText("kpi-acc-desc", st("metricNoData"));
    setText("kpi-scagr-desc", st("metricNoData"));
    setText("kpi-bcagr-desc", st("metricNoData"));
    setText("kpi-sharpe-desc", st("metricNoData"));
    renderHistory([]);
    drawCharts([], []);
    return;
  }
  setText("stock-query-status", q);
  paintSignal(latest.signal || "Neutral");
  setText("stock-latest-month", latest.latest_month || "--");
  setText("kpi-pred", fmtPct(latest.predicted_return));
  setText("kpi-up", fmtPct(latest.up_probability));
  const acc = Number(latest.model_metrics?.direction_accuracy);
  const scagr = Number(latest.model_metrics?.strategy_cagr);
  const bcagr = Number(latest.model_metrics?.buy_hold_cagr);
  const sharpe = Number(latest.model_metrics?.sharpe_ratio);
  setText("kpi-acc", fmtPct(acc));
  setText("kpi-scagr", fmtPct(scagr));
  setText("kpi-bcagr", fmtPct(bcagr));
  setText("kpi-sharpe", fmtNum(sharpe, 2));
  setText("kpi-pred-desc", explainPredReturn(Number(latest.predicted_return)));
  setText("kpi-up-desc", explainUpProb(Number(latest.up_probability)));
  setText("kpi-acc-desc", explainAccuracy(acc));
  setText("kpi-scagr-desc", explainStrategyCagr(scagr));
  setText("kpi-bcagr-desc", explainBuyHoldCagr(bcagr));
  setText("kpi-sharpe-desc", explainSharpe(sharpe));
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
  const input = document.getElementById("stock-ticker-input");
  const ticker = normalizeTicker(input?.value || tickers[0] || "PDD");
  await loadTickerData(ticker);
  document.getElementById("stock-refresh")?.addEventListener("click", async () => {
    await loadTickerData(input?.value || ticker);
  });
  input?.addEventListener("keydown", async (evt) => {
    if (evt.key !== "Enter") return;
    evt.preventDefault();
    await loadTickerData(input?.value || ticker);
  });
  document.getElementById("stock-lang-toggle")?.addEventListener("click", async () => {
    localStorage.setItem(STOCK_LANG_KEY, sLang() === "zh" ? "en" : "zh");
    applyStockI18n();
    ensureFooter();
    await loadTickerData(input?.value || ticker);
  });
}

document.addEventListener("DOMContentLoaded", initStockPage);
