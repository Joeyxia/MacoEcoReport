const STOCK_LANG_KEY = "nexo-stock-lang";
const STOCK_API_BASE = (document.querySelector('meta[name="macro-api-base"]')?.content || "").trim() || "https://api.nexo.hk";
let chartPred = null;
let chartCum = null;
let chartFeat = null;
let knownTickers = [];

const stockI18n = {
  en: {
    titleMeta: "Macro Risk Monitor | Stock Prediction",
    brand: "Macro Risk Monitor",
    navDashboard: "Dashboard",
    navDaily: "Daily Report",
    navIndicators: "Indicators",
    navGlossary: "Glossary",
    navSubscribe: "Subscribe",
    navAi: "AI Assistant",
    navPortfolio: "Portfolio Watchlist",
    navRegime: "Regime & Transmission",
    navStock: "Stock Prediction",
    navPolymarket: "Polymarket",
    navOpenrouter: "OpenRouter",
    navAbout: "About Nexo",
    eyebrow: "Quant Signal Engine",
    title: "Stock Prediction Dashboard",
    desc: "Database-driven prediction, backtest, and feature interpretation.",
    ticker: "Ticker",
    tickerPlaceholder: "e.g. PDD",
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
    predChartDesc: "Investor Guide: compare the model forecast (Predicted) with realized return (Actual). If Predicted and Actual move in similar direction for consecutive months, model timing is more reliable; persistent divergence means signal confidence should be reduced.",
    cumChart: "Strategy vs Buy & Hold",
    cumChartDesc: "Investor Guide: this chart shows cumulative performance paths. If Strategy stays above Buy & Hold over time, active signal trading is creating excess return; if it stays below, passive holding may be the better baseline.",
    featChart: "Top Features",
    importance: "Importance",
    history: "Latest 12 Predictions",
    noData: "No data available",
    month: "Month",
    pred: "Predicted",
    actual: "Actual",
    signal: "Signal",
    strat: "Strategy",
    tickerNotFound: "No data for this ticker in database",
    enterTicker: "Enter ticker and click Refresh",
    metricNoData: "Investor Takeaway: no interpretation available because this metric has no valid data.",
    signalBullish: "Investor Takeaway: model bias is bullish. You can consider trend-following entries, but keep stop-loss and position limits.",
    signalBearish: "Investor Takeaway: model bias is bearish. Reduce risk exposure and wait for better entry confirmation.",
    signalNeutral: "Investor Takeaway: no clear directional edge. Keep positions light and focus on risk management.",
  },
  zh: {
    titleMeta: "宏观风险监测 | 股票预测",
    brand: "宏观风险监测",
    navDashboard: "仪表盘",
    navDaily: "每日报告",
    navIndicators: "指标库",
    navGlossary: "术语表",
    navSubscribe: "订阅",
    navAi: "AI 助手",
    navPortfolio: "组合观察池",
    navRegime: "状态与传导",
    navStock: "股票预测",
    navPolymarket: "Polymarket",
    navOpenrouter: "OpenRouter",
    navAbout: "功能介绍",
    eyebrow: "量化信号引擎",
    title: "股票预测仪表盘",
    desc: "基于数据库的预测、回测与特征解释。",
    ticker: "股票代码",
    tickerPlaceholder: "例如：PDD",
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
    predChartDesc: "投资者解读：这张图对比“模型预测”和“实际结果”。若两条线长期同向，说明模型择时稳定；若长期背离，说明信号可靠性下降，需要降低仓位。",
    cumChart: "策略累计收益 vs 买入持有",
    cumChartDesc: "投资者解读：这张图对比两种路径的累计收益。若策略线长期高于买入持有，说明主动信号在创造超额收益；若长期低于，则被动持有可能更优。",
    featChart: "关键驱动因子",
    importance: "重要度",
    history: "最近 12 期预测",
    noData: "暂无可用数据",
    month: "月份",
    pred: "预测",
    actual: "实际",
    signal: "信号",
    strat: "策略收益",
    tickerNotFound: "数据库中暂无此 ticker 数据",
    enterTicker: "输入 ticker 后点击刷新",
    metricNoData: "投资者解读：该指标暂无有效数据，暂时无法给出解读。",
    signalBullish: "投资者解读：当前模型偏多，可考虑顺势参与，但要设置止损并控制仓位。",
    signalBearish: "投资者解读：当前模型偏空，建议先降风险敞口，等待更明确的买点确认。",
    signalNeutral: "投资者解读：当前缺乏明确方向优势，建议轻仓和严格风控。",
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
  // Auth-required APIs must stay same-origin to keep public session cookies.
  if (p.startsWith("/api/")) return p;
  return `${STOCK_API_BASE}${p}`;
}

async function apiGet(path){
  const r = await fetch(api(path), { credentials: "include", cache: "no-store" });
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
  document.title = st("titleMeta");
  setText("stock-brand", st("brand"));
  setText("stock-nav-dashboard", st("navDashboard"));
  setText("stock-nav-daily", st("navDaily"));
  setText("stock-nav-indicators", st("navIndicators"));
  setText("stock-nav-glossary", st("navGlossary"));
  setText("stock-nav-subscribe", st("navSubscribe"));
  setText("stock-nav-ai", st("navAi"));
  setText("stock-nav-portfolio", st("navPortfolio"));
  setText("stock-nav-regime", st("navRegime"));
  setText("stock-nav-stock", st("navStock"));
  setText("stock-nav-polymarket", st("navPolymarket"));
  setText("stock-nav-openrouter", st("navOpenrouter"));
  setText("stock-nav-about", st("navAbout"));
  setText("stock-eyebrow", st("eyebrow"));
  setText("stock-page-title", st("title"));
  setText("stock-page-desc", st("desc"));
  setText("stock-ticker-label", st("ticker"));
  const input = document.getElementById("stock-ticker-input");
  if (input) input.setAttribute("placeholder", st("tickerPlaceholder"));
  setText("stock-refresh", st("refresh"));
  setText("stock-latest-signal-label", st("latestSignal"));
  setText("kpi_pred_label", st("predReturn"));
  setText("kpi_up_label", st("upProb"));
  setText("kpi_acc_label", st("acc"));
  setText("kpi_scagr_label", st("scagr"));
  setText("kpi_bcagr_label", st("bcagr"));
  setText("kpi_sharpe_label", st("sharpe"));
  setText("chart_pred_title", st("predChart"));
  setText("chart_pred_desc", st("predChartDesc"));
  setText("chart_cum_title", st("cumChart"));
  setText("chart_cum_desc", st("cumChartDesc"));
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
  if (n >= 0.05) return sLang() === "zh" ? "投资者解读：预期收益较高，说明上行空间相对充足；可考虑进攻型仓位，但要防波动回撤。" : "Investor Takeaway: expected return is strong, suggesting meaningful upside; consider offensive positioning with drawdown controls.";
  if (n >= 0) return sLang() === "zh" ? "投资者解读：预期小幅正收益，偏多但优势不大，适合分批建仓而非一次性重仓。" : "Investor Takeaway: expected return is modestly positive; prefer staged entries over heavy one-shot allocation.";
  return sLang() === "zh" ? "投资者解读：预期为负，短线下行压力更大，优先控制仓位和回撤风险。" : "Investor Takeaway: expected return is negative; prioritize downside protection and smaller exposure.";
}

function explainUpProb(v){
  const n = Number(v);
  if (!Number.isFinite(n)) return st("metricNoData");
  if (n >= 0.65) return sLang() === "zh" ? "投资者解读：上涨胜率较高，说明多头方向更占优，可适度提高风险预算。" : "Investor Takeaway: upside win-rate is high; bullish side has edge and can justify a slightly higher risk budget.";
  if (n >= 0.5) return sLang() === "zh" ? "投资者解读：上涨概率略占优，信号偏多但并不强，建议控制交易节奏。" : "Investor Takeaway: upside probability is only mildly favorable; keep entries disciplined.";
  return sLang() === "zh" ? "投资者解读：上涨概率低于 50%，说明空头风险偏高，宜降低激进仓位。" : "Investor Takeaway: upside probability is below 50%; bearish risk is elevated, so reduce aggressive exposure.";
}

function explainAccuracy(v){
  const n = Number(v);
  if (!Number.isFinite(n)) return st("metricNoData");
  if (n >= 0.6) return sLang() === "zh" ? "投资者解读：历史判断稳定性较好，可作为决策中的核心参考之一。" : "Investor Takeaway: historical signal quality is solid and can be a core input in decision-making.";
  if (n >= 0.52) return sLang() === "zh" ? "投资者解读：准确率仅小幅领先随机，建议与基本面或技术面信号结合使用。" : "Investor Takeaway: edge over random is limited; combine with fundamental or technical confirmation.";
  return sLang() === "zh" ? "投资者解读：历史准确率偏弱，单独依赖该模型的风险较高。" : "Investor Takeaway: historical accuracy is weak; relying on this model alone carries higher risk.";
}

function explainStrategyCagr(v){
  const n = Number(v);
  if (!Number.isFinite(n)) return st("metricNoData");
  if (n > 0) return sLang() === "zh" ? "投资者解读：策略年化复合回报为正，长期执行的收益潜力更好。" : "Investor Takeaway: positive strategy CAGR suggests better long-term compounding potential.";
  return sLang() === "zh" ? "投资者解读：策略年化复合回报为负，说明现有信号体系需要优化后再加大使用。" : "Investor Takeaway: negative strategy CAGR indicates the signal framework needs improvement before scaling.";
}

function explainBuyHoldCagr(v){
  const n = Number(v);
  if (!Number.isFinite(n)) return st("metricNoData");
  if (n > 0) return sLang() === "zh" ? "投资者解读：标的长期自然上涨趋势较好，长期持有本身就有回报基础。" : "Investor Takeaway: the asset’s own long-term trend is constructive, so passive holding has a return base.";
  return sLang() === "zh" ? "投资者解读：标的长期自然趋势偏弱，更依赖择时和风控来获取超额收益。" : "Investor Takeaway: long-term baseline trend is weak, so timing and risk control matter more.";
}

function explainSharpe(v){
  const n = Number(v);
  if (!Number.isFinite(n)) return st("metricNoData");
  if (n >= 1) return sLang() === "zh" ? "投资者解读：风险回报效率较高，说明承担同样波动能换来更好的收益质量。" : "Investor Takeaway: risk-adjusted return is strong; each unit of volatility is compensated better.";
  if (n >= 0.5) return sLang() === "zh" ? "投资者解读：风险回报效率中等，可用但需严格控制回撤和仓位。" : "Investor Takeaway: risk-adjusted return is moderate; usable with strict drawdown and position control.";
  return sLang() === "zh" ? "投资者解读：风险回报效率较低，当前阶段更适合保守配置。" : "Investor Takeaway: risk-adjusted return is weak; a conservative stance is preferable.";
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
    data: { labels, datasets: [{ label: st("strat"), data: cumS, borderColor: "#0b8d73" }, { label: st("bcagr"), data: cumB, borderColor: "#425a67" }] },
    options: { responsive: true, maintainAspectRatio: false },
  });

  const fs = featureRows || [];
  chartFeat = new Chart(document.getElementById("stock-chart-feat"), {
    type: "bar",
    data: {
      labels: fs.map((x) => x.feature_name),
      datasets: [{ label: st("importance"), data: fs.map((x) => Number(x.importance || 0)), backgroundColor: "#0b8d73" }],
    },
    options: { indexAxis: "y", responsive: true, maintainAspectRatio: false },
  });
}

async function loadTickers(){
  const res = await apiGet("/api/stocks/tickers");
  const rows = Array.isArray(res?.tickers) ? res.tickers : [];
  const list = rows
    .map((x) => (typeof x === "string" ? x : (x?.ticker || x?.symbol || "")))
    .map((x) => normalizeTicker(x))
    .filter(Boolean);
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
