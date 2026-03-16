const CW_API_BASE = (document.querySelector('meta[name="macro-api-base"]')?.content || "").trim() || "https://api.nexo.hk";
const CW_LANG_KEY = "macro-monitor-lang";

const CW_I18N = {
  en: {
    code: "Code",
    confidence: "Confidence",
    score_delta: "Score",
    delta: "Delta",
    conflict: "Conflict",
    supply: "Supply",
    summary: "Summary",
    favored_styles: "Favored styles",
    avoided_styles: "Avoided styles",
    positions: "Positions",
    top_risk: "Top risk",
    top_benefit: "Top benefit",
    active_market_alerts: "Active market alerts",
    no_regime_snapshot: "No regime snapshot.",
    no_overlay_snapshot: "No overlay snapshot.",
    no_action_bias_snapshot: "No action bias snapshot.",
    no_transmission_snapshot: "No transmission snapshot.",
    rates: "Rates",
    equities: "Equities",
    credit: "Credit",
    usd: "USD",
    commodities: "Commodities",
    crypto: "Crypto",
    growth: "Growth",
    inflation: "Inflation",
    oil: "Oil",
    geopolitics: "Geopolitics",
    volatility: "Volatility",
    favored_sectors: "Favored sectors",
    avoided_sectors: "Avoided sectors",
    inflation_impact: "Inflation impact",
    risk_asset_impact: "Risk asset impact",
    action_bias: "Action Bias",
    macro_risk_score: "Macro Risk Score",
    no_macro_exposure_data: "No macro exposure data.",
    no_macro_signal_data: "No macro signal data.",
    enter_email: "Enter email to view risk summary.",
    no_summary: "No summary.",
    create_or_select_watchlist: "Create or select a watchlist first.",
    no_positions_yet: "No positions yet.",
    no_watchlists: "No watchlists.",
    ticker: "Ticker",
    qty: "Qty",
    action: "Action",
    date: "Date",
    regime: "Regime",
    no_history: "No history.",
    macro_regime_growth_slowdown_credit_stable: "Growth Slowdown / Credit Stable",
    macro_regime_liquidity_repair_growth_stable: "Liquidity Repair / Growth Stable",
    macro_regime_inflation_reacceleration_energy_shock: "Inflation Reacceleration / Energy Shock",
    macro_regime_credit_stress_risk_off: "Credit Stress / Risk Off",
    macro_regime_recession_policy_easing: "Recession / Policy Easing",
    bias_avoid_new_adds: "Avoid New Adds",
    bias_hold: "Hold",
    bias_watch_to_add: "Watch to Add",
    bias_reduce: "Reduce",
    bias_increase: "Increase",
    signal_risk_off: "Risk Off",
    signal_neutral: "Neutral",
    signal_favorable: "Favorable",
    level_low: "Low",
    level_medium: "Medium",
    level_high: "High",
    level_elevated: "Elevated",
    trans_bull_steepening: "Bull Steepening",
    trans_defensive_duration: "Defensive Duration",
    trans_selective: "Selective",
    trans_tightening: "Tightening",
    trans_stress: "Stress",
    trans_stable: "Stable",
    trans_firm: "Firm",
    trans_inflation_hedge: "Inflation Hedge",
    trans_fragile: "Fragile",
    trans_beta_positive: "Beta Positive",
    trans_neutral: "Neutral",
    explain_key_indicators: "Key indicators",
    explain_how_to_read: "How to read",
    explain_investor_takeaway: "Investor takeaway",
    investor_scenario: "Macro scenario",
    investor_risk: "Main risk",
    investor_positioning: "Suggested positioning",
    investor_watch: "Watch list"
  },
  zh: {
    code: "代码",
    confidence: "置信度",
    score_delta: "得分",
    delta: "变化",
    conflict: "冲突等级",
    supply: "供应冲击",
    summary: "摘要",
    favored_styles: "偏好风格",
    avoided_styles: "回避风格",
    positions: "持仓数",
    top_risk: "最高风险",
    top_benefit: "潜在受益",
    active_market_alerts: "当前市场预警",
    no_regime_snapshot: "暂无状态快照。",
    no_overlay_snapshot: "暂无地缘叠加快照。",
    no_action_bias_snapshot: "暂无动作偏向快照。",
    no_transmission_snapshot: "暂无传导快照。",
    rates: "利率",
    equities: "权益",
    credit: "信用",
    usd: "美元",
    commodities: "大宗商品",
    crypto: "加密资产",
    growth: "增长",
    inflation: "通胀",
    oil: "原油",
    geopolitics: "地缘政治",
    volatility: "波动率",
    favored_sectors: "偏好板块",
    avoided_sectors: "回避板块",
    inflation_impact: "通胀影响",
    risk_asset_impact: "风险资产影响",
    action_bias: "动作偏向",
    macro_risk_score: "宏观风险分",
    no_macro_exposure_data: "暂无宏观暴露数据。",
    no_macro_signal_data: "暂无宏观信号数据。",
    enter_email: "请输入邮箱以查看风险摘要。",
    no_summary: "暂无摘要。",
    create_or_select_watchlist: "请先创建或选择一个观察池。",
    no_positions_yet: "暂无持仓。",
    no_watchlists: "暂无观察池。",
    ticker: "代码",
    qty: "数量",
    action: "动作",
    date: "日期",
    regime: "状态",
    no_history: "暂无历史。",
    macro_regime_growth_slowdown_credit_stable: "增长放缓 / 信用稳定",
    macro_regime_liquidity_repair_growth_stable: "流动性修复 / 增长稳定",
    macro_regime_inflation_reacceleration_energy_shock: "通胀再加速 / 能源冲击",
    macro_regime_credit_stress_risk_off: "信用承压 / 风险回避",
    macro_regime_recession_policy_easing: "衰退 / 政策宽松",
    bias_avoid_new_adds: "暂不新增",
    bias_hold: "持有",
    bias_watch_to_add: "观察后加仓",
    bias_reduce: "减仓",
    bias_increase: "增持",
    signal_risk_off: "风险回避",
    signal_neutral: "中性",
    signal_favorable: "偏正面",
    level_low: "低",
    level_medium: "中等",
    level_high: "高",
    level_elevated: "偏高",
    trans_bull_steepening: "牛陡",
    trans_defensive_duration: "防御久期",
    trans_selective: "选择性",
    trans_tightening: "收紧",
    trans_stress: "承压",
    trans_stable: "稳定",
    trans_firm: "偏强",
    trans_inflation_hedge: "抗通胀",
    trans_fragile: "脆弱",
    trans_beta_positive: "高贝塔偏正面",
    trans_neutral: "中性",
    explain_key_indicators: "关键指标",
    explain_how_to_read: "解读方式",
    explain_investor_takeaway: "投资含义",
    investor_scenario: "宏观情景",
    investor_risk: "主要风险",
    investor_positioning: "建议仓位",
    investor_watch: "重点观察"
  }
};

function cwLang() {
  try {
    return localStorage.getItem(CW_LANG_KEY) === "zh" ? "zh" : "en";
  } catch {
    return "en";
  }
}

function cwT(key) {
  const lang = cwLang();
  return CW_I18N[lang]?.[key] || CW_I18N.en[key] || "";
}

function cwHumanize(text) {
  const raw = String(text ?? "").trim();
  if (!raw) return "--";
  if (/[\u4e00-\u9fff]/.test(raw)) return raw;
  const spaced = raw.replaceAll("_", " ").replaceAll("-", " ").replace(/\s+/g, " ").trim();
  return spaced.replace(/\b\w/g, (m) => m.toUpperCase());
}

function cwMapValue(value) {
  const text = String(value ?? "").trim();
  if (!text) return "--";
  const map = {
    growth_slowdown_credit_stable: "macro_regime_growth_slowdown_credit_stable",
    liquidity_repair_growth_stable: "macro_regime_liquidity_repair_growth_stable",
    inflation_reacceleration_energy_shock: "macro_regime_inflation_reacceleration_energy_shock",
    credit_stress_risk_off: "macro_regime_credit_stress_risk_off",
    recession_policy_easing: "macro_regime_recession_policy_easing",
    avoid_new_adds: "bias_avoid_new_adds",
    hold: "bias_hold",
    watch_to_add: "bias_watch_to_add",
    reduce: "bias_reduce",
    increase: "bias_increase",
    risk_off: "signal_risk_off",
    neutral: "signal_neutral",
    favorable: "signal_favorable",
    low: "level_low",
    medium: "level_medium",
    high: "level_high",
    elevated: "level_elevated",
    bull_steepening: "trans_bull_steepening",
    defensive_duration: "trans_defensive_duration",
    selective: "trans_selective",
    tightening: "trans_tightening",
    stress: "trans_stress",
    stable: "trans_stable",
    firm: "trans_firm",
    inflation_hedge: "trans_inflation_hedge",
    fragile: "trans_fragile",
    beta_positive: "trans_beta_positive",
    neutral_value: "trans_neutral"
  };
  const textMapZh = {
    "energy hedge": "能源对冲",
    quality: "高质量",
    "consumer cyclicals": "可选消费周期",
    "platform tech": "平台科技",
    "select cyclicals": "精选周期板块",
    "rate sensitive laggards": "利率敏感落后板块",
    "growth slowing but credit not yet broken": "增长正在放缓，但信用尚未失稳",
    "watch labor and credit spread path": "关注就业与信用利差走势",
    "local supply disruption and oil pressure need monitoring.": "需关注局部供应扰动与油价压力",
    "energy shock scenario argues against new risk additions.": "能源冲击情景下不宜新增风险敞口"
  };
  const key = map[text] || map[text.toLowerCase()] || "";
  if (key) return cwT(key);
  if (cwLang() === "zh") {
    const zh = textMapZh[text.toLowerCase()];
    if (zh) return zh;
  }
  return cwHumanize(text);
}

function cwApi(path) {
  const normalized = path.startsWith("/") ? path : `/${path}`;
  // Keep auth-bound APIs on same origin so session cookies always apply.
  if (normalized.startsWith("/api/")) return normalized;
  return `${CW_API_BASE}${normalized}`;
}

async function cwGet(path) {
  try {
    const res = await fetch(cwApi(path), { credentials: "include" });
    if (!res.ok) return null;
    return await res.json();
  } catch {
    return null;
  }
}

function cwText(el, html) {
  if (el) el.innerHTML = html;
}

function cwEsc(v) {
  return String(v ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}

function cwList(items) {
  if (!items?.length) return "<p class='subtle'>--</p>";
  return `<ul class="preview-list">${items.map((x) => `<li>${cwEsc(cwMapValue(x))}</li>`).join("")}</ul>`;
}

function cwObjTable(obj) {
  if (!obj || typeof obj !== "object") return "<p class='subtle'>--</p>";
  const rows = Object.entries(obj);
  const keyMap = {
    rates_bias: "rates",
    equities_bias: "equities",
    credit_bias: "credit",
    usd_bias: "usd",
    commodities_bias: "commodities",
    crypto_bias: "crypto",
    favored_sectors: "favored_sectors",
    avoided_sectors: "avoided_sectors",
    rates: "rates",
    equities: "equities",
    credit: "credit",
    usd: "usd",
    commodities: "commodities",
    crypto: "crypto",
    favored: "favored_sectors",
    avoided: "avoided_sectors",
    interest_rate: "rates",
    growth: "growth",
    inflation: "inflation",
    oil: "oil",
    geopolitics: "geopolitics",
    volatility: "volatility"
  };
  function formatCellValue(v) {
    if (Array.isArray(v)) return v.map((item) => cwMapValue(item)).join(", ");
    if (typeof v === "object" && v) return JSON.stringify(v);
    return cwMapValue(v);
  }
  return `<table class="data-table"><tbody>${rows.map(([k, v]) => `<tr><th>${cwEsc(cwT(keyMap[k] || k))}</th><td>${cwEsc(formatCellValue(v))}</td></tr>`).join("")}</tbody></table>`;
}

function cwSentences(items) {
  if (!items?.length) return "<p class='subtle'>--</p>";
  return items.map((x) => `<p class="summary-line">${cwEsc(x)}</p>`).join("");
}

function cwExplainBlock({ indicators = [], how = [], takeaway = [] }) {
  const i = (indicators || []).map((x) => cwMapValue(x)).join(" / ") || "--";
  const h = how?.length ? how : ["--"];
  const t = takeaway?.length ? takeaway : ["--"];
  return `
    <div class="summary-line"><strong>${cwEsc(cwT("explain_key_indicators"))}:</strong> ${cwEsc(i)}</div>
    <div class="summary-line"><strong>${cwEsc(cwT("explain_how_to_read"))}:</strong></div>
    ${cwSentences(h)}
    <div class="summary-line"><strong>${cwEsc(cwT("explain_investor_takeaway"))}:</strong></div>
    ${cwSentences(t)}
  `;
}

function buildRegimeExplain(regime, hist) {
  const zh = cwLang() === "zh";
  const score = Number(regime?.score || 0);
  const delta = Number(regime?.score_delta || 0);
  const trend = hist?.length >= 2 ? Number(hist[0]?.score || 0) - Number(hist[Math.min(6, hist.length - 1)]?.score || 0) : 0;
  const trendText = trend > 1 ? (zh ? "近一周分数上行" : "score rose over the last week") : trend < -1 ? (zh ? "近一周分数下行" : "score fell over the last week") : (zh ? "近一周分数大体持平" : "score stayed roughly flat over the last week");
  return cwExplainBlock({
    indicators: ["regime_code", "confidence", "score", "delta"],
    how: zh
      ? [`当前总分 ${score.toFixed(2)}，日变化 ${delta.toFixed(2)}。`, `${trendText}，需要与历史表中的斜率一起看。`]
      : [`Current score is ${score.toFixed(2)} with daily delta ${delta.toFixed(2)}.`, `${trendText}; read it with the history slope.`],
    takeaway: zh
      ? ["分数上行可逐步增加风险预算；分数回落时先降杠杆、再降高波动敞口。"]
      : ["When score trends up, increase risk budget gradually; when it drops, reduce leverage and high-volatility exposure first."]
  });
}

function buildOverlayExplain(overlay) {
  const zh = cwLang() === "zh";
  const conflict = cwMapValue(overlay?.conflict_level || "--");
  const supply = cwMapValue(overlay?.supply_disruption_level || "--");
  return cwExplainBlock({
    indicators: ["conflict", "supply", "summary"],
    how: zh
      ? [`冲突等级 ${conflict}，供应冲击 ${supply}。`, "两者同时抬升时，通常先影响油价，再传导到通胀预期和风险资产波动。"]
      : [`Conflict level is ${conflict}, supply disruption is ${supply}.`, "When both rise together, oil usually moves first, then inflation expectations and risk-asset volatility follow."],
    takeaway: zh
      ? ["地缘风险抬升期，优先控制单一高贝塔行业集中度，并提高对冲比率。"]
      : ["During geopolitical stress, reduce concentration in single high-beta sectors and raise hedge ratio."]
  });
}

function buildActionExplain(bias) {
  const zh = cwLang() === "zh";
  const action = cwMapValue(bias?.overall_bias || "--");
  const favored = (bias?.favored_styles_json || []).map((x) => cwMapValue(x)).join(", ") || "--";
  const avoided = (bias?.avoided_styles_json || []).map((x) => cwMapValue(x)).join(", ") || "--";
  return cwExplainBlock({
    indicators: ["action_bias", "favored_styles", "avoided_styles"],
    how: zh
      ? [`当前动作偏向为“${action}”。`, `偏好风格：${favored}；回避风格：${avoided}。`]
      : [`Current action bias is "${action}".`, `Favored styles: ${favored}; avoided styles: ${avoided}.`],
    takeaway: zh
      ? ["把动作偏向当作仓位节奏器：先调仓位速度，再调整方向。"]
      : ["Use action bias as a position-timing tool: adjust speed of risk first, then direction."]
  });
}

function buildTransmissionExplain(trans) {
  const zh = cwLang() === "zh";
  return cwExplainBlock({
    indicators: ["rates", "equities", "credit", "usd", "commodities", "crypto"],
    how: zh
      ? [
          `利率=${cwMapValue(trans?.rates_bias)}, 权益=${cwMapValue(trans?.equities_bias)}, 信用=${cwMapValue(trans?.credit_bias)}。`,
          "传导热力表用于判断“风险如何扩散”：先看利率与信用，再看权益和商品确认。"
        ]
      : [
          `Rates=${cwMapValue(trans?.rates_bias)}, Equities=${cwMapValue(trans?.equities_bias)}, Credit=${cwMapValue(trans?.credit_bias)}.`,
          "Use the heatmap to track how risk propagates: read rates and credit first, then confirm with equities and commodities."
        ],
    takeaway: zh
      ? ["若信用与利率同时走弱，建议降低组合净风险暴露并缩短复盘周期。"]
      : ["If credit and rates weaken together, reduce net portfolio risk and shorten review cycle."]
  });
}

function buildInvestorBrief(regime, overlay, bias, trans, hist) {
  const zh = cwLang() === "zh";
  const regimeLabel = cwMapValue(regime?.regime_code || regime?.regime_label || "--");
  const action = cwMapValue(bias?.overall_bias || "--");
  const trend = hist?.length >= 2 ? Number(hist[0]?.score || 0) - Number(hist[Math.min(6, hist.length - 1)]?.score || 0) : 0;
  const trendView = trend > 1 ? (zh ? "边际改善" : "improving at the margin") : trend < -1 ? (zh ? "边际走弱" : "softening at the margin") : (zh ? "总体平稳" : "broadly stable");
  const risk = zh
    ? `地缘冲突=${cwMapValue(overlay?.conflict_level || "--")}，供应冲击=${cwMapValue(overlay?.supply_disruption_level || "--")}。`
    : `Geopolitical conflict=${cwMapValue(overlay?.conflict_level || "--")}, supply shock=${cwMapValue(overlay?.supply_disruption_level || "--")}.`;
  const positioning = zh
    ? `当前建议“${action}”，并参考传导：利率=${cwMapValue(trans?.rates_bias || "--")}、信用=${cwMapValue(trans?.credit_bias || "--")}。`
    : `Current stance is "${action}" with transmission check: rates=${cwMapValue(trans?.rates_bias || "--")}, credit=${cwMapValue(trans?.credit_bias || "--")}.`;
  const watch = zh
    ? "若未来两天信用与权益同时恶化，应优先减高波动仓位并提高现金/对冲比例。"
    : "If credit and equities deteriorate together over the next 2 days, cut high-volatility positions first and increase cash/hedges.";
  return `
    <div class="summary-line"><strong>${cwEsc(cwT("investor_scenario"))}:</strong> ${cwEsc(`${regimeLabel} · ${trendView}`)}</div>
    <div class="summary-line"><strong>${cwEsc(cwT("investor_risk"))}:</strong> ${cwEsc(risk)}</div>
    <div class="summary-line"><strong>${cwEsc(cwT("investor_positioning"))}:</strong> ${cwEsc(positioning)}</div>
    <div class="summary-line"><strong>${cwEsc(cwT("investor_watch"))}:</strong> ${cwEsc(watch)}</div>
  `;
}

async function renderDashboardCapitalWarning() {
  const [
    regimeRes,
    overlayRes,
    transRes,
    biasRes,
    alertsRes,
    summaryRes,
  ] = await Promise.all([
    cwGet("/api/regime/latest"),
    cwGet("/api/geopolitical-overlay/latest"),
    cwGet("/api/transmission/latest"),
    cwGet("/api/action-bias/latest"),
    cwGet("/api/alerts/latest"),
    cwGet("/api/portfolio/risk-summary?user_email=xiayiping@gmail.com"),
  ]);
  const regime = regimeRes?.item;
  const overlay = overlayRes?.item;
  const trans = transRes?.item;
  const bias = biasRes?.item;
  const alerts = alertsRes?.items || [];
  const portfolio = summaryRes?.summary;

  cwText(document.getElementById("regime-engine-card"),
    regime ? `
      <div class="summary-score">${cwEsc(cwMapValue(regime.regime_code) || regime.regime_label)}</div>
      <div class="summary-line">${cwT("code")}: ${cwEsc(regime.regime_code)}</div>
      <div class="summary-line">${cwT("confidence")}: ${cwEsc(regime.confidence)}</div>
      <div class="summary-line">${cwT("score_delta")}: ${cwEsc(regime.score)} / ${cwT("delta")}: ${cwEsc(regime.score_delta)}</div>
      ${cwList(regime.drivers_json || [])}
    ` : `<p class='subtle'>${cwEsc(cwT("no_regime_snapshot"))}</p>`
  );
  cwText(document.getElementById("geopolitical-overlay-card"),
    overlay ? `
      <div class="summary-score">${cwEsc(overlay.oil_shock_scenario || "--")}</div>
      <div class="summary-line">${cwT("conflict")}: ${cwEsc(cwMapValue(overlay.conflict_level || "--"))}</div>
      <div class="summary-line">${cwT("supply")}: ${cwEsc(cwMapValue(overlay.supply_disruption_level || "--"))}</div>
      <div class="summary-line">${cwT("summary")}: ${cwEsc(cwMapValue(overlay.summary || "--"))}</div>
    ` : `<p class='subtle'>${cwEsc(cwT("no_overlay_snapshot"))}</p>`
  );
  cwText(document.getElementById("action-bias-card"),
    bias ? `
      <div class="summary-score">${cwEsc(cwMapValue(bias.overall_bias || "--"))}</div>
      <div class="summary-line">${cwT("favored_styles")}: ${cwEsc((bias.favored_styles_json || []).map((x) => cwMapValue(x)).join(", "))}</div>
      <div class="summary-line">${cwT("avoided_styles")}: ${cwEsc((bias.avoided_styles_json || []).map((x) => cwMapValue(x)).join(", "))}</div>
      <div class="summary-line">${cwEsc(cwMapValue(bias.summary || "--"))}</div>
    ` : `<p class='subtle'>${cwEsc(cwT("no_action_bias_snapshot"))}</p>`
  );
  cwText(document.getElementById("portfolio-macro-risk-card"),
    portfolio ? `
      <div class="summary-score">${cwEsc(portfolio.average_macro_risk_score || 0)}</div>
      <div class="summary-line">${cwT("positions")}: ${cwEsc(portfolio.count || 0)}</div>
      <div class="summary-line">${cwT("top_risk")}: ${cwEsc((portfolio.top_risk_positions || []).map((x) => x.ticker).join(", ") || "--")}</div>
      <div class="summary-line">${cwT("top_benefit")}: ${cwEsc((portfolio.top_benefit_positions || []).map((x) => x.ticker).join(", ") || "--")}</div>
    ` : `
      <div class="summary-score">${alerts.length}</div>
      <div class="summary-line">${cwT("active_market_alerts")}</div>
      <div class="summary-line">${cwEsc(alerts.slice(0, 5).map((x) => x.alert_code).join(", "))}</div>
    `
  );
  cwText(document.getElementById("transmission-heatmap"),
    trans ? cwObjTable({
      rates_bias: trans.rates_bias,
      equities_bias: trans.equities_bias,
      credit_bias: trans.credit_bias,
      usd_bias: trans.usd_bias,
      commodities_bias: trans.commodities_bias,
      crypto_bias: trans.crypto_bias,
      favored_sectors: (trans.sectors_json || {}).favored || [],
      avoided_sectors: (trans.sectors_json || {}).avoided || [],
    }) : `<p class='subtle'>${cwEsc(cwT("no_transmission_snapshot"))}</p>`
  );
}

async function renderDailyCapitalWarning() {
  const params = new URLSearchParams(location.search);
  const reportDate = params.get("date") || new Date().toISOString().slice(0, 10);
  const [regimeRes, impactRes, overlayRes, transRes, biasRes, watchRes] = await Promise.all([
    cwGet(`/api/reports/${reportDate}/regime`),
    cwGet(`/api/reports/${reportDate}/portfolio-impact?user_email=xiayiping@gmail.com`),
    cwGet("/api/geopolitical-overlay/latest"),
    cwGet("/api/transmission/latest"),
    cwGet("/api/action-bias/latest"),
    cwGet("/api/portfolio/risk-summary?user_email=xiayiping@gmail.com"),
  ]);
  const regime = regimeRes?.item;
  const impact = impactRes;
  const overlay = overlayRes?.item;
  const trans = transRes?.item;
  const bias = biasRes?.item;
  const watch = watchRes?.summary;
  cwText(document.getElementById("report-regime-block"),
    regime ? `
      <div class="summary-score">${cwEsc(cwMapValue(regime.regime_code) || regime.regime_label)}</div>
      <div class="summary-line">${cwT("confidence")}: ${cwEsc(regime.confidence)}</div>
      ${cwList(regime.drivers_json || [])}
    ` : "<p class='subtle'>--</p>");
  cwText(document.getElementById("report-transmission-block"),
    trans ? `
      <div class="summary-line">${cwT("rates")}: ${cwEsc(cwMapValue(trans.rates_bias))}</div>
      <div class="summary-line">${cwT("equities")}: ${cwEsc(cwMapValue(trans.equities_bias))}</div>
      <div class="summary-line">${cwT("credit")}: ${cwEsc(cwMapValue(trans.credit_bias))}</div>
      <div class="summary-line">${cwT("usd")}: ${cwEsc(cwMapValue(trans.usd_bias))}</div>
    ` : "<p class='subtle'>--</p>");
  cwText(document.getElementById("report-portfolio-impact-block"),
    impact ? `
      <div class="summary-score">${cwEsc(impact.average_macro_risk_score || 0)}</div>
      <div class="summary-line">${cwEsc(impact.summary_text || "--")}</div>
    ` : "<p class='subtle'>--</p>");
  cwText(document.getElementById("report-action-bias-block"),
    bias ? `
      <div class="summary-score">${cwEsc(cwMapValue(bias.overall_bias))}</div>
      <div class="summary-line">${cwT("favored_sectors")}: ${cwEsc((bias.favored_sectors_json || []).map((x) => cwMapValue(x)).join(", "))}</div>
      <div class="summary-line">${cwT("avoided_sectors")}: ${cwEsc((bias.avoided_sectors_json || []).map((x) => cwMapValue(x)).join(", "))}</div>
      <div class="summary-line">${cwEsc(cwMapValue(bias.summary || "--"))}</div>
    ` : "<p class='subtle'>--</p>");
  cwText(document.getElementById("report-overlay-block"),
    overlay ? `
      <div class="summary-score">${cwEsc(overlay.oil_shock_scenario)}</div>
      <div class="summary-line">${cwT("inflation_impact")}: ${cwEsc(overlay.inflation_impact)}</div>
      <div class="summary-line">${cwT("risk_asset_impact")}: ${cwEsc(overlay.risk_asset_impact)}</div>
      <div class="summary-line">${cwEsc(cwMapValue(overlay.summary || "--"))}</div>
    ` : "<p class='subtle'>--</p>");
  cwText(document.getElementById("report-watchlist-block"),
    watch ? `
      <div class="summary-line">${cwT("top_risk")}: ${cwEsc((watch.top_risk_positions || []).map((x) => x.ticker).join(", ") || "--")}</div>
      <div class="summary-line">${cwT("top_benefit")}: ${cwEsc((watch.top_benefit_positions || []).map((x) => x.ticker).join(", ") || "--")}</div>
    ` : "<p class='subtle'>--</p>");
}

async function renderStockCapitalWarning() {
  const input = document.getElementById("stock-ticker-input");
  const ticker = String(input?.value || "PDD").trim().toUpperCase();
  const [exposureRes, signalRes] = await Promise.all([
    cwGet(`/api/stocks/${encodeURIComponent(ticker)}/macro-exposure`),
    cwGet(`/api/stocks/${encodeURIComponent(ticker)}/macro-signal/latest`),
  ]);
  cwText(document.getElementById("stock-macro-exposure"), exposureRes?.item ? cwObjTable({
    interest_rate: exposureRes.item.interest_rate_sensitivity,
    growth: exposureRes.item.growth_sensitivity,
    inflation: exposureRes.item.inflation_sensitivity,
    oil: exposureRes.item.oil_sensitivity,
    credit: exposureRes.item.credit_sensitivity,
    usd: exposureRes.item.usd_sensitivity,
    geopolitics: exposureRes.item.geopolitics_sensitivity,
    volatility: exposureRes.item.volatility_sensitivity,
  }) : `<p class='subtle'>${cwEsc(cwT("no_macro_exposure_data"))}</p>`);
  cwText(document.getElementById("stock-macro-signal"), signalRes?.item ? `
    <div class="summary-score">${cwEsc(cwMapValue(signalRes.item.signal || "--"))}</div>
    <div class="summary-line">${cwT("action_bias")}: ${cwEsc(cwMapValue(signalRes.item.action_bias || "--"))}</div>
    <div class="summary-line">${cwT("macro_risk_score")}: ${cwEsc(signalRes.item.macro_risk_score || "--")}</div>
    <div class="summary-line">${cwEsc(cwMapValue(signalRes.item.explanation_short || "--"))}</div>
  ` : `<p class='subtle'>${cwEsc(cwT("no_macro_signal_data"))}</p>`);
}

async function initPortfolioWatchlistPage() {
  const listRoot = document.getElementById("watchlist-list");
  const posRoot = document.getElementById("watchlist-positions");
  const riskRoot = document.getElementById("portfolio-risk-summary");
  const emailInput = document.getElementById("watchlist-email");
  const emailLabel = document.getElementById("watchlist-user-email");
  const nameInput = document.getElementById("watchlist-name");
  const tickerInput = document.getElementById("position-ticker");
  const qtyInput = document.getElementById("position-qty");
  const tickerDatalist = document.getElementById("ticker-suggestions");
  let currentWatchlistId = null;
  let currentUserEmail = "";

  async function loadCurrentUser() {
    const me = await cwGet("/api/auth/me");
    currentUserEmail = String(me?.user?.email || "").trim().toLowerCase();
    if (emailInput) emailInput.value = currentUserEmail;
    if (emailLabel) {
      emailLabel.textContent = currentUserEmail
        ? (cwLang() === "zh" ? `当前用户：${currentUserEmail}` : `Current user: ${currentUserEmail}`)
        : (cwLang() === "zh" ? "当前用户未登录" : "No signed-in user");
    }
  }

  async function loadTickerSuggestions() {
    const data = await cwGet("/api/stocks/tickers");
    const rows = data?.items || [];
    if (!tickerDatalist) return;
    tickerDatalist.innerHTML = "";
    rows.forEach((x) => {
      const t = String(x || "").trim().toUpperCase();
      if (!t) return;
      const op = document.createElement("option");
      op.value = t;
      tickerDatalist.appendChild(op);
    });
  }

  async function refreshSummary() {
    const email = currentUserEmail;
    if (!email) {
      cwText(riskRoot, `<p class='subtle'>${cwEsc(cwT("enter_email"))}</p>`);
      return;
    }
    const q = currentWatchlistId ? `?watchlist_id=${encodeURIComponent(currentWatchlistId)}` : "";
    const res = await cwGet(`/api/portfolio/risk-summary${q}`);
    const s = res?.summary;
    cwText(riskRoot, s ? `
      <div class="summary-score">${cwEsc(s.average_macro_risk_score || 0)}</div>
      <div class="summary-line">${cwT("positions")}: ${cwEsc(s.count || 0)}</div>
      <div class="summary-line">${cwT("top_risk")}: ${cwEsc((s.top_risk_positions || []).map((x) => x.ticker).join(", ") || "--")}</div>
      <div class="summary-line">${cwT("top_benefit")}: ${cwEsc((s.top_benefit_positions || []).map((x) => x.ticker).join(", ") || "--")}</div>
      <div class="summary-line">${cwEsc(s.advice || "--")}</div>
    ` : `<p class='subtle'>${cwEsc(cwT("no_summary"))}</p>`);
  }

  async function refreshPositions() {
    if (!currentWatchlistId) {
      cwText(posRoot, `<p class='subtle'>${cwEsc(cwT("create_or_select_watchlist"))}</p>`);
      return;
    }
    const res = await cwGet(`/api/portfolio/watchlists/${currentWatchlistId}/positions`);
    const rows = res?.items || [];
    if (!rows.length) {
      cwText(posRoot, `<p class='subtle'>${cwEsc(cwT("no_positions_yet"))}</p>`);
      return;
    }
    cwText(posRoot, `<table class="data-table"><thead><tr><th>${cwT("ticker")}</th><th>${cwT("qty")}</th><th>${cwT("macro_risk_score")}</th><th>${cwT("action")}</th></tr></thead><tbody>${
      rows.map((x) => `<tr><td>${cwEsc(x.ticker)}</td><td>${cwEsc(x.quantity)}</td><td>${cwEsc(x.macro_signal?.macro_risk_score ?? "--")}</td><td>${cwEsc(x.macro_signal?.action_bias ?? "--")}</td></tr>`).join("")
    }</tbody></table>`);
  }

  async function refreshLists() {
    const res = await cwGet("/api/portfolio/watchlists");
    const rows = res?.items || [];
    if (!rows.length) {
      cwText(listRoot, `<p class='subtle'>${cwEsc(cwT("no_watchlists"))}</p>`);
      return;
    }
    cwText(listRoot, rows.map((x) => `<button class="report-link" data-watchlist="${x.id}">${cwEsc(x.list_name)} · ${cwEsc(x.user_email)}</button>`).join(""));
    listRoot.querySelectorAll("[data-watchlist]").forEach((btn) => {
      btn.addEventListener("click", async () => {
        currentWatchlistId = btn.getAttribute("data-watchlist");
        await refreshPositions();
        await refreshSummary();
      });
    });
    if (!currentWatchlistId && rows[0]) currentWatchlistId = rows[0].id;
    await refreshPositions();
    await refreshSummary();
  }

  document.getElementById("watchlist-create")?.addEventListener("click", async () => {
    const name = String(nameInput?.value || "").trim();
    if (!name) return;
    await fetch(cwApi("/api/portfolio/watchlists"), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      credentials: "include",
      body: JSON.stringify({ list_name: name }),
    });
    await refreshLists();
  });

  document.getElementById("position-add")?.addEventListener("click", async () => {
    if (!currentWatchlistId) return;
    const ticker = String(tickerInput?.value || "").trim().toUpperCase();
    if (!ticker) return;
    await fetch(cwApi(`/api/portfolio/watchlists/${currentWatchlistId}/positions`), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      credentials: "include",
      body: JSON.stringify({ ticker, quantity: Number(qtyInput?.value || 0) || 0 }),
    });
    await refreshPositions();
    await refreshSummary();
  });

  await loadCurrentUser();
  await loadTickerSuggestions();
  await refreshLists();
}

async function initRegimeTransmissionPage() {
  const [regimeRes, histRes, overlayRes, transRes, biasRes] = await Promise.all([
    cwGet("/api/regime/latest"),
    cwGet("/api/regime/history?days=90"),
    cwGet("/api/geopolitical-overlay/latest"),
    cwGet("/api/transmission/latest"),
    cwGet("/api/action-bias/latest"),
  ]);
  const regime = regimeRes?.item;
  const hist = histRes?.items || [];
  const overlay = overlayRes?.item;
  const trans = transRes?.item;
  const bias = biasRes?.item;
  cwText(document.getElementById("rt-regime-card"), regime ? `
    <div class="summary-score">${cwEsc(cwMapValue(regime.regime_code) || regime.regime_label)}</div>
    <div class="summary-line">${cwT("code")}: ${cwEsc(regime.regime_code)}</div>
    <div class="summary-line">${cwT("confidence")}: ${cwEsc(regime.confidence)}</div>
    ${cwList(regime.drivers_json || [])}
  ` : "<p class='subtle'>--</p>");
  cwText(document.getElementById("rt-overlay-card"), overlay ? `
    <div class="summary-score">${cwEsc(overlay.oil_shock_scenario)}</div>
    <div class="summary-line">${cwT("conflict")}: ${cwEsc(cwMapValue(overlay.conflict_level))}</div>
    <div class="summary-line">${cwT("supply")}: ${cwEsc(cwMapValue(overlay.supply_disruption_level))}</div>
    <div class="summary-line">${cwEsc(cwMapValue(overlay.summary || "--"))}</div>
  ` : "<p class='subtle'>--</p>");
  cwText(document.getElementById("rt-regime-history"), hist.length ? `
    <table class="data-table"><thead><tr><th>${cwT("date")}</th><th>${cwT("regime")}</th><th>${cwT("score_delta")}</th><th>${cwT("delta")}</th></tr></thead><tbody>${
      hist.map((x) => `<tr><td>${cwEsc(x.as_of_date)}</td><td>${cwEsc(cwMapValue(x.regime_code) || x.regime_label)}</td><td>${cwEsc(x.score)}</td><td>${cwEsc(x.score_delta)}</td></tr>`).join("")
    }</tbody></table>` : `<p class='subtle'>${cwEsc(cwT("no_history"))}</p>`);
  cwText(document.getElementById("rt-action-bias"), bias ? `
    <div class="summary-score">${cwEsc(cwMapValue(bias.overall_bias))}</div>
    <div class="summary-line">${cwT("favored_styles")}: ${cwEsc((bias.favored_styles_json || []).map((x) => cwMapValue(x)).join(", "))}</div>
    <div class="summary-line">${cwT("avoided_styles")}: ${cwEsc((bias.avoided_styles_json || []).map((x) => cwMapValue(x)).join(", "))}</div>
    <div class="summary-line">${cwEsc(cwMapValue(bias.summary || "--"))}</div>
  ` : "<p class='subtle'>--</p>");
  cwText(document.getElementById("rt-transmission"), trans ? cwObjTable({
    rates: trans.rates_bias,
    equities: trans.equities_bias,
    credit: trans.credit_bias,
    usd: trans.usd_bias,
    commodities: trans.commodities_bias,
    crypto: trans.crypto_bias,
    favored: (trans.sectors_json || {}).favored || [],
    avoided: (trans.sectors_json || {}).avoided || [],
  }) : "<p class='subtle'>--</p>");

  cwText(document.getElementById("rt-regime-explain"), regime ? buildRegimeExplain(regime, hist) : "<p class='subtle'>--</p>");
  cwText(document.getElementById("rt-overlay-explain"), overlay ? buildOverlayExplain(overlay) : "<p class='subtle'>--</p>");
  cwText(document.getElementById("rt-action-explain"), bias ? buildActionExplain(bias) : "<p class='subtle'>--</p>");
  cwText(document.getElementById("rt-transmission-explain"), trans ? buildTransmissionExplain(trans) : "<p class='subtle'>--</p>");
  cwText(document.getElementById("rt-investor-brief"), buildInvestorBrief(regime, overlay, bias, trans, hist));
}

async function rerenderCurrentCapitalWarningPage() {
  const page = document.body?.dataset?.page || "";
  if (page === "dashboard") await renderDashboardCapitalWarning();
  if (page === "daily-report") await renderDailyCapitalWarning();
  if (page === "stock-prediction") await renderStockCapitalWarning();
  if (page === "portfolio-watchlist") await initPortfolioWatchlistPage();
  if (page === "regime-transmission") await initRegimeTransmissionPage();
}

document.addEventListener("DOMContentLoaded", async () => {
  const page = document.body?.dataset?.page || "";
  await rerenderCurrentCapitalWarningPage();
  if (page === "stock-prediction") {
    document.getElementById("stock-refresh")?.addEventListener("click", () => setTimeout(renderStockCapitalWarning, 50));
  }
  document.getElementById("lang-toggle")?.addEventListener("click", () => {
    setTimeout(() => {
      rerenderCurrentCapitalWarningPage();
    }, 20);
  });
});
