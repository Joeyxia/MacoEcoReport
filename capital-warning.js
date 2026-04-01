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
    operation: "Operation",
    edit_qty: "Modify",
    delete_position: "Delete",
    invalid_qty: "Invalid quantity",
    delete_confirm: "Delete this holding?",
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
    recommend_sell: "Recommend Sell",
    recommend_hold: "Recommend Hold",
    recommend_keep_position: "Recommend Keep Current Position",
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
    investor_watch: "Watch list",
    final_regime: "Final Regime",
    overlay_level: "Overlay Level",
    decision_source: "Decision Source",
    score_background: "Background Score",
    normalized_score: "Normalized Score",
    score_cap: "Score Cap",
    layer_shock: "Shock Layer",
    layer_tactical: "Tactical Layer",
    layer_cyclical: "Cyclical Layer",
    decision_priority: "Decision Priority",
    signal_confidence_score: "Signal Confidence",
    action_size_cap: "Action Size Cap",
    hedge_preference: "Hedge Preference",
    run_date: "Run Date",
    no_v21_run: "No v2.1 run snapshot."
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
    operation: "操作",
    edit_qty: "修改",
    delete_position: "删除",
    invalid_qty: "数量格式不正确",
    delete_confirm: "确认删除这只持仓吗？",
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
    recommend_sell: "建议卖出",
    recommend_hold: "建议持仓",
    recommend_keep_position: "建议保持现有仓位",
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
    investor_watch: "重点观察",
    final_regime: "最终状态",
    overlay_level: "叠加层级",
    decision_source: "决策来源",
    score_background: "背景分",
    normalized_score: "归一分",
    score_cap: "分数上限",
    layer_shock: "冲击层",
    layer_tactical: "战术层",
    layer_cyclical: "周期层",
    decision_priority: "决策优先级",
    signal_confidence_score: "信号置信分",
    action_size_cap: "动作仓位上限",
    hedge_preference: "对冲偏好",
    run_date: "运行日期",
    no_v21_run: "暂无 v2.1 运行快照。"
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
    recommend_sell: "recommend_sell",
    recommend_hold: "recommend_hold",
    recommend_keep_position: "recommend_keep_position",
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

function cwRunDateOrToday(runDate) {
  const raw = String(runDate || "").trim();
  if (raw) return raw;
  return new Date().toISOString().slice(0, 10);
}

function cwLayerMap(rows) {
  const out = { shock: null, tactical: null, cyclical: null };
  (rows || []).forEach((x) => {
    const k = String(x.layer || "").trim().toLowerCase();
    if (k && Object.prototype.hasOwnProperty.call(out, k)) out[k] = x;
  });
  return out;
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

function cwFmtNum(v, digits = 1) {
  const n = Number(v);
  if (!Number.isFinite(n)) return "--";
  return n.toFixed(digits);
}

function cwFmtTop(items, zh) {
  const rows = Array.isArray(items) ? items : [];
  if (!rows.length) return "--";
  return rows.slice(0, 3).map((x) => {
    const ticker = String(x?.ticker || "--").toUpperCase();
    const score = cwFmtNum(x?.macro_risk_score, 1);
    const action = cwMapValue(x?.recommended_action || x?.action_bias || "--");
    return zh ? `${ticker}（分数 ${score}，建议 ${action}）` : `${ticker} (score ${score}, suggestion ${action})`;
  }).join(zh ? "；" : "; ");
}

function cwMacroStatusText(rawStatus) {
  const raw = String(rawStatus || "").trim();
  if (!raw) return "--";
  const zhMap = {
    mild_expansion: "温和扩张",
    expansion: "温和扩张",
    neutral: "中性",
    caution: "谨慎",
    contraction_warning: "收缩预警",
    contraction: "收缩",
    crisis_watch: "危机观察",
    crisis: "危机",
  };
  const enMap = {
    温和扩张: "Mild Expansion",
    中性: "Neutral",
    谨慎: "Cautious",
    收缩预警: "Contraction Warning",
    收缩: "Contraction",
    危机观察: "Crisis Watch",
    危机: "Crisis",
  };
  const key = raw.toLowerCase().replace(/\s+/g, "_");
  if (cwLang() === "zh") return zhMap[key] || raw;
  return enMap[raw] || cwHumanize(raw);
}

function buildPortfolioDetailedSummary(summary, modelCtx) {
  const s = summary || {};
  const zh = cwLang() === "zh";
  const macroScore = Number(modelCtx?.totalScore || 0);
  const macroAsOf = String(modelCtx?.asOf || "--");
  const macroStatus = cwMacroStatusText(modelCtx?.status || "--");
  const avg = Number(s.average_macro_risk_score || 0);
  const cnt = Number(s.count || 0);
  const highRisk = Number(s.high_risk_count || 0);
  const riskOff = Number(s.risk_off_count || 0);
  const domBias = cwMapValue(s.dominant_action_bias || "--");
  const recCnt = s.recommendation_count || {};
  const sellCnt = Number(recCnt.recommend_sell || 0);
  const holdCnt = Number(recCnt.recommend_hold || 0);
  const keepCnt = Number(recCnt.recommend_keep_position || 0);
  const topRiskText = cwFmtTop(s.top_risk_positions, zh);
  const topBenefitText = cwFmtTop(s.top_benefit_positions, zh);

  const recs = [];
  if (macroScore >= 70 || avg >= 70) {
    recs.push(zh
      ? "总风险偏高：控制总仓位节奏，优先降低高波动与高估值敞口。"
      : "Risk is elevated: control gross exposure pace and reduce high-volatility / high-valuation names first.");
  } else if (macroScore <= 55 && avg <= 55) {
    recs.push(zh
      ? "风险处于可控区：可分批优化仓位，但仍需保留风控缓冲。"
      : "Risk is relatively contained: rebalance in tranches while keeping risk buffers.");
  } else {
    recs.push(zh
      ? "风险中性偏谨慎：以结构调整为主，避免一次性大幅加仓。"
      : "Risk is neutral-cautious: prioritize rotation and avoid aggressive one-shot adds.");
  }
  if (highRisk > 0) {
    recs.push(zh
      ? `高风险持仓 ${highRisk} 个：对高风险个股设置更紧的止损/减仓阈值。`
      : `${highRisk} high-risk holdings: set tighter stop-loss/de-risk thresholds for these names.`);
  }
  if (riskOff > 0) {
    recs.push(zh
      ? `存在 ${riskOff} 个“风险回避”信号：建议增加对冲或提升现金比例。`
      : `${riskOff} holdings are in risk-off mode: add hedges or increase cash buffer.`);
  }
  recs.push(zh
    ? `主导动作偏向为“${domBias}”，建议围绕该偏向做组合微调。`
    : `Dominant action bias is "${domBias}", and portfolio tuning should follow this stance.`);

  return `
    <div class="summary-score">${cwEsc(cwFmtNum(avg, 1))}</div>
    <div class="summary-line">${cwEsc(zh ? "持仓数量" : "Positions")}: ${cwEsc(cnt)}</div>
    <div class="summary-line"><strong>${cwEsc(zh ? "宏观环境" : "Macro context")}:</strong> ${cwEsc(zh ? `模型分数 ${cwFmtNum(macroScore, 1)}/100，状态 ${macroStatus}（更新日 ${macroAsOf}）` : `Model score ${cwFmtNum(macroScore, 1)}/100, regime ${macroStatus} (as of ${macroAsOf})`)}</div>
    <div class="summary-line"><strong>${cwEsc(zh ? "风险结构" : "Risk structure")}:</strong> ${cwEsc(zh ? `高风险持仓 ${highRisk} 个；风险回避信号 ${riskOff} 个；主导动作偏向 ${domBias}` : `High-risk holdings ${highRisk}; risk-off signals ${riskOff}; dominant bias ${domBias}`)}</div>
    <div class="summary-line"><strong>${cwEsc(zh ? "直接建议分布" : "Direct suggestion mix")}:</strong> ${cwEsc(zh ? `建议卖出 ${sellCnt} 个；建议持仓 ${holdCnt} 个；建议保持现有仓位 ${keepCnt} 个` : `Recommend Sell ${sellCnt}; Recommend Hold ${holdCnt}; Recommend Keep Position ${keepCnt}`)}</div>
    <div class="summary-line"><strong>${cwEsc(zh ? "主要风险持仓" : "Top risk holdings")}:</strong> ${cwEsc(topRiskText)}</div>
    <div class="summary-line"><strong>${cwEsc(zh ? "相对受益持仓" : "Potential beneficiaries")}:</strong> ${cwEsc(topBenefitText)}</div>
    <div class="summary-line"><strong>${cwEsc(zh ? "投资动作建议" : "Action plan")}:</strong></div>
    <ul class="preview-list">${recs.map((x) => `<li>${cwEsc(x)}</li>`).join("")}</ul>
  `;
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

function cwRegimeScoreBand(score, zh) {
  if (score >= 75) return zh ? "强扩张（风险偏好可提高）" : "Strong expansion (risk-on can be increased)";
  if (score >= 60) return zh ? "温和扩张（可偏进攻，但要控回撤）" : "Moderate expansion (lean risk-on with drawdown control)";
  if (score >= 45) return zh ? "中性过渡（仓位中性、等待确认）" : "Neutral transition (stay balanced, wait for confirmation)";
  return zh ? "偏防守（先保资本再求收益）" : "Defensive (protect capital first)";
}

function cwOverlayRiskLevel(conflict, supply) {
  const c = String(conflict || "").toLowerCase();
  const s = String(supply || "").toLowerCase();
  const toN = (x) => (x === "high" ? 3 : x === "medium" ? 2 : x === "low" ? 1 : 0);
  return toN(c) + toN(s);
}

function cwTransmissionRiskCount(trans) {
  const keys = ["rates_bias", "equities_bias", "credit_bias", "usd_bias", "commodities_bias", "crypto_bias"];
  let n = 0;
  keys.forEach((k) => {
    const v = String(trans?.[k] || "").toLowerCase();
    if (["tightening", "fragile", "risk_off", "defensive", "bearish", "stress"].includes(v)) n += 1;
  });
  return n;
}

function buildRegimeExplain(regime, hist) {
  const zh = cwLang() === "zh";
  const score = Number(regime?.score || 0);
  const delta = Number(regime?.score_delta || 0);
  const trend = hist?.length >= 2 ? Number(hist[0]?.score || 0) - Number(hist[Math.min(6, hist.length - 1)]?.score || 0) : 0;
  const trendText = trend > 1 ? (zh ? "近一周分数上行" : "score rose over the last week") : trend < -1 ? (zh ? "近一周分数下行" : "score fell over the last week") : (zh ? "近一周分数大体持平" : "score stayed roughly flat over the last week");
  const band = cwRegimeScoreBand(score, zh);
  const actionNow = delta >= 1.5
    ? (zh ? "短线可小幅加仓风险资产，但不要一次性满仓。" : "You may add risk in small steps, avoid one-shot full allocation.")
    : delta <= -1.5
      ? (zh ? "先降杠杆和高波动仓位，观察 1-2 天再决定是否继续减仓。" : "Cut leverage/high-vol first, then reassess in 1-2 days.")
      : (zh ? "保持中性仓位，等下一次数据确认后再调整方向。" : "Keep neutral sizing until the next data confirmation.");
  return cwExplainBlock({
    indicators: ["regime_code", "confidence", "score", "delta"],
    how: zh
      ? [
          `当前总分 ${score.toFixed(2)}，日变化 ${delta.toFixed(2)}。用白话说：现在属于“${band}”。`,
          `${trendText}。判断是否转向时，看“连续 3 天”而不是单日波动。`,
          `实操上：分数高于 60 更适合做进攻配置；低于 45 时先做防守和降波动。`
        ]
      : [
          `Score is ${score.toFixed(2)} and daily delta is ${delta.toFixed(2)}. In plain terms: "${band}".`,
          `${trendText}. Use a 3-day confirmation rule instead of reacting to one day.`,
          `In practice: above 60 supports offensive allocation; below 45 favors defense and volatility reduction.`
        ],
    takeaway: zh
      ? [
          `决策建议：${actionNow}`,
          "风险管理：每次调仓控制在组合净值 10%-20% 的增减，避免情绪化全仓切换。"
        ]
      : [
          `Decision cue: ${actionNow}`,
          "Risk rule: rebalance in 10%-20% NAV steps instead of abrupt all-in/all-out switches."
        ]
  });
}

function buildOverlayExplain(overlay) {
  const zh = cwLang() === "zh";
  const conflict = cwMapValue(overlay?.conflict_level || "--");
  const supply = cwMapValue(overlay?.supply_disruption_level || "--");
  const rawRisk = cwOverlayRiskLevel(overlay?.conflict_level, overlay?.supply_disruption_level);
  const riskTag = rawRisk >= 5 ? (zh ? "高压区" : "high-stress") : rawRisk >= 3 ? (zh ? "观察区" : "watch zone") : (zh ? "常态区" : "normal zone");
  return cwExplainBlock({
    indicators: ["conflict", "supply", "summary"],
    how: zh
      ? [
          `冲突等级 ${conflict}，供应冲击 ${supply}，当前属于“${riskTag}”。`,
          "直观理解：这两个指标上升，通常先推升油气和运输成本，再抬高通胀预期，最后压制成长资产估值。",
          "如果你是股票投资者，这往往意味着：高估值成长股的波动会先放大。"
        ]
      : [
          `Conflict=${conflict}, supply shock=${supply}, currently in "${riskTag}".`,
          "Plain reading: geopolitics usually lifts energy/logistics first, then inflation expectations, then compresses growth valuations.",
          "For equity investors: high-multiple growth names usually feel this pressure first."
        ],
    takeaway: zh
      ? [
          "决策建议：地缘风险在“高压区”时，降低单一赛道集中度，优先保留现金流稳健标的。",
          "对冲建议：可用指数对冲或降低净仓位，目标是把组合日波动先压下来。"
        ]
      : [
          "Decision cue: in high-stress regime, reduce single-theme concentration and prefer cash-flow resilient names.",
          "Hedging cue: use index hedges or lower net exposure to compress daily portfolio volatility."
        ]
  });
}

function buildActionExplain(bias) {
  const zh = cwLang() === "zh";
  const action = cwMapValue(bias?.overall_bias || "--");
  const favored = (bias?.favored_styles_json || []).map((x) => cwMapValue(x)).join(", ") || "--";
  const avoided = (bias?.avoided_styles_json || []).map((x) => cwMapValue(x)).join(", ") || "--";
  const actionHint = zh
    ? (String(action).includes("不新增") || String(action).includes("防守")
      ? "当前更适合“先管风险，再找机会”。"
      : "当前允许提高进攻仓位，但仍要分批执行。")
    : (String(action).toLowerCase().includes("avoid")
      ? "Priority is risk control first, opportunity second."
      : "You can lean offensive, but still scale in by tranches.");
  return cwExplainBlock({
    indicators: ["action_bias", "favored_styles", "avoided_styles"],
    how: zh
      ? [
          `当前动作偏向为“${action}”。${actionHint}`,
          `偏好风格：${favored}；回避风格：${avoided}。`,
          "可把它理解成交易节奏器：它不一定告诉你买哪只票，但会告诉你“该快还是该慢”。"
        ]
      : [
          `Current action bias is "${action}". ${actionHint}`,
          `Favored styles: ${favored}; avoided styles: ${avoided}.`,
          "Think of it as a pacing signal: not always what to buy, but how fast to deploy risk."
        ],
    takeaway: zh
      ? [
          "执行建议：先定总仓位上限，再在偏好风格里做结构优化，避免边加仓边追高。",
          "复盘建议：若连续两天动作偏向未变，执行可以更稳定；若频繁切换，优先减交易频率。"
        ]
      : [
          "Execution cue: set exposure cap first, then rotate into favored styles without chasing spikes.",
          "Review cue: stable bias for 2+ days supports steady execution; frequent flips call for lower trading frequency."
        ]
  });
}

function buildTransmissionExplain(trans) {
  const zh = cwLang() === "zh";
  const riskCount = cwTransmissionRiskCount(trans);
  const riskBand = riskCount >= 4 ? (zh ? "高传导风险" : "high transmission risk")
    : riskCount >= 2 ? (zh ? "中等传导风险" : "medium transmission risk")
      : (zh ? "低传导风险" : "low transmission risk");
  return cwExplainBlock({
    indicators: ["rates", "equities", "credit", "usd", "commodities", "crypto"],
    how: zh
      ? [
          `利率=${cwMapValue(trans?.rates_bias)}, 权益=${cwMapValue(trans?.equities_bias)}, 信用=${cwMapValue(trans?.credit_bias)}。`,
          `当前判定为“${riskBand}”。传导热力表用于判断“风险如何扩散”：先看利率与信用，再看权益和商品确认。`,
          "白话理解：当“利率+信用”同时偏紧时，后续更容易看到估值压缩和融资成本上升。"
        ]
      : [
          `Rates=${cwMapValue(trans?.rates_bias)}, Equities=${cwMapValue(trans?.equities_bias)}, Credit=${cwMapValue(trans?.credit_bias)}.`,
          `Current reading is "${riskBand}". Use heatmap as risk propagation map: rates/credit first, then equities/commodities confirmation.`,
          "Plain language: when rates and credit tighten together, valuation compression and financing stress often follow."
        ],
    takeaway: zh
      ? [
          "决策建议：若信用与利率同时走弱，降低净风险暴露，并把复盘频率提高到“每日”。",
          "持仓建议：优先减少高负债、高估值、对融资环境敏感的资产。"
        ]
      : [
          "Decision cue: if credit and rates deteriorate together, cut net risk and move to daily review cadence.",
          "Positioning cue: reduce leverage-sensitive and high-duration/high-valuation exposures first."
        ]
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
  const oneLine = zh
    ? "一句话给投资人：先看方向，再控节奏；先守回撤，再争收益。"
    : "One-line investor cue: direction first, pacing second; protect drawdown before chasing return.";
  return `
    <div class="summary-line"><strong>${cwEsc(zh ? "一句话结论" : "One-line conclusion")}:</strong> ${cwEsc(oneLine)}</div>
    <div class="summary-line"><strong>${cwEsc(cwT("investor_scenario"))}:</strong> ${cwEsc(`${regimeLabel} · ${trendView}`)}</div>
    <div class="summary-line"><strong>${cwEsc(cwT("investor_risk"))}:</strong> ${cwEsc(risk)}</div>
    <div class="summary-line"><strong>${cwEsc(cwT("investor_positioning"))}:</strong> ${cwEsc(positioning)}</div>
    <div class="summary-line"><strong>${cwEsc(cwT("investor_watch"))}:</strong> ${cwEsc(watch)}</div>
  `;
}

async function renderDashboardCapitalWarning() {
  const [
    latestRunRes,
    latestAnalysisRes,
    regimeRes,
    overlayRes,
    transRes,
    biasRes,
    alertsRes,
    summaryRes,
  ] = await Promise.all([
    cwGet("/api/latest-run"),
    cwGet("/api/latest-analysis"),
    cwGet("/api/regime/latest"),
    cwGet("/api/geopolitical-overlay/latest"),
    cwGet("/api/transmission/latest"),
    cwGet("/api/action-bias/latest"),
    cwGet("/api/alerts/latest"),
    cwGet("/api/portfolio/risk-summary"),
  ]);
  const latestRun = latestRunRes?.item || null;
  const latestAnalysis = latestAnalysisRes?.item || null;
  const runDate = cwRunDateOrToday(latestRun?.as_of_date || latestAnalysisRes?.as_of_date);
  const [layersRes, overlayDecisionRes, geoRunRes] = await Promise.all([
    cwGet(`/api/run/${encodeURIComponent(runDate)}/regime-layers`),
    cwGet(`/api/run/${encodeURIComponent(runDate)}/overlay-decision`),
    cwGet(`/api/run/${encodeURIComponent(runDate)}/geopolitical-overlay`),
  ]);
  const layerMap = cwLayerMap(layersRes?.items || []);
  const overlayDecision = overlayDecisionRes?.item || null;
  const geoRun = geoRunRes?.item || null;
  const regime = regimeRes?.item;
  const overlay = overlayRes?.item;
  const trans = transRes?.item;
  const bias = biasRes?.item;
  const alerts = alertsRes?.items || [];
  const portfolio = summaryRes?.summary;

  cwText(document.getElementById("regime-engine-card"),
    latestRun ? `
      <div class="summary-score">${cwEsc(cwMapValue(latestRun.final_regime || "--"))}</div>
      <div class="summary-line">${cwT("run_date")}: ${cwEsc(runDate)}</div>
      <div class="summary-line">${cwT("decision_source")}: ${cwEsc(cwMapValue(latestRun.primary_decision_source || "--"))}</div>
      <div class="summary-line">${cwT("score_background")}: ${cwEsc(latestRun.score_background ?? "--")} / ${cwT("normalized_score")}: ${cwEsc(latestRun.normalized_score ?? "--")}</div>
      <div class="summary-line">${cwT("confidence")}: ${cwEsc(latestRun.regime_confidence ?? "--")}</div>
      <div class="summary-line">${cwEsc(latestRun.topline_message || "--")}</div>
    ` : regime ? `
      <div class="summary-score">${cwEsc(cwMapValue(regime.regime_code) || regime.regime_label)}</div>
      <div class="summary-line">${cwT("code")}: ${cwEsc(regime.regime_code)}</div>
      <div class="summary-line">${cwT("confidence")}: ${cwEsc(regime.confidence)}</div>
      <div class="summary-line">${cwT("score_delta")}: ${cwEsc(regime.score)} / ${cwT("delta")}: ${cwEsc(regime.score_delta)}</div>
      ${cwList(regime.drivers_json || [])}
    ` : `<p class='subtle'>${cwEsc(cwT("no_v21_run"))}</p>`
  );
  cwText(document.getElementById("geopolitical-overlay-card"),
    (geoRun || overlayDecision || overlay) ? `
      <div class="summary-score">${cwEsc(cwMapValue((overlayDecision?.overlay_level) || (geoRun?.overlay_level) || (overlay?.oil_shock_scenario) || "--"))}</div>
      <div class="summary-line">${cwT("overlay_level")}: ${cwEsc(cwMapValue((overlayDecision?.overlay_level) || (geoRun?.overlay_level) || "--"))}</div>
      <div class="summary-line">${cwT("score_cap")}: ${cwEsc(overlayDecision?.score_cap ?? "--")}</div>
      <div class="summary-line">${cwT("conflict")}: ${cwEsc(geoRun?.conflict_intensity_score ?? cwMapValue(overlay?.conflict_level || "--"))}</div>
      <div class="summary-line">${cwT("supply")}: ${cwEsc(geoRun?.supply_disruption_score ?? cwMapValue(overlay?.supply_disruption_level || "--"))}</div>
      <div class="summary-line">${cwT("summary")}: ${cwEsc(cwMapValue((overlayDecision?.rationale) || (geoRun?.conclusion) || (overlay?.summary) || "--"))}</div>
    ` : `<p class='subtle'>${cwEsc(cwT("no_overlay_snapshot"))}</p>`
  );
  cwText(document.getElementById("action-bias-card"),
    (latestAnalysis || bias) ? `
      <div class="summary-score">${cwEsc(cwMapValue(bias?.overall_bias || "--"))}</div>
      <div class="summary-line">${cwT("decision_priority")}: ${cwEsc(cwMapValue(latestAnalysis?.decision_priority || "--"))}</div>
      <div class="summary-line">${cwT("signal_confidence_score")}: ${cwEsc(latestAnalysis?.signal_confidence_score ?? "--")}</div>
      <div class="summary-line">${cwT("action_size_cap")}: ${cwEsc(latestAnalysis?.action_size_cap ?? "--")}</div>
      <div class="summary-line">${cwT("hedge_preference")}: ${cwEsc(cwMapValue(latestAnalysis?.hedge_preference || "--"))}</div>
      <div class="summary-line">${cwT("favored_styles")}: ${cwEsc(((bias?.favored_styles_json) || []).map((x) => cwMapValue(x)).join(", "))}</div>
      <div class="summary-line">${cwT("avoided_styles")}: ${cwEsc(((bias?.avoided_styles_json) || []).map((x) => cwMapValue(x)).join(", "))}</div>
      <div class="summary-line">${cwEsc(cwMapValue((latestAnalysis?.overlay_summary) || (bias?.summary) || "--"))}</div>
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
    (trans || layersRes?.items?.length) ? cwObjTable({
      [cwT("layer_shock")]: layerMap.shock ? `${cwMapValue(layerMap.shock.layer_regime)} (${layerMap.shock.layer_score})` : "--",
      [cwT("layer_tactical")]: layerMap.tactical ? `${cwMapValue(layerMap.tactical.layer_regime)} (${layerMap.tactical.layer_score})` : "--",
      [cwT("layer_cyclical")]: layerMap.cyclical ? `${cwMapValue(layerMap.cyclical.layer_regime)} (${layerMap.cyclical.layer_score})` : "--",
      rates_bias: trans?.rates_bias || "--",
      equities_bias: trans?.equities_bias || "--",
      credit_bias: trans?.credit_bias || "--",
      usd_bias: trans?.usd_bias || "--",
      commodities_bias: trans?.commodities_bias || "--",
      crypto_bias: trans?.crypto_bias || "--",
      favored_sectors: (trans?.sectors_json || {}).favored || [],
      avoided_sectors: (trans?.sectors_json || {}).avoided || [],
    }) : `<p class='subtle'>${cwEsc(cwT("no_transmission_snapshot"))}</p>`
  );
}

async function renderDailyCapitalWarning() {
  const params = new URLSearchParams(location.search);
  const reportDate = params.get("date") || new Date().toISOString().slice(0, 10);
  const [regimeRes, impactRes, overlayRes, transRes, biasRes, watchRes] = await Promise.all([
    cwGet(`/api/reports/${reportDate}/regime`),
    cwGet(`/api/reports/${reportDate}/portfolio-impact`),
    cwGet("/api/geopolitical-overlay/latest"),
    cwGet("/api/transmission/latest"),
    cwGet("/api/action-bias/latest"),
    cwGet("/api/portfolio/risk-summary"),
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
    const rows = Array.isArray(data?.tickers) ? data.tickers : (Array.isArray(data?.items) ? data.items : []);
    if (!tickerDatalist) return;
    tickerDatalist.innerHTML = "";
    rows.forEach((x) => {
      const t = String(typeof x === "string" ? x : (x?.ticker || x?.symbol || ""))
        .trim()
        .toUpperCase();
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
    const [res, modelCtx] = await Promise.all([
      cwGet(`/api/portfolio/risk-summary${q}`),
      cwGet("/api/model/current?view=core"),
    ]);
    const s = res?.summary;
    cwText(riskRoot, s ? buildPortfolioDetailedSummary(s, modelCtx) : `<p class='subtle'>${cwEsc(cwT("no_summary"))}</p>`);
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
    cwText(posRoot, `<table class="data-table"><thead><tr><th>${cwT("ticker")}</th><th>${cwT("qty")}</th><th>${cwT("macro_risk_score")}</th><th>${cwT("action")}</th><th>${cwT("operation")}</th></tr></thead><tbody>${
      rows.map((x) => `
        <tr>
          <td>${cwEsc(x.ticker)}</td>
          <td>${cwEsc(x.quantity)}</td>
          <td>${cwEsc(x.macro_signal?.macro_risk_score ?? "--")}</td>
          <td>
            <div>${cwEsc(cwMapValue(x.portfolio_recommendation?.recommended_action || "--"))}</div>
            <div class="subtle">${cwEsc(cwMapValue(x.macro_signal?.action_bias ?? "--"))}</div>
          </td>
          <td>
            <div style="display:flex;gap:6px;flex-wrap:wrap;">
              <button type="button" class="btn ghost" data-pos-edit="${cwEsc(x.ticker)}" data-pos-qty="${cwEsc(x.quantity)}" style="padding:4px 8px;font-size:.78rem;">${cwEsc(cwT("edit_qty"))}</button>
              <button type="button" class="btn ghost" data-pos-del="${cwEsc(x.ticker)}" style="padding:4px 8px;font-size:.78rem;">${cwEsc(cwT("delete_position"))}</button>
            </div>
          </td>
        </tr>`).join("")
    }</tbody></table>`);

    posRoot.querySelectorAll("[data-pos-edit]").forEach((btn) => {
      btn.addEventListener("click", async () => {
        const t = String(btn.getAttribute("data-pos-edit") || "").toUpperCase();
        const currentQty = String(btn.getAttribute("data-pos-qty") || "0");
        const nextRaw = window.prompt(cwLang() === "zh" ? `请输入 ${t} 的新数量` : `Enter new quantity for ${t}`, currentQty);
        if (nextRaw === null) return;
        const nextQty = Number(String(nextRaw).trim());
        if (!Number.isFinite(nextQty) || nextQty < 0) {
          window.alert(cwT("invalid_qty"));
          return;
        }
        await fetch(cwApi(`/api/portfolio/watchlists/${currentWatchlistId}/positions`), {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          credentials: "include",
          body: JSON.stringify({ ticker: t, quantity: nextQty }),
        });
        await refreshPositions();
        await refreshSummary();
      });
    });

    posRoot.querySelectorAll("[data-pos-del]").forEach((btn) => {
      btn.addEventListener("click", async () => {
        const t = String(btn.getAttribute("data-pos-del") || "").toUpperCase();
        if (!window.confirm(`${cwT("delete_confirm")} ${t}`)) return;
        await fetch(cwApi(`/api/portfolio/watchlists/${currentWatchlistId}/positions/${encodeURIComponent(t)}`), {
          method: "DELETE",
          credentials: "include",
        });
        await refreshPositions();
        await refreshSummary();
      });
    });
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

  tickerInput?.addEventListener("input", () => {
    const cur = String(tickerInput.value || "");
    tickerInput.value = cur.toUpperCase();
  });

  await loadCurrentUser();
  await loadTickerSuggestions();
  await refreshLists();
}

async function initRegimeTransmissionPage() {
  const [latestRunRes, latestAnalysisRes, regimeRes, histRes, overlayRes, transRes, biasRes] = await Promise.all([
    cwGet("/api/latest-run"),
    cwGet("/api/latest-analysis"),
    cwGet("/api/regime/latest"),
    cwGet("/api/regime/history?days=90"),
    cwGet("/api/geopolitical-overlay/latest"),
    cwGet("/api/transmission/latest"),
    cwGet("/api/action-bias/latest"),
  ]);
  const latestRun = latestRunRes?.item || null;
  const latestAnalysis = latestAnalysisRes?.item || null;
  const runDate = cwRunDateOrToday(latestRun?.as_of_date || latestAnalysisRes?.as_of_date);
  const [layersRes, overlayDecisionRes, geoRunRes] = await Promise.all([
    cwGet(`/api/run/${encodeURIComponent(runDate)}/regime-layers`),
    cwGet(`/api/run/${encodeURIComponent(runDate)}/overlay-decision`),
    cwGet(`/api/run/${encodeURIComponent(runDate)}/geopolitical-overlay`),
  ]);
  const layerMap = cwLayerMap(layersRes?.items || []);
  const overlayDecision = overlayDecisionRes?.item || null;
  const geoRun = geoRunRes?.item || null;
  const regime = regimeRes?.item;
  const hist = histRes?.items || [];
  const overlay = overlayRes?.item;
  const trans = transRes?.item;
  const bias = biasRes?.item;
  cwText(document.getElementById("rt-regime-card"), regime ? `
    <div class="summary-score">${cwEsc(cwMapValue((latestRun?.final_regime) || regime.regime_code || regime.regime_label))}</div>
    <div class="summary-line">${cwT("run_date")}: ${cwEsc(runDate)}</div>
    <div class="summary-line">${cwT("decision_source")}: ${cwEsc(cwMapValue((latestRun?.primary_decision_source) || "--"))}</div>
    <div class="summary-line">${cwT("score_background")}: ${cwEsc((latestRun?.score_background) ?? regime.score ?? "--")} / ${cwT("normalized_score")}: ${cwEsc((latestRun?.normalized_score) ?? "--")}</div>
    <div class="summary-line">${cwT("confidence")}: ${cwEsc((latestRun?.regime_confidence) ?? regime.confidence ?? "--")}</div>
    ${cwList(regime.drivers_json || [])}
  ` : "<p class='subtle'>--</p>");
  cwText(document.getElementById("rt-overlay-card"), (overlay || overlayDecision || geoRun) ? `
    <div class="summary-score">${cwEsc(cwMapValue((overlayDecision?.overlay_level) || (geoRun?.overlay_level) || (overlay?.oil_shock_scenario) || "--"))}</div>
    <div class="summary-line">${cwT("overlay_level")}: ${cwEsc(cwMapValue((overlayDecision?.overlay_level) || (geoRun?.overlay_level) || "--"))}</div>
    <div class="summary-line">${cwT("score_cap")}: ${cwEsc(overlayDecision?.score_cap ?? "--")}</div>
    <div class="summary-line">${cwT("conflict")}: ${cwEsc((geoRun?.conflict_intensity_score) ?? cwMapValue(overlay?.conflict_level || "--"))}</div>
    <div class="summary-line">${cwT("supply")}: ${cwEsc((geoRun?.supply_disruption_score) ?? cwMapValue(overlay?.supply_disruption_level || "--"))}</div>
    <div class="summary-line">${cwEsc(cwMapValue((overlayDecision?.rationale) || (geoRun?.conclusion) || (overlay?.summary) || "--"))}</div>
  ` : "<p class='subtle'>--</p>");
  cwText(document.getElementById("rt-regime-history"), hist.length ? `
    <table class="data-table"><thead><tr><th>${cwT("date")}</th><th>${cwT("regime")}</th><th>${cwT("score_delta")}</th><th>${cwT("delta")}</th></tr></thead><tbody>${
      hist.map((x) => `<tr><td>${cwEsc(x.as_of_date)}</td><td>${cwEsc(cwMapValue(x.regime_code) || x.regime_label)}</td><td>${cwEsc(x.score)}</td><td>${cwEsc(x.score_delta)}</td></tr>`).join("")
    }</tbody></table>` : `<p class='subtle'>${cwEsc(cwT("no_history"))}</p>`);
  cwText(document.getElementById("rt-action-bias"), (bias || latestAnalysis) ? `
    <div class="summary-score">${cwEsc(cwMapValue(bias?.overall_bias || "--"))}</div>
    <div class="summary-line">${cwT("decision_priority")}: ${cwEsc(cwMapValue(latestAnalysis?.decision_priority || "--"))}</div>
    <div class="summary-line">${cwT("signal_confidence_score")}: ${cwEsc(latestAnalysis?.signal_confidence_score ?? "--")}</div>
    <div class="summary-line">${cwT("action_size_cap")}: ${cwEsc(latestAnalysis?.action_size_cap ?? "--")}</div>
    <div class="summary-line">${cwT("hedge_preference")}: ${cwEsc(cwMapValue(latestAnalysis?.hedge_preference || "--"))}</div>
    <div class="summary-line">${cwT("favored_styles")}: ${cwEsc(((bias?.favored_styles_json) || []).map((x) => cwMapValue(x)).join(", "))}</div>
    <div class="summary-line">${cwT("avoided_styles")}: ${cwEsc(((bias?.avoided_styles_json) || []).map((x) => cwMapValue(x)).join(", "))}</div>
    <div class="summary-line">${cwEsc(cwMapValue((latestAnalysis?.overlay_summary) || (bias?.summary) || "--"))}</div>
  ` : "<p class='subtle'>--</p>");
  cwText(document.getElementById("rt-transmission"), (trans || layersRes?.items?.length) ? cwObjTable({
    [cwT("layer_shock")]: layerMap.shock ? `${cwMapValue(layerMap.shock.layer_regime)} (${layerMap.shock.layer_score})` : "--",
    [cwT("layer_tactical")]: layerMap.tactical ? `${cwMapValue(layerMap.tactical.layer_regime)} (${layerMap.tactical.layer_score})` : "--",
    [cwT("layer_cyclical")]: layerMap.cyclical ? `${cwMapValue(layerMap.cyclical.layer_regime)} (${layerMap.cyclical.layer_score})` : "--",
    rates: trans?.rates_bias || "--",
    equities: trans?.equities_bias || "--",
    credit: trans?.credit_bias || "--",
    usd: trans?.usd_bias || "--",
    commodities: trans?.commodities_bias || "--",
    crypto: trans?.crypto_bias || "--",
    favored: (trans?.sectors_json || {}).favored || [],
    avoided: (trans?.sectors_json || {}).avoided || [],
  }) : "<p class='subtle'>--</p>");

  cwText(document.getElementById("rt-regime-explain"), regime ? buildRegimeExplain(regime, hist) : "<p class='subtle'>--</p>");
  cwText(document.getElementById("rt-overlay-explain"), (overlay || geoRun) ? buildOverlayExplain(overlay || geoRun) : "<p class='subtle'>--</p>");
  cwText(document.getElementById("rt-action-explain"), (bias || latestAnalysis) ? buildActionExplain(bias || latestAnalysis) : "<p class='subtle'>--</p>");
  cwText(document.getElementById("rt-transmission-explain"), trans ? buildTransmissionExplain(trans) : "<p class='subtle'>--</p>");
  cwText(document.getElementById("rt-investor-brief"), buildInvestorBrief(regime || latestRun, overlay || geoRun, bias || latestAnalysis, trans, hist));
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
