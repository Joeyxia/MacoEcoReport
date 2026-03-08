const DB_NAME = "macro-monitor-db";
const DB_VERSION = 1;
const STORAGE_KEY = "macro-monitor-model";
const LANG_KEY = "macro-monitor-lang";
const DEFAULT_MODEL_FILE = "./model.xlsx";
const STATIC_REPORT_INDEX = "./reports/index.json";
const STATIC_SNAPSHOT = "./data/latest_snapshot.json";
const STATIC_SUBSCRIBERS = "./data/subscribers.json";
const SUBSCRIPTION_ISSUE_URL = "https://github.com/Joeyxia/MacoEcoReport/issues/new";
const API_BASE_KEY = "macro-monitor-api-base";
const MIGRATED_KEY = "macro-monitor-db-migrated";
let dashboardHeavyRenderToken = 0;

const i18n = {
  en: {
    nav_dashboard: "Dashboard",
    nav_daily_report: "Daily Report",
    nav_indicators: "Indicators",
    nav_glossary: "Glossary",
    dashboard_eyebrow: "Global Macro Crisis Radar",
    dashboard_title: "Institutional 14-Dimension Monitoring Dashboard",
    dashboard_subtitle: "Upload your model workbook to refresh total score, dimension contribution, and warning signals.",
    load_model: "Load Model (.xlsx)",
    using_sample_data: "Using built-in sample data",
    macro_composite_score: "Macro Composite Score",
    as_of: "As of",
    triggered_alerts: "Triggered Alerts",
    top_dimension_contributors: "Top Dimension Contributors",
    primary_drivers: "Primary Drivers",
    key_indicators_overview: "Key Indicators Snapshot",
    latest_report_summary: "Latest Report Summary",
    daily_watch_items: "Daily Watch Items",
    dimensions_14_detail: "All 14 Dimensions Detail (from Dimensions Sheet)",
    dimensions_14_detail_desc: "This section shows complete dimension definitions, tiers, weights, and update frequencies.",
    model_core_tables: "Model Core Tables",
    model_core_tables_desc: "Complete structured data from your 14-dimension model.",
    dimensions: "Dimensions",
    inputs_latest: "Inputs (Latest)",
    indicators: "Indicators",
    scores: "Scores",
    alerts: "Alerts",
    workbook_explorer: "Workbook Data Explorer",
    workbook_explorer_desc: "Full raw content from every worksheet in the Excel file (all rows and columns).",
    need_today_note: "Need Today's Note?",
    need_today_note_desc: "Generate and edit a daily narrative from the current model snapshot.",
    open_daily_report: "Open Daily Report",
    daily_note: "Daily Intelligence Note",
    daily_report_title: "Macro Monitoring Daily Report",
    daily_report_desc: "Auto-drafted from the latest dashboard snapshot. Edit as needed, then save.",
    snapshot: "Snapshot",
    regenerate_draft: "Regenerate Draft",
    generate_final_report: "Generate Final Report",
    save: "Save",
    download_txt: "Download .txt",
    run_online_check: "Run online data check before final report",
    report_preview: "Report Preview (Reference Format)",
    daily_report_archive: "Daily Report Archive",
    daily_report_archive_desc: "Each saved day has a direct link.",
    online_check_results: "Online Data Check Results",
    detailed_indicator_scores: "Detailed Indicators Score",
    reference: "Reference",
    glossary_title: "Macro Terms Used in This System",
    glossary_desc: "Aligned to your 14-dimension monitoring framework and alert thresholds.",
    indicators_eyebrow: "Indicator Library",
    indicators_page_title: "All Indicators Information (from Indicators Sheet)",
    indicators_page_desc: "Full indicator definitions, scoring settings, data source links, and update frequencies.",
    all_indicators_info: "All Indicators Information"
    ,
    subscribe_title: "Email Subscription",
    subscribe_desc: "Subscribe to receive the daily report summary and link after 09:00 China time generation.",
    subscribe_email_label: "Email",
    subscribe_submit: "Subscribe",
    subscribe_note: "If backend is unavailable, subscription will fallback to GitHub request.",
    subscribe_count: "Active Subscribers"
  },
  zh: {
    nav_dashboard: "仪表盘",
    nav_daily_report: "每日报告",
    nav_indicators: "指标库",
    nav_glossary: "术语表",
    dashboard_eyebrow: "全球宏观危机雷达",
    dashboard_title: "14维机构级宏观监控仪表盘",
    dashboard_subtitle: "上传模型工作簿后，可刷新总分、维度贡献和预警信号。",
    load_model: "加载模型（.xlsx）",
    using_sample_data: "使用内置样例数据",
    macro_composite_score: "宏观综合评分",
    as_of: "更新日",
    triggered_alerts: "触发预警",
    top_dimension_contributors: "维度贡献 Top",
    primary_drivers: "核心驱动",
    key_indicators_overview: "关键指标概览",
    latest_report_summary: "最新报告摘要",
    daily_watch_items: "当日关注项",
    dimensions_14_detail: "14个维度信息（来自 Dimensions 表）",
    dimensions_14_detail_desc: "展示维度定义、层级、权重和更新频率。",
    model_core_tables: "模型核心数据表",
    model_core_tables_desc: "完整展示你的14维模型结构化数据。",
    dimensions: "维度",
    inputs_latest: "Inputs（最新）",
    indicators: "指标",
    scores: "评分",
    alerts: "预警",
    workbook_explorer: "工作簿全量浏览",
    workbook_explorer_desc: "展示 Excel 每个工作表的全部行列原始数据。",
    need_today_note: "需要今日简报？",
    need_today_note_desc: "基于当前模型快照自动生成并编辑每日报告。",
    open_daily_report: "打开每日报告",
    daily_note: "每日情报",
    daily_report_title: "宏观监控每日报告",
    daily_report_desc: "根据最新仪表盘自动起草，可编辑后保存。",
    snapshot: "快照",
    regenerate_draft: "重新生成草稿",
    generate_final_report: "生成最终报告",
    save: "保存",
    download_txt: "下载 .txt",
    run_online_check: "生成最终报告前执行在线数据校验",
    report_preview: "报告预览（参考格式）",
    daily_report_archive: "每日报告归档",
    daily_report_archive_desc: "每一天报告都生成可访问链接。",
    online_check_results: "在线数据校验结果",
    detailed_indicator_scores: "指标详细评分",
    reference: "参考",
    glossary_title: "系统术语说明",
    glossary_desc: "与14维监控框架和预警阈值保持一致。",
    indicators_eyebrow: "指标信息库",
    indicators_page_title: "全部指标信息（来自 Indicators 表）",
    indicators_page_desc: "完整展示指标定义、评分参数、数据源和更新频率。",
    all_indicators_info: "全部指标信息",
    subscribe_title: "邮件订阅",
    subscribe_desc: "每日北京时间09:00生成报告后，向订阅邮箱发送摘要与报告链接。",
    subscribe_email_label: "邮箱",
    subscribe_submit: "订阅",
    subscribe_note: "若后端不可用，将自动回退到 GitHub 请求订阅。",
    subscribe_count: "当前有效订阅数"
  }
};

const sampleModel = {
  asOf: "2026-03-01",
  totalScore: 58.4,
  status: "Neutral Fragile",
  alerts: [{ id: "A03", level: "YELLOW", condition: "10Y-3M < -50bps", triggered: true }],
  dimensions: [
    { name: "Monetary Policy & Liquidity", score: 46.2, contribution: 5.5 },
    { name: "Growth & Forward Signals", score: 57.8, contribution: 6.4 },
    { name: "Inflation & Price Pressure", score: 61.3, contribution: 6.1 }
  ],
  drivers: [
    { title: "Primary Support", text: "Core inflation has moderated toward the target zone." },
    { title: "Primary Drag", text: "Yield curve inversion and liquidity tightness still cap risk appetite." },
    { title: "Risk Trigger", text: "A volatility spike can quickly shift the regime to defense." }
  ],
  tables: { dimensions: [], indicators: [], inputs: [], scores: [], alerts: [] },
  workbook: { sheets: [] },
  onlineCheck: []
};

const glossaryTerms = [
  {
    en: {
      term: "Macro Composite Score",
      desc: "Weighted 0-100 aggregate score across the 14 dimensions.",
      why: "It is the top-level regime signal used for strategic risk stance.",
      read: "75+ overheated expansion; 60-75 moderate expansion; 45-60 fragile neutral; 30-45 defensive; below 30 crisis.",
      use: "Use score trend (not one-point value) to size risk budgets."
    },
    zh: {
      term: "宏观综合评分",
      desc: "14个维度加权后的0-100综合分。",
      why: "它是用于战略风险配置的顶层状态信号。",
      read: "75以上偏热扩张；60-75温和扩张；45-60中性偏脆弱；30-45防御；30以下危机。",
      use: "建议看趋势而不是单日点位，用于调整组合风险预算。"
    }
  },
  {
    en: {
      term: "Alert Trigger",
      desc: "Threshold-based risk event (for example VIX > 30).",
      why: "Alerts provide tactical risk control signals that react faster than smoothed scores.",
      read: "RED means immediate stress, YELLOW means rising fragility, no trigger means thresholds are stable.",
      use: "Use alerts for hedging and gross/net exposure adjustments."
    },
    zh: {
      term: "预警触发",
      desc: "基于阈值触发的风险事件（例如 VIX > 30）。",
      why: "预警比平滑后的总分更快，适合战术风控。",
      read: "红灯代表即时压力，黄灯代表脆弱性上升，无触发代表阈值内稳定。",
      use: "可用于对冲比例和仓位暴露的快速调整。"
    }
  },
  {
    en: {
      term: "TargetBand",
      desc: "Scoring mode where a defined value range receives highest score.",
      why: "Many macro variables are healthiest in a middle range instead of monotonic higher/lower.",
      read: "Inside band = high score; outside band decays toward worst bounds.",
      use: "Typical for inflation, unemployment, and policy-sensitive variables."
    },
    zh: {
      term: "TargetBand",
      desc: "目标区间评分：数值落在区间内得分最高。",
      why: "许多宏观变量并非越高越好或越低越好，而是存在最优中枢。",
      read: "落在目标区间内高分，偏离后向最差边界线性衰减。",
      use: "常用于通胀、失业率、政策敏感型变量。"
    }
  },
  {
    en: {
      term: "WeightedContribution",
      desc: "Dimension contribution after dimension weight is applied.",
      why: "It identifies what truly drives headline score changes.",
      read: "High score with low weight can contribute less than medium score with high weight.",
      use: "Track contribution deltas to explain day-over-day regime shifts."
    },
    zh: {
      term: "加权贡献",
      desc: "维度分乘以维度权重后的总分贡献。",
      why: "它能定位真正推动总分变化的来源。",
      read: "低权重高分维度可能贡献小于高权重中等分维度。",
      use: "可用于解释日报中总分变动的核心原因。"
    }
  },
  {
    en: {
      term: "HY OAS",
      desc: "US high-yield option-adjusted spread, a credit stress gauge.",
      why: "It is a direct market-implied proxy for default and refinancing pressure.",
      read: "Widening spread usually means tighter financial conditions and weaker risk appetite.",
      use: "Use together with VIX/MOVE for cross-asset stress confirmation."
    },
    zh: {
      term: "HY OAS",
      desc: "美国高收益债 OAS，反映信用压力。",
      why: "它直接反映违约与再融资压力的市场定价。",
      read: "利差走阔通常意味着金融条件收紧、风险偏好下降。",
      use: "建议与VIX/MOVE联动观察确认跨资产压力。"
    }
  },
  {
    en: {
      term: "Net Liquidity Proxy",
      desc: "Common proxy: Fed assets - TGA - RRP.",
      why: "It approximates system liquidity available to risk assets.",
      read: "Rising proxy often supports risk assets; falling proxy can tighten market breadth.",
      use: "Compare with equity/credit drawdowns to detect liquidity-led risk events."
    },
    zh: {
      term: "净流动性代理",
      desc: "常用口径：美联储资产 - TGA - RRP。",
      why: "它近似反映可流向风险资产的系统流动性。",
      read: "上行通常支撑风险资产，下行可能对应广度收缩和估值压力。",
      use: "可与股债回撤联动判断是否为流动性主导的风险事件。"
    }
  }
];

function getLang() {
  const stored = localStorage.getItem(LANG_KEY);
  return stored === "zh" ? "zh" : "en";
}

function setLang(lang) {
  localStorage.setItem(LANG_KEY, lang);
  document.documentElement.lang = lang === "zh" ? "zh-CN" : "en";
}

function t(key) {
  const lang = getLang();
  return i18n[lang]?.[key] || i18n.en[key] || key;
}

function applyI18n() {
  document.querySelectorAll("[data-i18n]").forEach((node) => {
    const key = node.getAttribute("data-i18n");
    const value = t(key);
    if (value) node.textContent = value;
  });

  const toggle = document.getElementById("lang-toggle");
  if (toggle) toggle.textContent = getLang() === "zh" ? "EN" : "中文";
}

function getApiBase() {
  const fromStorage = asText(localStorage.getItem(API_BASE_KEY));
  if (fromStorage) return fromStorage.replace(/\/+$/, "");
  const meta = document.querySelector('meta[name="macro-api-base"]');
  const fromMeta = asText(meta?.content);
  if (fromMeta) return fromMeta.replace(/\/+$/, "");
  if (window.location.hostname === "localhost" || window.location.hostname === "127.0.0.1") return "http://127.0.0.1:5000";
  return "";
}

async function apiFetch(path, options = {}) {
  const base = getApiBase();
  if (!base) return null;
  const headers = { "Content-Type": "application/json", ...(options.headers || {}) };
  try {
    const res = await fetch(`${base}${path}`, { ...options, headers });
    if (!res.ok) return null;
    if (res.status === 204) return {};
    return await res.json();
  } catch {
    return null;
  }
}

function setupLangToggle(onChange) {
  const btn = document.getElementById("lang-toggle");
  if (!btn) return;

  btn.addEventListener("click", () => {
    const next = getLang() === "zh" ? "en" : "zh";
    setLang(next);
    applyI18n();
    if (onChange) onChange();
  });
}

function openDB() {
  return new Promise((resolve, reject) => {
    const request = indexedDB.open(DB_NAME, DB_VERSION);
    request.onupgradeneeded = () => {
      const db = request.result;
      if (!db.objectStoreNames.contains("model")) db.createObjectStore("model", { keyPath: "id" });
      if (!db.objectStoreNames.contains("reports")) db.createObjectStore("reports", { keyPath: "date" });
      if (!db.objectStoreNames.contains("checks")) db.createObjectStore("checks", { keyPath: "id" });
    };
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error);
  });
}

async function dbPut(store, value) {
  const db = await openDB();
  await new Promise((resolve, reject) => {
    const tx = db.transaction(store, "readwrite");
    tx.objectStore(store).put(value);
    tx.oncomplete = resolve;
    tx.onerror = () => reject(tx.error);
  });
  db.close();
}

async function dbGet(store, key) {
  const db = await openDB();
  const result = await new Promise((resolve, reject) => {
    const tx = db.transaction(store, "readonly");
    const req = tx.objectStore(store).get(key);
    req.onsuccess = () => resolve(req.result || null);
    req.onerror = () => reject(req.error);
  });
  db.close();
  return result;
}

async function dbGetAll(store) {
  const db = await openDB();
  const result = await new Promise((resolve, reject) => {
    const tx = db.transaction(store, "readonly");
    const req = tx.objectStore(store).getAll();
    req.onsuccess = () => resolve(req.result || []);
    req.onerror = () => reject(req.error);
  });
  db.close();
  return result;
}

function loadModelFallback() {
  const raw = localStorage.getItem(STORAGE_KEY);
  if (!raw) return sampleModel;
  try {
    return { ...sampleModel, ...JSON.parse(raw) };
  } catch {
    return sampleModel;
  }
}

function saveModelFallback(model) {
  localStorage.setItem(STORAGE_KEY, JSON.stringify(model));
}

async function loadCurrentModel() {
  const fromApi = await apiFetch("/api/model/current");
  if (fromApi) return normalizeModel({ ...sampleModel, ...fromApi });
  const snapshot = await loadStaticSnapshot();
  if (snapshot) return normalizeModel({ ...sampleModel, ...snapshot });
  const fromDb = await dbGet("model", "current");
  if (fromDb?.payload) return normalizeModel(fromDb.payload);
  return normalizeModel(loadModelFallback());
}

async function loadDashboardSummary() {
  const fromApi = await apiFetch("/api/model/summary");
  return fromApi && !fromApi.error ? fromApi : null;
}

async function saveCurrentModel(model) {
  const normalized = normalizeModel(model);
  await apiFetch("/api/model/current", { method: "POST", body: JSON.stringify(normalized) });
  saveModelFallback(normalized);
  await dbPut("model", { id: "current", payload: normalized, updatedAt: new Date().toISOString() });
}

async function saveReport(date, text, meta) {
  await apiFetch("/api/reports", {
    method: "POST",
    body: JSON.stringify({ date, text, meta, path: `reports/${date}.html` })
  });
  await dbPut("reports", { date, text, meta, updatedAt: new Date().toISOString() });
}

async function loadReport(date) {
  const fromApi = await apiFetch(`/api/reports/${encodeURIComponent(date)}`);
  if (fromApi) return fromApi;
  const local = await dbGet("reports", date);
  if (local) return local;
  const staticReports = await loadStaticReports();
  return staticReports.find((r) => r.date === date) || null;
}

async function listReports(limit = 400) {
  const safeLimit = Math.max(1, Math.min(Number(limit) || 400, 1000));
  const fromApi = await apiFetch(`/api/reports?limit=${safeLimit}`);
  if (Array.isArray(fromApi?.reports)) return fromApi.reports.sort((a, b) => b.date.localeCompare(a.date));
  const reports = await dbGetAll("reports");
  const staticReports = await loadStaticReports();
  const merged = new Map();
  [...staticReports, ...reports].forEach((r) => {
    if (r?.date) merged.set(r.date, r);
  });
  return [...merged.values()].sort((a, b) => b.date.localeCompare(a.date));
}

async function loadStaticReports() {
  try {
    const res = await fetch(STATIC_REPORT_INDEX, { cache: "no-cache" });
    if (!res.ok) return [];
    const payload = await res.json();
    return Array.isArray(payload?.reports) ? payload.reports : [];
  } catch {
    return [];
  }
}

async function loadStaticSnapshot() {
  try {
    const res = await fetch(STATIC_SNAPSHOT, { cache: "no-cache" });
    if (!res.ok) return null;
    return await res.json();
  } catch {
    return null;
  }
}

async function loadStaticSubscribers() {
  const fromApi = await apiFetch("/api/subscribers");
  if (Array.isArray(fromApi?.subscribers)) return fromApi.subscribers;
  try {
    const res = await fetch(STATIC_SUBSCRIBERS, { cache: "no-cache" });
    if (!res.ok) return [];
    const payload = await res.json();
    if (!Array.isArray(payload?.subscribers)) return [];
    return payload.subscribers.filter((s) => asText(s.status).toLowerCase() === "active");
  } catch {
    return [];
  }
}

async function migrateBrowserDataToServer() {
  const base = getApiBase();
  if (!base) return;
  if (localStorage.getItem(MIGRATED_KEY) === "1") return;
  const modelRow = await dbGet("model", "current");
  const reports = await dbGetAll("reports");
  const checks = await dbGetAll("checks");
  const hasAny = !!(modelRow?.payload || reports.length || checks.length);
  if (!hasAny) {
    localStorage.setItem(MIGRATED_KEY, "1");
    return;
  }
  const payload = {
    model: modelRow?.payload || null,
    reports,
    checks
  };
  const res = await apiFetch("/api/migrate", { method: "POST", body: JSON.stringify(payload) });
  if (res?.ok) localStorage.setItem(MIGRATED_KEY, "1");
}

function asText(v) {
  return v === undefined || v === null ? "" : String(v).trim();
}

function normalizeInputsTable(rows, fallbackAsOf = "") {
  const list = Array.isArray(rows) ? rows : [];
  if (!list.length) return [];
  if (Object.keys(list[0] || {}).some((k) => k.toLowerCase().includes("indicatorcode"))) return list;

  const firstKeys = Object.keys(list[0] || {});
  if (firstKeys.length !== 2) return list;

  const codeCol = firstKeys[0];
  const valueCol = firstKeys[1];
  const headerRow = list.find(
    (r) =>
      asText(r[codeCol]).toLowerCase() === "indicatorcode" &&
      asText(r[valueCol]).toLowerCase().includes("latestvalue")
  );
  if (!headerRow) return list;

  const asOfFromCol = /^\d{4}-\d{2}-\d{2}$/.test(asText(valueCol)) ? asText(valueCol) : "";
  const out = [];
  list.forEach((r) => {
    const code = asText(r[codeCol]);
    if (!/^[A-Z][A-Z0-9_]{1,40}$/i.test(code) || code.toLowerCase() === "indicatorcode") return;
    out.push({
      IndicatorCode: code,
      LatestValue: r[valueCol],
      ValueDate: asOfFromCol || fallbackAsOf
    });
  });
  return out.length ? out : list;
}

function normalizeModel(model) {
  const next = { ...model };
  next.tables = { ...(model.tables || {}) };
  next.tables.inputs = normalizeInputsTable(next.tables.inputs || [], next.asOf || "");
  return next;
}

function asNumber(v) {
  if (typeof v === "number") return Number.isFinite(v) ? v : null;
  const parsed = Number(String(v).replace(/,/g, ""));
  return Number.isFinite(parsed) ? parsed : null;
}

function round(v, d = 1) {
  const n = Number(v);
  if (!Number.isFinite(n)) return 0;
  const p = 10 ** d;
  return Math.round(n * p) / p;
}

function escapeHtml(input) {
  return asText(input)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function findSheet(workbook, candidates) {
  const names = workbook.SheetNames || [];
  const normalized = names.map((name) => ({ name, lower: name.toLowerCase() }));
  for (const c of candidates) {
    const hit = normalized.find((item) => item.lower.includes(c));
    if (hit) return hit.name;
  }
  return null;
}

function keyByIncludes(row, includes) {
  const keys = Object.keys(row || {});
  for (const key of keys) {
    const lower = key.toLowerCase();
    if (includes.some((s) => lower.includes(s))) return key;
  }
  return null;
}

function cleanSheetRows(rows) {
  const cleaned = (rows || []).map((row) => row.map((cell) => asText(cell)));
  const nonEmpty = cleaned.filter((row) => row.some((cell) => cell !== ""));
  if (!nonEmpty.length) return [];
  const maxCols = nonEmpty.reduce((acc, row) => {
    let right = 0;
    for (let i = row.length - 1; i >= 0; i -= 1) {
      if (row[i] !== "") {
        right = i + 1;
        break;
      }
    }
    return Math.max(acc, right);
  }, 0);
  return nonEmpty.map((row) => row.slice(0, maxCols));
}

function inferStatus(score) {
  if (score >= 75) return getLang() === "zh" ? "扩张偏热" : "Expansion Overheating";
  if (score >= 60) return getLang() === "zh" ? "温和扩张" : "Moderate Expansion";
  if (score >= 45) return getLang() === "zh" ? "中性偏脆弱" : "Neutral Fragile";
  if (score >= 30) return getLang() === "zh" ? "防御区" : "Defensive";
  return getLang() === "zh" ? "衰退/危机" : "Recession/Crisis";
}

function clamp(n, min, max) {
  return Math.max(min, Math.min(max, n));
}

function computeModelFromTables(tables) {
  const indicators = tables?.indicators || [];
  const inputs = tables?.inputs || [];
  const dimensionsTable = tables?.dimensions || [];

  const inputCodeKey = inputs.length ? keyByIncludes(inputs[0], ["indicatorcode", "code"]) : null;
  const inputValueKey = inputs.length ? keyByIncludes(inputs[0], ["latestvalue", "value", "值"]) : null;
  const inputMap = new Map();
  inputs.forEach((row) => {
    const code = asText(row[inputCodeKey]);
    const value = asNumber(row[inputValueKey]);
    if (code && value !== null) inputMap.set(code, value);
  });

  const dimNameMap = new Map();
  const dimWeightMap = new Map();
  dimensionsTable.forEach((row) => {
    const id = asText(findValue(row, ["dimensionid", "维度id", "id"]));
    if (!id) return;
    dimNameMap.set(id, asText(findValue(row, ["dimensionname", "维度名称", "维度"])));
    dimWeightMap.set(id, asNumber(findValue(row, ["weight", "权重", "%"])) ?? 0);
  });

  const indicatorScores = [];
  indicators.forEach((row) => {
    const code = asText(findValue(row, ["indicatorcode", "code"]));
    const dim = asText(findValue(row, ["dimensionid", "维度id", "id"]));
    if (!code || !dim) return;

    const raw = inputMap.get(code);
    if (raw === undefined || raw === null) return;

    const capLow = asNumber(findValue(row, ["caplow", "低截断"]));
    const capHigh = asNumber(findValue(row, ["caphigh", "高截断"]));
    let capped = raw;
    if (capLow !== null) capped = Math.max(capped, capLow);
    if (capHigh !== null) capped = Math.min(capped, capHigh);

    const scaleType = asText(findValue(row, ["scaletype"])).toLowerCase();
    const direction = asText(findValue(row, ["direction"])).toLowerCase();
    const best = asNumber(findValue(row, ["best"]));
    const worst = asNumber(findValue(row, ["worst"]));
    const worstLow = asNumber(findValue(row, ["worstlow"]));
    const targetLow = asNumber(findValue(row, ["targetlow"]));
    const targetHigh = asNumber(findValue(row, ["targethigh"]));
    const worstHigh = asNumber(findValue(row, ["worsthigh"]));
    const weightWithinDim = asNumber(findValue(row, ["weightwithindim", "权重"])) ?? 0;

    let score = null;
    if (scaleType.includes("targetband") && worstLow !== null && targetLow !== null && targetHigh !== null && worstHigh !== null) {
      if (capped >= targetLow && capped <= targetHigh) score = 100;
      else if (capped < targetLow) score = ((capped - worstLow) / (targetLow - worstLow)) * 100;
      else score = ((worstHigh - capped) / (worstHigh - targetHigh)) * 100;
    } else if (direction.includes("higher") && best !== null && worst !== null && best !== worst) {
      score = ((capped - worst) / (best - worst)) * 100;
    } else if (direction.includes("lower") && best !== null && worst !== null && worst !== best) {
      score = ((worst - capped) / (worst - best)) * 100;
    }
    if (score === null || !Number.isFinite(score)) return;
    score = clamp(score, 0, 100);

    indicatorScores.push({
      IndicatorCode: code,
      DimensionID: dim,
      IndicatorName: asText(findValue(row, ["indicatorname", "指标", "name"])),
      LatestValue: round(raw, 4),
      CappedValue: round(capped, 4),
      "Score(0-100)": round(score, 2),
      WeightWithinDim: round(weightWithinDim, 4),
      WeightedScore: round(score * weightWithinDim, 4)
    });
  });

  const dims = [...new Set(indicatorScores.map((x) => x.DimensionID))];
  const dimensionScores = dims.map((id) => {
    const rows = indicatorScores.filter((x) => x.DimensionID === id);
    const wsum = rows.reduce((a, r) => a + (asNumber(r.WeightWithinDim) ?? 0), 0);
    const weighted = rows.reduce((a, r) => a + (asNumber(r.WeightedScore) ?? 0), 0);
    const score = wsum > 0 ? weighted / wsum : 0;
    const dimWeight = dimWeightMap.get(id) ?? 0;
    const contribution = (score * dimWeight) / 100;
    return {
      id,
      name: dimNameMap.get(id) || id,
      score,
      contribution,
      dimWeight
    };
  });

  const totalScore = dimensionScores.reduce((a, d) => a + d.contribution, 0);
  return { indicatorScores, dimensionScores, totalScore };
}

function buildDefaultAlerts(inputRows) {
  const inputCodeKey = inputRows.length ? keyByIncludes(inputRows[0], ["indicatorcode", "code"]) : null;
  const inputValueKey = inputRows.length ? keyByIncludes(inputRows[0], ["latestvalue", "value", "值"]) : null;
  const valueOf = (code) => {
    const row = inputRows.find((r) => asText(r[inputCodeKey]) === code);
    return row ? asNumber(row[inputValueKey]) : null;
  };

  const checks = [
    { id: "A01", level: "RED", condition: "VIX > 30", triggered: (valueOf("VIX") ?? -Infinity) > 30 },
    { id: "A02", level: "RED", condition: "MOVE > 140", triggered: (valueOf("MOVE") ?? -Infinity) > 140 },
    { id: "A03", level: "YELLOW", condition: "HY OAS > 600bps", triggered: (valueOf("HY_OAS") ?? -Infinity) > 600 },
    { id: "A04", level: "YELLOW", condition: "10Y-3M < -50bps", triggered: (valueOf("YC_10Y3M") ?? Infinity) < -50 },
    { id: "A05", level: "YELLOW", condition: "Unemployment > 6%", triggered: (valueOf("UNRATE") ?? -Infinity) > 6 },
    { id: "A06", level: "YELLOW", condition: "Core PCE > 3.5%", triggered: (valueOf("CORE_PCE_YOY") ?? -Infinity) > 3.5 },
    { id: "A07", level: "YELLOW", condition: "WTI > 100", triggered: (valueOf("WTI") ?? -Infinity) > 100 }
  ];
  return checks;
}

function parseWorkbook(arrayBuffer) {
  const workbook = XLSX.read(arrayBuffer, { type: "array" });

  const sheetNames = {
    dimensions: findSheet(workbook, ["dimensions", "维度"]),
    indicators: findSheet(workbook, ["indicators", "指标"]),
    inputs: findSheet(workbook, ["inputs", "输入"]),
    scores: findSheet(workbook, ["scores", "分数"]),
    alerts: findSheet(workbook, ["alerts", "预警"])
  };

  const tables = {
    dimensions: sheetNames.dimensions ? XLSX.utils.sheet_to_json(workbook.Sheets[sheetNames.dimensions], { defval: "" }) : [],
    indicators: sheetNames.indicators ? XLSX.utils.sheet_to_json(workbook.Sheets[sheetNames.indicators], { defval: "" }) : [],
    inputs: sheetNames.inputs ? XLSX.utils.sheet_to_json(workbook.Sheets[sheetNames.inputs], { defval: "" }) : [],
    scores: sheetNames.scores ? XLSX.utils.sheet_to_json(workbook.Sheets[sheetNames.scores], { defval: "" }) : [],
    alerts: sheetNames.alerts ? XLSX.utils.sheet_to_json(workbook.Sheets[sheetNames.alerts], { defval: "" }) : []
  };

  const workbookSheets = (workbook.SheetNames || []).map((name) => {
    const rows = XLSX.utils.sheet_to_json(workbook.Sheets[name], { header: 1, defval: "", raw: false, blankrows: false });
    return { name, rows: cleanSheetRows(rows) };
  });

  let asOf = "";
  tables.inputs.forEach((row) => {
    if (asOf) return;
    const values = Object.values(row).map(asText);
    const dateLike = values.find((v) => /^\d{4}-\d{2}-\d{2}$/.test(v));
    if (dateLike) asOf = dateLike;
  });

  const computed = computeModelFromTables(tables);
  const dimensions = computed.dimensionScores
    .sort((a, b) => a.id.localeCompare(b.id, undefined, { numeric: true }))
    .map((d) => ({ name: d.name, score: round(d.score, 2), contribution: round(d.contribution, 4), id: d.id }));
  const totalScore = computed.totalScore || average(dimensions.map((d) => d.score));
  const alerts = buildDefaultAlerts(tables.inputs || []);

  tables.scores = computed.indicatorScores;

  const activeAlerts = alerts.filter((a) => a.triggered).length;
  const drivers = buildDrivers(dimensions, activeAlerts, totalScore);

  return normalizeModel({
    asOf: asOf || new Date().toISOString().slice(0, 10),
    totalScore: round(totalScore, 1),
    status: inferStatus(totalScore),
    alerts,
    dimensions,
    drivers,
    tables,
    workbook: { sheets: workbookSheets },
    onlineCheck: []
  });
}

function average(values) {
  if (!values.length) return sampleModel.totalScore;
  return values.reduce((acc, v) => acc + v, 0) / values.length;
}

function buildDrivers(dimensions, activeAlerts, totalScore) {
  const ranked = [...dimensions].sort((a, b) => b.score - a.score);
  const best = ranked[0];
  const worst = ranked[ranked.length - 1];
  const zh = getLang() === "zh";

  return [
    {
      title: zh ? "主要支撑" : "Primary Support",
      text: best
        ? zh
          ? `${best.name} 是当前最强维度（${round(best.score)}），对总分形成支撑。`
          : `${best.name} is the strongest block (${round(best.score)}), supporting the composite.`
        : zh
          ? "领先维度对总分形成中性支撑。"
          : "Leading dimensions are providing neutral support."
    },
    {
      title: zh ? "主要拖累" : "Primary Drag",
      text: worst
        ? zh
          ? `${worst.name} 是当前主要拖累项（${round(worst.score)}）。`
          : `${worst.name} remains the key drag (${round(worst.score)}).`
        : zh
          ? "弱势维度仍限制风险偏好。"
          : "Weaker dimensions still cap risk appetite."
    },
    {
      title: zh ? "风险触发" : "Risk Trigger",
      text: activeAlerts > 0
        ? zh
          ? `当前有 ${activeAlerts} 条预警触发，建议保持风控优先。`
          : `${activeAlerts} alert(s) are triggered. Keep risk controls tight.`
        : totalScore >= 60
          ? zh
            ? "当前无预警触发，策略可维持中性偏积极。"
            : "No active alerts; tactical stance can remain neutral-positive."
          : zh
            ? "当前无预警触发，但总分仍偏弱，建议均衡配置。"
            : "No active alerts, but score remains soft; keep positioning balanced."
    }
  ];
}

function objectRowsToColumns(rows) {
  const cols = new Set();
  (rows || []).forEach((row) => Object.keys(row || {}).forEach((k) => cols.add(k)));
  return [...cols];
}

function renderObjectTable(targetId, rows) {
  const root = document.getElementById(targetId);
  if (!root) return;

  const dataRows = Array.isArray(rows) ? rows : [];
  const columns = objectRowsToColumns(dataRows);
  if (!dataRows.length || !columns.length) {
    root.innerHTML = `<p class="table-empty">${getLang() === "zh" ? "暂无数据。" : "No data found."}</p>`;
    return;
  }

  const head = `<thead><tr>${columns.map((c) => `<th>${escapeHtml(c)}</th>`).join("")}</tr></thead>`;
  const body = `<tbody>${dataRows
    .map((row) => `<tr>${columns.map((c) => `<td>${escapeHtml(row[c])}</td>`).join("")}</tr>`)
    .join("")}</tbody>`;
  root.innerHTML = `<table class="data-table">${head}${body}</table>`;
}

function renderSheetTable(targetId, rows) {
  const root = document.getElementById(targetId);
  if (!root) return;

  if (!rows || !rows.length) {
    root.innerHTML = `<p class="table-empty">${getLang() === "zh" ? "该工作表无数据。" : "No rows in this sheet."}</p>`;
    return;
  }

  const maxCols = rows.reduce((m, row) => Math.max(m, row.length), 0);
  const first = rows[0] || [];
  const hasHeader = first.some((c) => asText(c));
  const headers = Array.from({ length: maxCols }, (_, i) => asText(first[i]) || `Col ${i + 1}`);
  const bodyRows = hasHeader ? rows.slice(1) : rows;

  const head = `<thead><tr>${headers.map((h) => `<th>${escapeHtml(h)}</th>`).join("")}</tr></thead>`;
  const body = `<tbody>${bodyRows
    .map(
      (row) =>
        `<tr>${Array.from({ length: maxCols }, (_, i) => `<td>${escapeHtml(row[i])}</td>`).join("")}</tr>`
    )
    .join("")}</tbody>`;

  root.innerHTML = `<table class="data-table">${head}${body}</table>`;
}

function renderWorkbookExplorer(workbook) {
  const tabs = document.getElementById("sheet-tabs");
  const table = document.getElementById("sheet-table");
  if (!tabs || !table) return;

  const sheets = workbook?.sheets || [];
  tabs.innerHTML = "";
  if (!sheets.length) {
    table.innerHTML = `<p class="table-empty">${getLang() === "zh" ? "请加载模型文件。" : "Load model file to view all sheets."}</p>`;
    return;
  }

  let active = 0;
  const renderActive = () => {
    renderSheetTable("sheet-table", sheets[active]?.rows || []);
    tabs.querySelectorAll(".sheet-tab").forEach((button, idx) => button.classList.toggle("active", idx === active));
  };

  sheets.forEach((sheet, idx) => {
    const btn = document.createElement("button");
    btn.type = "button";
    btn.className = "sheet-tab";
    btn.textContent = `${sheet.name} (${sheet.rows.length})`;
    btn.addEventListener("click", () => {
      active = idx;
      renderActive();
    });
    tabs.appendChild(btn);
  });

  renderActive();
}

function scoreTrend(score) {
  if (score >= 60) return { cls: "trend-up", symbol: "↑" };
  if (score < 45) return { cls: "trend-down", symbol: "↓" };
  return { cls: "trend-flat", symbol: "→" };
}

function parsePercentValue(value) {
  const text = asText(value).replace("%", "");
  const n = Number(text);
  return Number.isFinite(n) ? n : 0;
}

function findDimensionMetric(model, id, name) {
  const dim = (model.dimensions || []).find((item) => {
    const n = item.name.toLowerCase();
    return (id && n.includes(id.toLowerCase())) || (name && n.includes(name.toLowerCase()));
  });
  if (dim) return dim;

  const scoreRow = (model.tables?.scores || []).find((row) => {
    const rowId = findValue(row, ["dimensionid", "维度id", "id"]).toLowerCase();
    const rowName = findValue(row, ["dimensionname", "维度名称", "维度"]).toLowerCase();
    return rowId === id.toLowerCase() || rowName === name.toLowerCase();
  });
  if (!scoreRow) return { score: 0, contribution: 0 };

  return {
    score: asNumber(findValue(scoreRow, ["dimscore", "score", "维度分"])) || 0,
    contribution: asNumber(findValue(scoreRow, ["weightedcontribution", "contribution", "贡献"])) || 0
  };
}

function renderDimensionLayers(rows, model) {
  const root = document.getElementById("dimension-layers");
  if (!root) return;

  const list = (rows || [])
    .filter((row) => /^D\d{2}$/i.test(findValue(row, ["dimensionid", "维度id", "id"])))
    .sort((a, b) => {
      const ai = findValue(a, ["dimensionid", "维度id", "id"]);
      const bi = findValue(b, ["dimensionid", "维度id", "id"]);
      return ai.localeCompare(bi, undefined, { numeric: true });
    })
    .slice(0, 14);
  if (!list.length) {
    root.innerHTML = `<p class="table-empty">${getLang() === "zh" ? "Dimensions 表暂无数据。" : "No Dimensions data found."}</p>`;
    return;
  }

  const grouped = new Map();
  list.forEach((row) => {
    const tier = findValue(row, ["tier", "层级"]) || "Other";
    if (!grouped.has(tier)) grouped.set(tier, []);
    grouped.get(tier).push(row);
  });

  root.innerHTML = "";
  const payloadIndicators = Array.isArray(model.indicatorDetails) ? model.indicatorDetails : [];
  const tableIndicators = model.tables?.indicators || [];
  const tableInputs = model.tables?.inputs || [];
  const inputCodeKey = tableInputs.length ? keyByIncludes(tableInputs[0], ["indicatorcode", "code"]) : null;
  const inputValKey = tableInputs.length ? keyByIncludes(tableInputs[0], ["latestvalue", "value", "值"]) : null;

  for (const [tier, dims] of grouped.entries()) {
    const layer = document.createElement("section");
    layer.className = "layer-block";
    const totalWeight = dims.reduce((acc, row) => acc + parsePercentValue(findValue(row, ["weight", "权重", "%"])), 0);

    layer.innerHTML = `
      <div class="layer-header">
        <div class="layer-title">${escapeHtml(tier)}</div>
        <div class="layer-weight">${getLang() === "zh" ? "层级权重" : "Layer Weight"}: ${round(totalWeight, 1)}%</div>
      </div>
      <div class="layer-dimensions"></div>
    `;

    const container = layer.querySelector(".layer-dimensions");
    dims.forEach((row) => {
      const id = findValue(row, ["dimensionid", "维度id", "id"]);
      const name = findValue(row, ["dimensionname", "维度名称", "维度"]);
      const weight = findValue(row, ["weight", "权重", "%"]);
      const definition = findValue(row, ["definition", "说明", "定义"]);
      const update = findValue(row, ["typical update", "frequency", "更新"]);
      const metric = findDimensionMetric(model, id, name);
      const trend = scoreTrend(metric.score || 0);
      let dimIndicators = [];

      if (payloadIndicators.length) {
        dimIndicators = payloadIndicators
          .filter((x) => asText(x.DimensionID).toLowerCase() === asText(id).toLowerCase())
          .slice(0, 3)
          .map((x) => ({
            indicatorName: x.IndicatorName || x.IndicatorCode,
            value: x.LatestValue
          }));
      } else {
        dimIndicators = tableIndicators
          .filter((r) => findValue(r, ["dimensionid", "维度id", "id"]).toLowerCase() === id.toLowerCase())
          .slice(0, 3)
          .map((r) => {
            const code = findValue(r, ["indicatorcode", "code"]);
            const indicatorName = findValue(r, ["indicatorname", "指标", "name"]) || code;
            const input = tableInputs.find((x) => asText(x[inputCodeKey]) === code);
            const value = input ? asText(input[inputValKey]) : "";
            return { indicatorName, value };
          });
      }
      const indicatorList = dimIndicators
        .map((i) => `<li>${escapeHtml(i.indicatorName)}: ${escapeHtml(i.value ?? "--")}</li>`)
        .join("");

      const card = document.createElement("article");
      card.className = "dimension-card";
      card.innerHTML = `
        <h3>${escapeHtml(id)} ${escapeHtml(name)}</h3>
        <div class="dim-row">
          <span class="dim-chip">${getLang() === "zh" ? "权重" : "Weight"}: ${escapeHtml(weight)}</span>
          <span class="dim-chip">${getLang() === "zh" ? "更新" : "Update"}: ${escapeHtml(update)}</span>
        </div>
        <div class="score-line">
          <span class="score-pill">${round(metric.score || 0, 1)}/100</span>
          <span>${getLang() === "zh" ? "贡献" : "Contribution"}: ${round(metric.contribution || 0, 2)}</span>
          <span class="${trend.cls}">${trend.symbol}</span>
        </div>
        <ul class="preview-list">${indicatorList}</ul>
        <p>${escapeHtml(definition)}</p>
      `;
      container.appendChild(card);
    });
    root.appendChild(layer);
  }
}

function renderKeyIndicators(model) {
  const root = document.getElementById("key-indicators-grid");
  if (!root) return;

  if (Array.isArray(model.keyIndicatorsSnapshot) && model.keyIndicatorsSnapshot.length) {
    root.innerHTML = "";
    model.keyIndicatorsSnapshot.slice(0, 6).forEach((item) => {
      const card = document.createElement("article");
      card.className = "indicator-mini-card";
      card.innerHTML = `
        <h3>${escapeHtml(item.title || item.label || "--")}</h3>
        <div class="indicator-value">${escapeHtml(item.value ?? "--")}</div>
        <div class="indicator-source">${escapeHtml(item.source || "")}</div>
      `;
      root.appendChild(card);
    });
    return;
  }

  const indicators = model.tables?.indicators || [];
  const inputs = model.tables?.inputs || [];
  const inputCodeKey = inputs.length ? keyByIncludes(inputs[0], ["indicatorcode", "code"]) : null;
  const inputValueKey = inputs.length ? keyByIncludes(inputs[0], ["latestvalue", "value", "值"]) : null;

  const top = indicators.slice(0, 6).map((row) => {
    const code = findValue(row, ["indicatorcode", "code"]);
    const name = findValue(row, ["indicatorname", "指标", "name"]) || code;
    const source = findValue(row, ["source", "数据源", "主数据源"]);
    const input = inputs.find((x) => asText(x[inputCodeKey]) === code);
    const value = input ? asText(input[inputValueKey]) : "";
    return { code, name, source, value };
  });

  root.innerHTML = "";
  top.forEach((item) => {
    const card = document.createElement("article");
    card.className = "indicator-mini-card";
    card.innerHTML = `
      <h3>${escapeHtml(item.name)}</h3>
      <div class="indicator-value">${escapeHtml(item.value || "--")}</div>
      <div class="indicator-source">${escapeHtml(item.source || "")}</div>
    `;
    root.appendChild(card);
  });
}

function buildDimensionBundles(model) {
  const dims = (model.tables?.dimensions || [])
    .filter((row) => /^D\d{2}$/i.test(findValue(row, ["dimensionid", "维度id", "id"])))
    .sort((a, b) => {
      const ai = findValue(a, ["dimensionid", "维度id", "id"]);
      const bi = findValue(b, ["dimensionid", "维度id", "id"]);
      return ai.localeCompare(bi, undefined, { numeric: true });
    });

  const indicators = model.tables?.indicators || [];
  const inputs = model.tables?.inputs || [];
  const inputCodeKey = inputs.length ? keyByIncludes(inputs[0], ["indicatorcode", "code"]) : null;
  const inputValKey = inputs.length ? keyByIncludes(inputs[0], ["latestvalue", "value", "值"]) : null;

  return dims.map((dim) => {
    const id = findValue(dim, ["dimensionid", "维度id", "id"]);
    const name = findValue(dim, ["dimensionname", "维度名称", "维度"]);
    const tier = findValue(dim, ["tier", "层级"]);
    const weight = findValue(dim, ["weight", "权重", "%"]);
    const metric = findDimensionMetric(model, id, name);

    const dimIndicators = indicators
      .filter((row) => findValue(row, ["dimensionid", "维度id", "id"]).toLowerCase() === id.toLowerCase())
      .slice(0, 3)
      .map((row) => {
        const code = findValue(row, ["indicatorcode", "code"]);
        const indicatorName = findValue(row, ["indicatorname", "指标", "name"]) || code;
        const input = inputs.find((x) => asText(x[inputCodeKey]) === code);
        const value = input ? asText(input[inputValKey]) : "";
        return { indicatorName, value };
      });

    return { id, name, tier, weight, metric, indicators: dimIndicators };
  });
}

async function renderLatestReportSummary(model) {
  const root = document.getElementById("latest-report-summary");
  const watchRoot = document.getElementById("daily-watch-items");
  if (!root) return;

  const latest = model.latestReport || (await listReports(1))[0];
  const activeAlerts = (model.alerts || []).filter((a) => a.triggered);
  const weakDims = [...(model.dimensions || [])].sort((a, b) => a.score - b.score).slice(0, 3);

  root.innerHTML = `
    <div class="summary-score">${round(model.totalScore, 1)}/100 · ${escapeHtml(model.status)}</div>
    <div class="summary-line">${getLang() === "zh" ? "模型更新日" : "Model As-Of"}: ${escapeHtml(model.asOf)}</div>
    <div class="summary-line">${getLang() === "zh" ? "最新报告日期" : "Latest Report Date"}: ${escapeHtml(latest?.date || "--")}</div>
    <div class="summary-line">${getLang() === "zh" ? "简要结论" : "Short Summary"}: ${escapeHtml(
      model.latestReportSummary ||
        (getLang() === "zh"
          ? `当前处于${escapeHtml(model.status)}，重点关注${weakDims.map((d) => d.name).join(" / ")}。`
          : `Current regime is ${escapeHtml(model.status)}; watch ${weakDims.map((d) => d.name).join(" / ")}.`)
    )}</div>
  `;

  if (!watchRoot) return;
  watchRoot.innerHTML = "";
  const snapshotWatch = (model.dailyWatchedItems || model.keyWatch || []).map((x) =>
    typeof x === "string" ? x : `${x.label || x.title}: ${x.value ?? x.text ?? ""}`
  );
  const items = snapshotWatch.length
    ? snapshotWatch
    : [...activeAlerts.map((a) => `${a.id}: ${a.condition}`), ...weakDims.map((d) => `${d.name}: ${round(d.score, 1)}`)];
  if (!items.length) items.push(getLang() === "zh" ? "暂无重点关注项。" : "No urgent watch items.");

  items.slice(0, 6).forEach((text) => {
    const link = document.createElement("div");
    link.className = "report-link";
    link.textContent = text;
    watchRoot.appendChild(link);
  });
}

function groupByTier(bundles) {
  const groups = new Map();
  bundles.forEach((bundle) => {
    if (!groups.has(bundle.tier)) groups.set(bundle.tier, []);
    groups.get(bundle.tier).push(bundle);
  });
  return groups;
}

function renderDailyReportPreview(model, date) {
  const root = document.getElementById("report-preview");
  if (!root) return;

  const bundles = buildDimensionBundles(model);
  const tierGroups = groupByTier(bundles);
  const topWatch = [...(model.dimensions || [])].sort((a, b) => a.score - b.score).slice(0, 5);

  root.innerHTML = `
    <section class="preview-header">
      <h3>${getLang() === "zh" ? `14维宏观监控模型报告 (${date})` : `14-Dimension Macro Report (${date})`}</h3>
      <div>${getLang() === "zh" ? "综合评分" : "Composite Score"}: ${round(model.totalScore, 1)}/100</div>
      <div>${getLang() === "zh" ? "投资信号" : "Signal"}: ${escapeHtml(model.status)}</div>
    </section>
    <section class="preview-section">
      <h3>${getLang() === "zh" ? "核心结论" : "Core Conclusion"}</h3>
      <ul class="preview-list">
        ${topWatch.map((d) => `<li>${escapeHtml(d.name)}: ${round(d.score, 1)}</li>`).join("")}
      </ul>
    </section>
  `;

  for (const [tier, dims] of tierGroups.entries()) {
    const tierNode = document.createElement("section");
    tierNode.className = "preview-tier";
    tierNode.innerHTML = `<h3>${escapeHtml(tier)}</h3>`;

    dims.forEach((dim) => {
      const trend = scoreTrend(dim.metric.score || 0);
      const card = document.createElement("div");
      card.className = "preview-dim-card";
      card.innerHTML = `
        <strong>${escapeHtml(dim.id)} ${escapeHtml(dim.name)}</strong>
        <div>${getLang() === "zh" ? "权重" : "Weight"} ${escapeHtml(dim.weight)} | ${getLang() === "zh" ? "评分" : "Score"} ${round(dim.metric.score, 1)} ${trend.symbol} | ${getLang() === "zh" ? "贡献" : "Contribution"} ${round(dim.metric.contribution, 2)}</div>
        <ul class="preview-list">
          ${dim.indicators.map((i) => `<li>${escapeHtml(i.indicatorName)}: ${escapeHtml(i.value || "--")}</li>`).join("")}
        </ul>
      `;
      tierNode.appendChild(card);
    });
    root.appendChild(tierNode);
  }
}

function findValue(row, patterns) {
  const key = Object.keys(row || {}).find((k) => patterns.some((p) => k.toLowerCase().includes(p)));
  return key ? asText(row[key]) : "";
}

function renderDashboard(model) {
  const score = document.getElementById("total-score");
  const asOf = document.getElementById("as-of");
  const status = document.getElementById("macro-status");
  const alertList = document.getElementById("alert-list");
  const bars = document.getElementById("dimension-bars");
  const drivers = document.getElementById("drivers");
  if (!score) return;

  score.textContent = round(model.totalScore, 1).toFixed(1);
  asOf.textContent = model.asOf;
  status.textContent = model.status;

  const active = (model.triggerAlerts || model.alerts || []).filter((a) => a.triggered);
  alertList.innerHTML = "";
  if (!active.length) {
    const li = document.createElement("li");
    li.className = "alert-item none";
    li.textContent = getLang() === "zh" ? "当前无触发预警。" : "No active alerts.";
    alertList.appendChild(li);
  } else {
    active.forEach((alert) => {
      const li = document.createElement("li");
      li.className = `alert-item ${alert.level.toLowerCase()}`;
      li.innerHTML = `<strong>${escapeHtml(alert.id)} (${escapeHtml(alert.level)})</strong><br>${escapeHtml(alert.condition)}`;
      alertList.appendChild(li);
    });
  }

  bars.innerHTML = "";
  const topContributors = Array.isArray(model.topDimensionContributors) && model.topDimensionContributors.length
    ? model.topDimensionContributors
    : [...(model.dimensions || [])].sort((a, b) => (b.contribution || 0) - (a.contribution || 0));

  [...topContributors]
    .forEach((item) => {
      const row = document.createElement("div");
      row.className = "bar-row";
      const width = Math.max(0, Math.min(100, Number(item.score) || 0));
      row.innerHTML = `
        <div class="bar-top"><span>${escapeHtml(item.name)}</span><span>${round(item.score, 1)}</span></div>
        <div class="bar-track"><div class="bar-fill" style="width:${width}%"></div></div>
      `;
      bars.appendChild(row);
    });

  drivers.innerHTML = "";
  (model.primaryDrivers || model.drivers || []).forEach((item) => {
    const card = document.createElement("article");
    card.className = "driver-card";
    card.innerHTML = `<h3>${escapeHtml(item.title || "")}</h3><p>${escapeHtml(item.text || item.summary || "")}</p>`;
    drivers.appendChild(card);
  });

  renderKeyIndicators(model);
  renderLatestReportSummary(model);
  scheduleHeavyDashboardRender(model);
}

function scheduleHeavyDashboardRender(model) {
  dashboardHeavyRenderToken += 1;
  const token = dashboardHeavyRenderToken;
  const task = () => {
    if (token !== dashboardHeavyRenderToken) return;
    renderDimensionLayers(model.all14DimensionsDetailed || model.tables?.dimensions || [], model);
    renderObjectTable("dimensions-table", model.tables?.dimensions || []);
    renderObjectTable("inputs-table", model.tables?.inputs || []);
    renderObjectTable("indicators-table", model.tables?.indicators || []);
    renderObjectTable("scores-table", model.tables?.scores || []);
    renderObjectTable("alerts-table", model.tables?.alerts || []);
    renderWorkbookExplorer(model.workbook || {});
  };
  if (typeof window.requestIdleCallback === "function") {
    window.requestIdleCallback(task, { timeout: 900 });
    return;
  }
  window.setTimeout(task, 60);
}

function renderDashboardSummary(summary) {
  if (!summary || summary.error) return;
  const score = document.getElementById("total-score");
  const asOf = document.getElementById("as-of");
  const status = document.getElementById("macro-status");
  const alertList = document.getElementById("alert-list");
  const bars = document.getElementById("dimension-bars");
  const drivers = document.getElementById("drivers");
  const keyGrid = document.getElementById("key-indicators-grid");
  const layers = document.getElementById("dimension-layers");
  if (score) score.textContent = round(summary.totalScore, 1).toFixed(1);
  if (asOf) asOf.textContent = summary.asOf || "--";
  if (status) status.textContent = summary.status || "--";

  if (alertList) {
    const active = (summary.alerts || []).filter((a) => a.triggered);
    alertList.innerHTML = "";
    if (!active.length) {
      const li = document.createElement("li");
      li.className = "alert-item none";
      li.textContent = getLang() === "zh" ? "当前无触发预警。" : "No active alerts.";
      alertList.appendChild(li);
    } else {
      active.slice(0, 6).forEach((alert) => {
        const li = document.createElement("li");
        li.className = `alert-item ${(alert.level || "").toLowerCase()}`;
        li.innerHTML = `<strong>${escapeHtml(alert.id || "--")} (${escapeHtml(alert.level || "--")})</strong><br>${escapeHtml(alert.condition || "")}`;
        alertList.appendChild(li);
      });
    }
  }

  if (bars) {
    bars.innerHTML = "";
    (summary.topDimensionContributors || []).slice(0, 8).forEach((item) => {
      const row = document.createElement("div");
      row.className = "bar-row";
      const width = Math.max(0, Math.min(100, Number(item.score) || 0));
      row.innerHTML = `
        <div class="bar-top"><span>${escapeHtml(item.name || "--")}</span><span>${round(item.score, 1)}</span></div>
        <div class="bar-track"><div class="bar-fill" style="width:${width}%"></div></div>
      `;
      bars.appendChild(row);
    });
  }

  if (drivers) {
    drivers.innerHTML = "";
    (summary.primaryDrivers || []).slice(0, 3).forEach((item) => {
      const card = document.createElement("article");
      card.className = "driver-card";
      card.innerHTML = `<h3>${escapeHtml(item.title || "")}</h3><p>${escapeHtml(item.text || item.summary || "")}</p>`;
      drivers.appendChild(card);
    });
  }

  if (keyGrid && Array.isArray(summary.keyIndicatorsSnapshot) && summary.keyIndicatorsSnapshot.length) {
    keyGrid.innerHTML = "";
    summary.keyIndicatorsSnapshot.slice(0, 6).forEach((item) => {
      const card = document.createElement("article");
      card.className = "indicator-mini-card";
      card.innerHTML = `
        <h3>${escapeHtml(item.title || item.label || "--")}</h3>
        <div class="indicator-value">${escapeHtml(item.value ?? "--")}</div>
        <div class="indicator-source">${escapeHtml(item.source || "")}</div>
      `;
      keyGrid.appendChild(card);
    });
  }

  if (layers) {
    layers.innerHTML = `<p class="table-empty">${getLang() === "zh" ? "正在加载14维明细..." : "Loading 14-dimension details..."}</p>`;
  }
  renderLatestReportSummary({
    ...summary,
    latestReport: summary.latestReportDate
      ? {
          date: summary.latestReportDate,
          meta: { summary: summary.latestReportSummary || "" }
        }
      : null
  });
}

function isValidEmail(email) {
  return /^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$/i.test(asText(email));
}

function openGithubSubscriptionFallback(email) {
  const now = new Date().toISOString();
  const title = `Subscription Request: ${email}`;
  const body = [
    "Please add this email to the daily macro report mailing list.",
    "",
    `Email: ${email}`,
    `SubmittedAt: ${now}`
  ].join("\n");
  const link = `${SUBSCRIPTION_ISSUE_URL}?labels=subscription&title=${encodeURIComponent(title)}&body=${encodeURIComponent(body)}`;
  window.open(link, "_blank", "noopener,noreferrer");
}

async function setupSubscriptionForm() {
  const form = document.getElementById("subscribe-form");
  const emailInput = document.getElementById("subscribe-email");
  const status = document.getElementById("subscribe-status");
  const count = document.getElementById("subscriber-count");
  if (!form || !emailInput) return;

  const subs = await loadStaticSubscribers();
  if (count) count.textContent = `${t("subscribe_count")}: ${subs.length}`;

  if (form.dataset.bound === "1") return;
  form.dataset.bound = "1";

  form.addEventListener("submit", async (event) => {
    event.preventDefault();
    const email = asText(emailInput.value).toLowerCase();
    if (!isValidEmail(email)) {
      if (status) status.textContent = getLang() === "zh" ? "请输入有效邮箱地址。" : "Please enter a valid email address.";
      return;
    }
    const base = getApiBase();
    if (base) {
      const res = await apiFetch("/api/subscribers", { method: "POST", body: JSON.stringify({ email }) });
      if (res?.ok) {
        const list = await loadStaticSubscribers();
        if (count) count.textContent = `${t("subscribe_count")}: ${list.length}`;
        if (status) status.textContent = getLang() === "zh" ? "订阅成功，已加入邮件列表。" : "Subscribed successfully.";
        emailInput.value = "";
        return;
      }
    }

    openGithubSubscriptionFallback(email);
    if (status) {
      status.textContent =
        getLang() === "zh"
          ? "后端暂不可用，已跳转到 GitHub 订阅请求页，请提交后完成订阅。"
          : "Backend unavailable. Redirected to GitHub subscription request page; submit it to complete subscription.";
    }
    emailInput.value = "";
  });
}

function getReportDateFromUrl() {
  const params = new URLSearchParams(window.location.search);
  const d = params.get("date");
  return /^\d{4}-\d{2}-\d{2}$/.test(asText(d)) ? d : new Date().toISOString().slice(0, 10);
}

function updateUrlDate(date) {
  const params = new URLSearchParams(window.location.search);
  params.set("date", date);
  const query = params.toString();
  const next = `${window.location.pathname}${query ? `?${query}` : ""}`;
  history.replaceState({}, "", next);
}

function generateDailyText(model, date, onlineSummary) {
  const top = [...(model.dimensions || [])].sort((a, b) => b.score - a.score).slice(0, 3);
  const bottom = [...(model.dimensions || [])].sort((a, b) => a.score - b.score).slice(0, 3);
  const active = (model.alerts || []).filter((a) => a.triggered);
  const zh = getLang() === "zh";

  const lines = [
    zh ? "宏观监控每日报告" : "Macro Monitoring Daily Report",
    `${zh ? "报告日期" : "Date"}: ${date}`,
    `${zh ? "模型更新日" : "Model As-Of"}: ${model.asOf}`,
    `${zh ? "综合得分" : "Composite Score"}: ${round(model.totalScore, 1)} (${model.status})`,
    "",
    zh ? "执行摘要" : "Executive Summary",
    zh
      ? `- 当前模型处于“${model.status}”状态，总分 ${round(model.totalScore, 1)}。`
      : `- Current regime: ${model.status} with total score ${round(model.totalScore, 1)}.`,
    zh
      ? `- 当前触发预警 ${active.length} 条。`
      : `- ${active.length} alert(s) are currently triggered.`
  ];

  if (onlineSummary) {
    lines.push(
      zh
        ? `- 在线数据校验：检查 ${onlineSummary.checked} 项，更新 ${onlineSummary.updated} 项，失败 ${onlineSummary.failed} 项。`
        : `- Online data check: checked ${onlineSummary.checked}, updated ${onlineSummary.updated}, failed ${onlineSummary.failed}.`
    );
  }

  lines.push("", zh ? "主要支撑维度" : "Top Supporting Dimensions");
  top.forEach((item) => lines.push(`- ${item.name}: ${round(item.score, 1)}`));

  lines.push("", zh ? "主要拖累维度" : "Top Dragging Dimensions");
  bottom.forEach((item) => lines.push(`- ${item.name}: ${round(item.score, 1)}`));

  lines.push("", zh ? "预警清单" : "Alert Watchlist");
  if (!active.length) {
    lines.push(zh ? "- 今日无触发预警。" : "- No active alerts today.");
  } else {
    active.forEach((a) => lines.push(`- ${a.id} (${a.level}): ${a.condition}`));
  }

  lines.push("", zh ? "详细指标分数请见页面下方表格。" : "See the detailed indicator score table below on this page.");
  return lines.join("\n");
}

function extractSingleSeriesCode(raw) {
  const text = asText(raw);
  if (!text) return "";
  const match = text.match(/\b[A-Z][A-Z0-9_]{1,20}\b/);
  return match ? match[0] : "";
}

async function fetchFredLatestValue(seriesCode) {
  const url = `https://fred.stlouisfed.org/graph/fredgraph.csv?id=${encodeURIComponent(seriesCode)}`;
  const response = await fetch(url);
  if (!response.ok) throw new Error(`HTTP ${response.status}`);
  const csv = await response.text();
  const lines = csv.split(/\r?\n/).slice(1).filter(Boolean);
  for (let i = lines.length - 1; i >= 0; i -= 1) {
    const [date, value] = lines[i].split(",");
    if (value && value !== ".") return { date, value: Number(value) };
  }
  throw new Error("No valid value");
}

function findInputRowByCode(inputs, code) {
  const codeKey = inputs.length ? keyByIncludes(inputs[0], ["indicatorcode", "code"]) : null;
  if (!codeKey) return null;
  return inputs.find((row) => asText(row[codeKey]) === code) || null;
}

async function runOnlineDataCheck(model) {
  const indicators = model.tables?.indicators || [];
  const inputs = model.tables?.inputs || [];
  const results = [];

  const sourceKey = indicators.length ? keyByIncludes(indicators[0], ["sourceurl", "source", "数据源"]) : null;
  const seriesKey = indicators.length ? keyByIncludes(indicators[0], ["series/code", "series", "code", "建议系列"]) : null;
  const indCodeKey = indicators.length ? keyByIncludes(indicators[0], ["indicatorcode", "code"]) : null;
  const valueKey = inputs.length ? keyByIncludes(inputs[0], ["latestvalue", "value", "值"]) : null;
  const valueDateKey = inputs.length ? keyByIncludes(inputs[0], ["valuedate", "date", "日期"]) : null;

  let checked = 0;
  let updated = 0;
  let failed = 0;

  for (const row of indicators.slice(0, 60)) {
    const source = asText(row[sourceKey]).toLowerCase();
    const code = extractSingleSeriesCode(row[seriesKey]);
    const indicatorCode = asText(row[indCodeKey]) || code;
    if (!code) continue;

    const shouldCheck = source.includes("fred") || /^([A-Z]{2,}|[A-Z0-9_]+)$/.test(code);
    if (!shouldCheck) continue;

    checked += 1;
    try {
      const latest = await fetchFredLatestValue(code);
      const inputRow = findInputRowByCode(inputs, indicatorCode) || findInputRowByCode(inputs, code);
      let changed = false;

      if (inputRow && valueKey) {
        const oldValue = asNumber(inputRow[valueKey]);
        if (oldValue === null || Math.abs(oldValue - latest.value) > 1e-9) {
          inputRow[valueKey] = round(latest.value, 4);
          changed = true;
        }
      }
      if (inputRow && valueDateKey) inputRow[valueDateKey] = latest.date;
      if (changed) updated += 1;

      results.push({ indicator: indicatorCode, source: "FRED", series: code, status: changed ? "UPDATED" : "UNCHANGED", latestDate: latest.date, latestValue: latest.value });
    } catch (err) {
      failed += 1;
      results.push({ indicator: indicatorCode, source: "FRED", series: code, status: "FAILED", latestDate: "", latestValue: "", error: asText(err.message) });
    }
  }

  const checkedAt = new Date().toISOString();
  await dbPut("checks", { id: checkedAt, checkedAt, results });
  await apiFetch("/api/checks", {
    method: "POST",
    body: JSON.stringify({ checkedAt, summary: { checked, updated, failed }, rows: results })
  });

  return {
    checked,
    updated,
    failed,
    checkedAt,
    results,
    model: {
      ...model,
      tables: {
        ...model.tables,
        inputs
      },
      onlineCheck: results
    }
  };
}

function renderReportLinks(reports) {
  const root = document.getElementById("report-links");
  if (!root) return;

  if (!reports.length) {
    root.innerHTML = `<p class="table-empty">${getLang() === "zh" ? "暂无已保存报告。" : "No saved reports yet."}</p>`;
    return;
  }

  root.innerHTML = "";
  reports.forEach((report) => {
    const item = document.createElement("article");
    item.className = "report-item";
    const scoreLabel = getLang() === "zh" ? "综合评分" : "Score";
    const signalLabel = getLang() === "zh" ? "信号" : "Signal";
    item.innerHTML = `
      <div class="report-item-main">
        <div class="report-date">${escapeHtml(report.date)}</div>
        <div class="badge-row">
          <span class="badge">${scoreLabel}: ${escapeHtml(report.meta?.score ?? "--")}</span>
          <span class="badge">${signalLabel}: ${escapeHtml(report.meta?.status ?? "--")}</span>
        </div>
      </div>
      <a class="report-open" href="${escapeHtml(report.path || `daily-report.html?date=${encodeURIComponent(report.date)}`)}">${getLang() === "zh" ? "打开" : "Open"}</a>
    `;
    root.appendChild(item);
  });
}

function renderOnlineCheckTable(rows) {
  const mapped = (rows || []).map((row) => ({
    Indicator: row.indicator,
    Source: row.source,
    Series: row.series,
    Status: row.status,
    LatestDate: row.latestDate,
    LatestValue: row.latestValue,
    Error: row.error || ""
  }));
  renderObjectTable("online-check-table", mapped);
}

function renderIndicatorVerificationTable(rows) {
  const mapped = (rows || []).map((r) => ({
    IndicatorCode: r.IndicatorCode,
    IndicatorName: r.IndicatorName,
    DimensionID: r.DimensionID,
    LatestValue: r.LatestValue,
    ValueDate: r.ValueDate,
    SourceDate: r.SourceDate,
    VerifiedOnline: r.VerifiedOnline ? "YES" : "NO",
    VerificationStatus: r.VerificationStatus,
    VerificationError: r.VerificationError || "",
    GeneratedAt: r.GeneratedAt || ""
  }));
  renderObjectTable("indicator-verification-table", mapped);
}

async function renderDailyReport(model) {
  const date = getReportDateFromUrl();
  updateUrlDate(date);

  const scoreEl = document.getElementById("report-score");
  const dateEl = document.getElementById("report-date");
  const statusEl = document.getElementById("report-status");
  const editor = document.getElementById("report-editor");
  const saveStatus = document.getElementById("save-status");
  const regenBtn = document.getElementById("generate-report");
  const finalBtn = document.getElementById("finalize-report");
  const saveBtn = document.getElementById("save-report");
  const downloadBtn = document.getElementById("download-report");
  const runCheckBox = document.getElementById("run-online-check");

  if (!editor) return;

  scoreEl.textContent = round(model.totalScore, 1).toFixed(1);
  dateEl.textContent = date;
  statusEl.textContent = model.status;

  let existing = await loadReport(date);
  if (!existing) {
    const allReports = await listReports();
    if (allReports.length) {
      existing = allReports[0];
    }
  }
  const payload = existing?.reportPayload || {};
  const viewModel = {
    ...model,
    ...payload
  };
  const initial = existing?.text || generateDailyText(model, date, null);
  editor.value = initial;
  renderDailyReportPreview(viewModel, date);

  renderObjectTable("daily-scores-table", viewModel.tables?.scores || model.tables?.scores || []);
  renderOnlineCheckTable(viewModel.onlineCheck || model.onlineCheck || []);
  renderIndicatorVerificationTable(viewModel.indicatorDetails || model.indicatorDetails || []);
  const generatedAtEl = document.getElementById("data-generated-at");
  if (generatedAtEl) {
    const ga = viewModel.generatedAt || model.generatedAt;
    generatedAtEl.textContent = ga
      ? `${getLang() === "zh" ? "Data Generated At" : "Data Generated At"}: ${ga}`
      : `${getLang() === "zh" ? "Data Generated At" : "Data Generated At"}: --`;
  }
  renderReportLinks(await listReports());

  regenBtn?.addEventListener("click", () => {
    editor.value = generateDailyText(model, date, null);
    renderDailyReportPreview(viewModel, date);
    saveStatus.textContent = getLang() === "zh" ? "草稿已重新生成。" : "Draft regenerated.";
  });

  finalBtn?.addEventListener("click", async () => {
    let targetModel = model;
    let summary = null;

    if (runCheckBox?.checked) {
      saveStatus.textContent = getLang() === "zh" ? "正在执行在线数据校验..." : "Running online data check...";
      const checked = await runOnlineDataCheck(targetModel);
      targetModel = checked.model;
      summary = { checked: checked.checked, updated: checked.updated, failed: checked.failed };
      await saveCurrentModel(targetModel);
      renderOnlineCheckTable(checked.results);
      renderObjectTable("daily-scores-table", targetModel.tables?.scores || []);
      renderIndicatorVerificationTable(targetModel.indicatorDetails || []);
    }

    editor.value = generateDailyText(targetModel, date, summary);
    renderDailyReportPreview(targetModel, date);
    await saveReport(date, editor.value, { score: round(targetModel.totalScore, 1), status: targetModel.status });
    renderReportLinks(await listReports());
    saveStatus.textContent = getLang() === "zh" ? "最终报告已生成并保存。" : "Final report generated and saved.";
  });

  saveBtn?.addEventListener("click", async () => {
    await saveReport(date, editor.value, { score: round(model.totalScore, 1), status: model.status });
    renderReportLinks(await listReports());
    saveStatus.textContent = getLang() === "zh" ? "已保存。" : "Saved.";
  });

  downloadBtn?.addEventListener("click", () => {
    const blob = new Blob([editor.value], { type: "text/plain;charset=utf-8" });
    const link = document.createElement("a");
    link.href = URL.createObjectURL(blob);
    link.download = `macro-daily-report-${date}.txt`;
    document.body.appendChild(link);
    link.click();
    link.remove();
    URL.revokeObjectURL(link.href);
  });
}

function mapTierToCategory(tier) {
  const t = asText(tier).toLowerCase();
  if (t.includes("core")) return "core-macro";
  if (t.includes("policy") || t.includes("external") || t.includes("soft")) return "policy-external";
  if (t.includes("market") || t.includes("shock")) return "market-mapping";
  if (t.includes("theme")) return "theme-panel";
  return "core-macro";
}

function referenceGlossaryCatalog() {
  const zh = getLang() === "zh";
  return [
    {
      category: "core-macro",
      title: zh ? "D01 货币政策与流动性" : "D01 Monetary Policy & Liquidity",
      weight: "12%",
      definition: zh ? "利率曲线、短端资金利率、央行资产负债表与净流动性。" : "Yield curve, short-end funding, central bank balance sheet, and net liquidity.",
      details: zh ? "关键指标: 10Y-3M利差、SOFR、美联储总资产、净流动性代理" : "Key indicators: 10Y-3M spread, SOFR, Fed assets, net liquidity proxy",
      why: zh ? "用于判断流动性环境和政策方向是否支持风险资产。" : "Measures whether policy/liquidity backdrop supports risk assets.",
      read: zh ? "曲线修复、资金利率稳定、净流动性改善通常对应更高风险偏好。" : "Curve normalization and improving net liquidity are usually risk-supportive.",
      use: zh ? "在日报中作为市场风格切换的前置信号。" : "Use as a lead signal for market regime/style shifts."
    },
    {
      category: "core-macro",
      title: zh ? "D02 增长与前瞻" : "D02 Growth & Forward Signals",
      weight: "11%",
      definition: zh ? "GDP/PMI/订单/初请等前瞻增长信号。" : "Forward growth indicators such as GDP, PMI, orders, and claims.",
      details: zh ? "关键指标: 实际GDP、制造业PMI、初请失业金4周均值、LEI同比" : "Key indicators: real GDP, PMI, claims 4WMA, LEI YoY",
      why: zh ? "用于跟踪经济动能是否放缓或再加速。" : "Tracks macro momentum deceleration vs re-acceleration.",
      read: zh ? "前瞻项恶化往往早于盈利和就业回落。" : "Forward indicators typically lead earnings and labor turns.",
      use: zh ? "结合D05盈利维度用于判断景气持续性。" : "Pair with D05 to assess cycle sustainability."
    },
    {
      category: "core-macro",
      title: zh ? "D03 通胀与价格压力" : "D03 Inflation & Price Pressure",
      weight: "10%",
      definition: zh ? "核心通胀与通胀预期，判断政策约束与利润压力。" : "Core inflation and inflation expectations to infer policy/profit pressure.",
      details: zh ? "关键指标: 核心CPI、核心PCE、5Y5Y通胀预期" : "Key indicators: core CPI, core PCE, 5Y5Y inflation expectation",
      why: zh ? "决定利率路径、估值折现和利润空间。" : "Drives rates path, valuation discounting, and margin pressure.",
      read: zh ? "通胀回落至目标区间通常利好风险资产估值。" : "Disinflation toward target is generally supportive for valuations.",
      use: zh ? "与D01联动判断政策松紧边际变化。" : "Use with D01 to track policy tightening/easing margins."
    },
    {
      category: "core-macro",
      title: zh ? "D04 就业与居民部门" : "D04 Labor & Households",
      weight: "10%",
      definition: zh ? "就业、收入与居民信用压力。" : "Labor, income, and household credit stress.",
      details: zh ? "关键指标: 失业率、工资增速、信用卡拖欠率、实际可支配收入同比" : "Key indicators: unemployment, wage growth, card delinquency, real disposable income YoY",
      why: zh ? "居民部门是消费和经济韧性的核心。" : "Households drive consumption and macro resilience.",
      read: zh ? "失业恶化+拖欠上行通常对应防御风格上升。" : "Rising unemployment and delinquencies usually favor defensive posture.",
      use: zh ? "用于判断增长下行是否进入需求收缩阶段。" : "Helps detect transition into demand contraction."
    },
    {
      category: "core-macro",
      title: zh ? "D05 企业盈利与信用" : "D05 Earnings & Credit",
      weight: "8%",
      definition: zh ? "盈利预期修正与信用利差/违约压力。" : "Earnings revisions and credit-spread/default stress.",
      details: zh ? "关键指标: 标普500 Forward P/E、EPS修正广度、HY OAS" : "Key indicators: S&P 500 Fwd P/E, EPS revision breadth, HY OAS",
      why: zh ? "连接估值端和信用端，是资产定价核心桥梁。" : "Bridges equity valuation and credit stress in pricing.",
      read: zh ? "EPS下修与OAS走阔同时出现时风险上升更快。" : "Simultaneous EPS downgrades and OAS widening raise risk sharply.",
      use: zh ? "用于行业配置和信用敞口管理。" : "Use for sector allocation and credit exposure control."
    },
    {
      category: "core-macro",
      title: zh ? "D06 房地产与利率敏感部门" : "D06 Housing & Rate-sensitive Sectors",
      weight: "6%",
      definition: zh ? "按揭利率、成交与开工，反映利率传导。" : "Mortgage rates, transactions, and starts as rate transmission channel.",
      details: zh ? "关键指标: 30年按揭利率、成屋销售、新屋开工" : "Key indicators: 30Y mortgage, existing home sales, housing starts",
      why: zh ? "地产是政策利率传导到实体的重要通道。" : "Housing is a key channel from policy rates to real activity.",
      read: zh ? "融资成本高企+成交疲弱通常压制后续增长。" : "High financing costs + weak transactions usually weigh on growth.",
      use: zh ? "可作为衰退风险早期筛查项之一。" : "Use as an early recession-risk filter."
    },
    {
      category: "core-macro",
      title: zh ? "D10 金融条件与信用传导" : "D10 Financial Conditions & Credit Transmission",
      weight: "6%",
      definition: zh ? "金融条件指数、银行信贷供给与资金面压力。" : "FCI, bank credit supply, and funding pressure.",
      details: zh ? "关键指标: FCI、银行信贷增速、TED利差" : "Key indicators: FCI, bank lending growth, TED spread",
      why: zh ? "决定政策变化能否有效传导至实体融资端。" : "Shows whether policy changes pass through to real financing conditions.",
      read: zh ? "金融条件收紧且信贷放缓时，景气下行风险增加。" : "Tighter conditions with slower lending increase downside risk.",
      use: zh ? "用于判断信用周期的拐点位置。" : "Use to detect turning points in credit cycle."
    },
    {
      category: "policy-external",
      title: zh ? "D08 外部部门与美元条件" : "D08 External Sector & Dollar Conditions",
      weight: "7%",
      definition: zh ? "DXY/利差/资本流动等外部融资条件。" : "DXY, cross-rate spreads, and capital flows.",
      details: zh ? "关键指标: DXY/REER、美日利差、海外净买入美国资产" : "Key indicators: DXY/REER, US-JP spread, foreign net buying of US assets",
      why: zh ? "美元与跨境资金流影响全球流动性与风险偏好。" : "Dollar/flow dynamics shape global liquidity and risk appetite.",
      read: zh ? "美元过强通常对应外部融资条件收紧。" : "Overly strong USD usually tightens external financing conditions.",
      use: zh ? "用于跨市场和汇率敏感资产的风险管理。" : "Use for cross-market and FX-sensitive risk management."
    },
    {
      category: "policy-external",
      title: zh ? "D09 财政政策与债务约束" : "D09 Fiscal Policy & Debt Constraint",
      weight: "8%",
      definition: zh ? "赤字、利息负担与债务路径。" : "Deficit, interest burden, and debt trajectory.",
      details: zh ? "关键指标: 财政赤字/GDP、债务/GDP、利息支出/财政收入" : "Key indicators: deficit/GDP, debt/GDP, interest/revenue",
      why: zh ? "财政可持续性决定中长期政策空间与期限溢价。" : "Fiscal sustainability drives long-run policy room and term premium.",
      read: zh ? "利息负担上行快于收入时，财政约束显著强化。" : "Interest burden rising faster than revenue tightens fiscal constraints.",
      use: zh ? "用于评估长端利率与风险资产估值约束。" : "Use to assess long-rate and valuation constraints."
    },
    {
      category: "policy-external",
      title: zh ? "D12 信心与不确定性" : "D12 Confidence & Uncertainty",
      weight: "6%",
      definition: zh ? "消费者/企业信心与政策不确定性。" : "Consumer/business confidence and policy uncertainty.",
      details: zh ? "关键指标: 消费者信心、CEO Confidence、EPU" : "Key indicators: consumer confidence, CEO confidence, EPU",
      why: zh ? "软数据影响投资和招聘意愿，常领先硬数据拐点。" : "Soft data influences capex/hiring and often leads hard-data turns.",
      read: zh ? "信心回落与不确定性上升通常抑制企业支出。" : "Falling confidence and rising uncertainty usually suppress spending.",
      use: zh ? "用于验证增长指标是否会继续走弱。" : "Use to validate whether growth weakening may persist."
    },
    {
      category: "market-mapping",
      title: zh ? "D07 风险偏好与跨资产波动" : "D07 Risk Appetite & Cross-asset Volatility",
      weight: "6%",
      definition: zh ? "VIX/MOVE/回撤等市场风险映射。" : "Market risk mapping via VIX, MOVE, and drawdown metrics.",
      details: zh ? "关键指标: VIX、MOVE、3个月最大回撤" : "Key indicators: VIX, MOVE, 3M max drawdown",
      why: zh ? "直接反映市场风险溢价与去风险压力。" : "Directly reflects risk premia and de-risk pressure.",
      read: zh ? "波动率快速抬升通常先于资金面紧张扩散。" : "Vol spikes often precede broader stress propagation.",
      use: zh ? "用于仓位、杠杆与对冲强度管理。" : "Use for position sizing, leverage, and hedge intensity."
    },
    {
      category: "market-mapping",
      title: zh ? "D11 大宗商品与能源/地缘风险" : "D11 Commodities & Geopolitical Risk",
      weight: "5%",
      definition: zh ? "油价/商品与地缘风险对通胀和增长的冲击。" : "Commodity and geopolitical shocks to inflation and growth.",
      details: zh ? "关键指标: WTI、CRB同比、GPR指数" : "Key indicators: WTI, CRB YoY, GPR index",
      why: zh ? "外生冲击会同时改变通胀路径与增长预期。" : "Exogenous shocks can alter both inflation path and growth outlook.",
      read: zh ? "能源上行+地缘紧张上升时需防范滞胀尾部。" : "Energy spikes plus geopolitical stress raise stagflation tail risks.",
      use: zh ? "用于事件驱动风险预案与对冲。" : "Use for event-driven contingency and hedging."
    },
    {
      category: "theme-panel",
      title: zh ? "D13 AI资本开支周期（主题）" : "D13 AI Capex Cycle (Theme)",
      weight: "4%",
      definition: zh ? "云与AI资本开支/营收动能（主题观察）。" : "Cloud/AI capex and revenue momentum as theme monitor.",
      details: zh ? "关键指标: 云业务增速、AI Capex指引、半导体景气代理" : "Key indicators: cloud growth, AI capex guidance, semiconductor proxy",
      why: zh ? "反映技术投资景气和相关产业链盈利弹性。" : "Captures innovation-cycle strength and chain-level earnings beta.",
      read: zh ? "景气上行有利成长风格，但需警惕估值过热。" : "Upswing supports growth style but may raise valuation overheating risk.",
      use: zh ? "建议作为主题面板，不替代核心宏观维度。" : "Use as a theme panel, not a substitute for core macro blocks."
    },
    {
      category: "theme-panel",
      title: zh ? "D14 加密与稳定币流动性（主题）" : "D14 Crypto & Stablecoin Liquidity (Theme)",
      weight: "1%",
      definition: zh ? "稳定币与链上流动性的低权重主题观察。" : "Low-weight thematic monitor of stablecoin and on-chain liquidity.",
      details: zh ? "关键指标: BTC、USDC市值、稳定币总市值" : "Key indicators: BTC, USDC market cap, total stablecoin cap",
      why: zh ? "可提供风险偏好边际变化的补充信号。" : "Provides supplementary signal on marginal risk appetite shifts.",
      read: zh ? "波动高、噪音大，需与主模型交叉验证。" : "High volatility/noise requires cross-check with core model.",
      use: zh ? "保持低权重，避免对总分造成过度扰动。" : "Keep low weight to avoid over-influencing headline score."
    },
    {
      category: "data-source",
      title: "FRED (Federal Reserve Economic Data)",
      weight: "",
      definition: zh ? "美联储圣路易斯分行经济数据平台。" : "Federal Reserve Bank of St. Louis economic data platform.",
      details: zh ? "覆盖利率、就业、通胀、信用等核心时间序列。访问: https://fred.stlouisfed.org/" : "Covers rates, labor, inflation, credit and more. URL: https://fred.stlouisfed.org/",
      why: zh ? "主源稳定、可回溯，适合模型长期维护。" : "Stable and backtestable primary source for long-horizon maintenance.",
      read: zh ? "同一指标建议固定代码，减少口径漂移。" : "Keep fixed series code per indicator to reduce methodology drift.",
      use: zh ? "报告发布前核对最近观测与更新时间戳。" : "Validate latest observations and timestamps before publishing."
    },
    {
      category: "data-source",
      title: "BEA (Bureau of Economic Analysis)",
      weight: "",
      definition: zh ? "美国经济分析局，发布GDP/PCE等官方数据。" : "US Bureau of Economic Analysis, publisher of GDP/PCE data.",
      details: zh ? "关键数据: 实际GDP、个人消费支出、收入相关序列。访问: https://www.bea.gov/" : "Key data: real GDP, PCE, income-related series. URL: https://www.bea.gov/",
      why: zh ? "是增长与通胀核心数据的官方来源之一。" : "Official source for critical growth/inflation components.",
      read: zh ? "注意首发值与修订值版本差异。" : "Track first release vs revised vintages.",
      use: zh ? "模型中应统一使用同一版次口径。" : "Use consistent vintage methodology in the model."
    },
    {
      category: "data-source",
      title: "BLS (Bureau of Labor Statistics)",
      weight: "",
      definition: zh ? "美国劳工统计局，发布就业与CPI数据。" : "US Bureau of Labor Statistics for labor and CPI data.",
      details: zh ? "关键数据: CPI、失业率、工资等。访问: https://www.bls.gov/" : "Key data: CPI, unemployment, wages. URL: https://www.bls.gov/",
      why: zh ? "就业与通胀是政策路径判断核心变量。" : "Labor and inflation are core policy-path variables.",
      read: zh ? "应结合趋势而非单次读数判断拐点。" : "Infer turning points from trend, not one-off prints.",
      use: zh ? "与BEA/FRED交叉验证后用于日报结论。" : "Cross-check with BEA/FRED before daily conclusions."
    }
  ];
}

function referenceSignalGuide() {
  const zh = getLang() === "zh";
  return [
    {
      title: zh ? "≥70分 - 强烈看多" : ">=70 - Strong Bullish",
      text: zh ? "经济环境强劲、流动性支持明显，适合积极风险配置。" : "Strong macro/liquidity backdrop supports aggressive risk allocation."
    },
    {
      title: zh ? "60-69分 - 温和看多" : "60-69 - Mild Bullish",
      text: zh ? "基本面稳健但有不确定性，适合平衡偏进攻配置。" : "Fundamentals are healthy but mixed; balanced pro-risk stance."
    },
    {
      title: zh ? "40-59分 - 中性" : "40-59 - Neutral",
      text: zh ? "多空因素交织，建议精选资产并重视风控。" : "Mixed forces; selective positioning with tighter risk controls."
    },
    {
      title: zh ? "30-39分 - 温和看空" : "30-39 - Mild Bearish",
      text: zh ? "下行压力增加，宜降低β并提高防御仓位。" : "Downside pressure rises; reduce beta and increase defensives."
    },
    {
      title: zh ? "≤29分 - 强烈看空" : "<=29 - Strong Bearish",
      text: zh ? "衰退/危机风险高，优先资本保护与流动性管理。" : "High recession/crisis risk; prioritize capital preservation and liquidity."
    }
  ];
}

function buildGlossaryEntries(model) {
  const reference = referenceGlossaryCatalog();
  const dims = (model.tables?.dimensions || [])
    .filter((row) => /^D\\d{2}$/i.test(findValue(row, ["dimensionid", "维度id", "id"])))
    .slice(0, 14)
    .map((row) => {
      const id = findValue(row, ["dimensionid", "维度id", "id"]);
      const name = findValue(row, ["dimensionname", "维度名称", "维度"]);
      const weight = findValue(row, ["weight", "权重", "%"]);
      const tier = findValue(row, ["tier", "层级"]);
      const definition = findValue(row, ["definition", "定义", "说明"]);
      const update = findValue(row, ["typical update", "frequency", "更新"]);
      const indicators = (model.tables?.indicators || [])
        .filter((i) => findValue(i, ["dimensionid", "维度id", "id"]).toLowerCase() === id.toLowerCase())
        .slice(0, 3)
        .map((i) => findValue(i, ["indicatorname", "指标", "name"]))
        .filter(Boolean)
        .join(" / ");

      return {
        category: mapTierToCategory(tier),
        title: `${id} ${name}`,
        weight,
        definition,
        details: `${getLang() === "zh" ? "关键指标" : "Key Indicators"}: ${indicators || "--"} · ${
          getLang() === "zh" ? "更新频率" : "Update"
        }: ${update || "--"}`,
        why:
          getLang() === "zh"
            ? `该维度用于衡量“${name}”对宏观周期与风险偏好的传导影响。`
            : `This dimension measures how "${name}" transmits into macro cycle and risk appetite.`,
        read:
          getLang() === "zh"
            ? "一般而言，维度分上升代表该块环境改善；分数下降代表该块风险积累。"
            : "In general, rising dimension score means improving conditions, while falling score means risk build-up.",
        use:
          getLang() === "zh"
            ? "可与加权贡献一起观察，用于解释总分变化与日报结论。"
            : "Use with weighted contribution to explain headline score changes and daily conclusions."
      };
    });

  const sources = [...new Set((model.tables?.indicators || []).map((row) => findValue(row, ["主数据源", "source"])).filter(Boolean))]
    .slice(0, 6)
    .map((source) => ({
      category: "data-source",
      title: source,
      weight: "",
      definition: getLang() === "zh" ? "模型使用的主要数据来源。" : "Primary data source used by the model.",
      details: source,
      why:
        getLang() === "zh"
          ? "稳定、可回溯的数据源可减少模型口径漂移。"
          : "Stable and backtestable sources reduce methodology drift over time.",
      read:
        getLang() === "zh"
          ? "同一指标建议固定主源，必要时再使用备选源。"
          : "Keep a fixed primary source per indicator and use fallback sources only when needed.",
      use:
        getLang() === "zh"
          ? "发布报告前应核对数据时间戳与最近更新时间。"
          : "Validate source timestamps before publishing reports."
    }));

  return [...reference, ...dims, ...sources];
}

function renderGlossary(model) {
  const root = document.getElementById("glossary-grid");
  const search = document.getElementById("glossary-search");
  const filter = document.getElementById("glossary-filter");
  if (!root) return;

  const staticEntries = glossaryTerms.map((item) => ({
    category: "core-macro",
    title: item[getLang()].term,
    weight: "",
    definition: item[getLang()].desc,
    details: "",
    why: item[getLang()].why,
    read: item[getLang()].read,
    use: item[getLang()].use
  }));
  const entries = [...buildGlossaryEntries(model), ...staticEntries];

  const draw = () => {
    const q = asText(search?.value).toLowerCase();
    const c = asText(filter?.value) || "all";
    const filtered = entries.filter((entry) => {
      const hitCategory = c === "all" || entry.category === c;
      const blob = `${entry.title} ${entry.definition} ${entry.details} ${entry.why || ""} ${entry.read || ""} ${entry.use || ""}`.toLowerCase();
      return hitCategory && (!q || blob.includes(q));
    });

    root.innerHTML = "";
    filtered.forEach((item) => {
      const card = document.createElement("article");
      card.className = "glossary-card";
      card.innerHTML = `
        <h3>${escapeHtml(item.title)}</h3>
        ${item.weight ? `<div class="term-weight">${getLang() === "zh" ? "权重" : "Weight"}: ${escapeHtml(item.weight)}</div>` : ""}
        <p><strong>${getLang() === "zh" ? "定义" : "Definition"}:</strong> ${escapeHtml(item.definition)}</p>
        ${item.details ? `<p>${escapeHtml(item.details)}</p>` : ""}
        ${item.why ? `<p><strong>${getLang() === "zh" ? "为什么重要" : "Why It Matters"}:</strong> ${escapeHtml(item.why)}</p>` : ""}
        ${item.read ? `<p><strong>${getLang() === "zh" ? "如何解读" : "How To Read"}:</strong> ${escapeHtml(item.read)}</p>` : ""}
        ${item.use ? `<p><strong>${getLang() === "zh" ? "实务使用" : "Practical Use"}:</strong> ${escapeHtml(item.use)}</p>` : ""}
      `;
      root.appendChild(card);
    });
  };

  search?.addEventListener("input", draw);
  filter?.addEventListener("change", draw);
  draw();

  const signalRoot = document.getElementById("signal-guide-grid");
  if (signalRoot) {
    signalRoot.innerHTML = "";
    referenceSignalGuide().forEach((s) => {
      const card = document.createElement("article");
      card.className = "signal-card";
      card.innerHTML = `<h3>${escapeHtml(s.title)}</h3><p>${escapeHtml(s.text)}</p>`;
      signalRoot.appendChild(card);
    });
  }
}

function renderIndicatorsPage(model) {
  renderObjectTable("indicators-page-table", model.tables?.indicators || []);
}

async function loadDefaultWorkbook() {
  const response = await fetch(DEFAULT_MODEL_FILE, { cache: "no-cache" });
  if (!response.ok) throw new Error(`Unable to fetch ${DEFAULT_MODEL_FILE}`);
  return response.arrayBuffer();
}

function setupUpload(onLoaded) {
  const input = document.getElementById("xlsx-input");
  const status = document.getElementById("file-status");
  if (!input) return;

  input.addEventListener("change", async (event) => {
    const [file] = event.target.files || [];
    if (!file) return;

    try {
      const buffer = await file.arrayBuffer();
      const model = parseWorkbook(buffer);
      await saveCurrentModel(model);
      if (status) status.textContent = `Loaded: ${file.name}`;
      onLoaded(model);
    } catch {
      if (status) status.textContent = getLang() === "zh" ? "文件解析失败，请重试。" : "Failed to parse file.";
    }
  });
}

async function initDashboard() {
  const status = document.getElementById("file-status");
  const summary = await loadDashboardSummary();
  if (summary) renderDashboardSummary(summary);
  let model = await loadCurrentModel();
  renderDashboard(model);
  if (!model?.tables?.dimensions?.length) {
    try {
      const buffer = await loadDefaultWorkbook();
      model = parseWorkbook(buffer);
      await saveCurrentModel(model);
      renderDashboard(model);
      if (status) status.textContent = "Auto-loaded: model.xlsx";
    } catch {
      if (status) status.textContent = getLang() === "zh" ? "使用本地缓存/样例数据。" : "Using saved/sample data.";
    }
  } else if (status) {
    status.textContent = getLang() === "zh" ? "已加载最新快照数据" : "Loaded latest snapshot data";
  }

  setupUpload((next) => {
    renderDashboard(next);
  });
  await setupSubscriptionForm();
}

async function ensureModelData(model) {
  const hasData = (model?.tables?.dimensions || []).length || (model?.tables?.indicators || []).length;
  if (hasData) return model;

  try {
    const buffer = await loadDefaultWorkbook();
    const parsed = parseWorkbook(buffer);
    await saveCurrentModel(parsed);
    return parsed;
  } catch {
    return model;
  }
}

async function init() {
  setLang(getLang());
  applyI18n();
  await migrateBrowserDataToServer();

  const page = document.body.dataset.page;
  const model = await ensureModelData(await loadCurrentModel());

  setupLangToggle(async () => {
    const currentModel = await loadCurrentModel();
    if (page === "dashboard") {
      renderDashboard(currentModel);
      await setupSubscriptionForm();
    }
    if (page === "daily-report") renderDailyReport(currentModel);
    if (page === "indicators") renderIndicatorsPage(currentModel);
    if (page === "glossary") renderGlossary(currentModel);
  });

  if (page === "dashboard") await initDashboard();
  if (page === "daily-report") await renderDailyReport(model);
  if (page === "indicators") renderIndicatorsPage(model);
  if (page === "glossary") renderGlossary(model);
}

document.addEventListener("DOMContentLoaded", init);
