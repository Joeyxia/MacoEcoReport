const DB_NAME = "macro-monitor-db";
const DB_VERSION = 1;
const STORAGE_KEY = "macro-monitor-model";
const LANG_KEY = "macro-monitor-lang";
const DEFAULT_MODEL_FILE = "./model.xlsx";

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
    all_indicators_info: "全部指标信息"
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
  const fromDb = await dbGet("model", "current");
  if (fromDb?.payload) return fromDb.payload;
  return loadModelFallback();
}

async function saveCurrentModel(model) {
  saveModelFallback(model);
  await dbPut("model", { id: "current", payload: model, updatedAt: new Date().toISOString() });
}

async function saveReport(date, text, meta) {
  await dbPut("reports", { date, text, meta, updatedAt: new Date().toISOString() });
}

async function loadReport(date) {
  return dbGet("reports", date);
}

async function listReports() {
  const reports = await dbGetAll("reports");
  return reports.sort((a, b) => b.date.localeCompare(a.date));
}

function asText(v) {
  return v === undefined || v === null ? "" : String(v).trim();
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

  const scoreRows = [];
  tables.scores.forEach((row) => {
    const nameKey = keyByIncludes(row, ["dimensionname", "dimension", "维度名称", "维度"]);
    const scoreKey = keyByIncludes(row, ["dimscore", "score", "维度分"]);
    const contribKey = keyByIncludes(row, ["weightedcontribution", "contribution", "贡献"]);
    if (!nameKey || !scoreKey) return;

    const name = asText(row[nameKey]);
    const score = asNumber(row[scoreKey]);
    if (!name || score === null) return;
    scoreRows.push({ name, score, contribution: asNumber(row[contribKey]) ?? 0 });
  });

  const totalRow = scoreRows.find((r) => r.name.toLowerCase() === "total");
  const dimensions = scoreRows.filter((r) => r.name.toLowerCase() !== "total").slice(0, 14);
  const totalScore = totalRow ? totalRow.score : average(dimensions.map((d) => d.score));

  const alerts = [];
  tables.alerts.forEach((row) => {
    const idKey = keyByIncludes(row, ["alertid", "id"]);
    const levelKey = keyByIncludes(row, ["level", "等级"]);
    const condKey = keyByIncludes(row, ["condition", "条件"]);
    const triggerKey = keyByIncludes(row, ["triggered", "触发"]);

    const id = asText(row[idKey]);
    const level = asText(row[levelKey]).toUpperCase() || "YELLOW";
    const condition = asText(row[condKey]);
    const triggerRaw = asText(row[triggerKey]).toLowerCase();
    if (!id || !condition) return;

    const triggered = ["yes", "y", "true", "1", "触发"].includes(triggerRaw);
    alerts.push({ id, level, condition, triggered });
  });

  const activeAlerts = alerts.filter((a) => a.triggered).length;
  const drivers = buildDrivers(dimensions, activeAlerts, totalScore);

  return {
    asOf: asOf || new Date().toISOString().slice(0, 10),
    totalScore: round(totalScore, 1),
    status: inferStatus(totalScore),
    alerts,
    dimensions,
    drivers,
    tables,
    workbook: { sheets: workbookSheets },
    onlineCheck: []
  };
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

  const reports = await listReports();
  const latest = reports[0];
  const activeAlerts = (model.alerts || []).filter((a) => a.triggered);
  const weakDims = [...(model.dimensions || [])].sort((a, b) => a.score - b.score).slice(0, 3);

  root.innerHTML = `
    <div class="summary-score">${round(model.totalScore, 1)}/100 · ${escapeHtml(model.status)}</div>
    <div class="summary-line">${getLang() === "zh" ? "模型更新日" : "Model As-Of"}: ${escapeHtml(model.asOf)}</div>
    <div class="summary-line">${getLang() === "zh" ? "最新报告日期" : "Latest Report Date"}: ${escapeHtml(latest?.date || "--")}</div>
    <div class="summary-line">${getLang() === "zh" ? "简要结论" : "Short Summary"}: ${
      getLang() === "zh"
        ? `当前处于${escapeHtml(model.status)}，重点关注${weakDims.map((d) => d.name).join(" / ")}。`
        : `Current regime is ${escapeHtml(model.status)}; watch ${weakDims.map((d) => d.name).join(" / ")}.`
    }</div>
  `;

  if (!watchRoot) return;
  watchRoot.innerHTML = "";
  const items = [
    ...activeAlerts.map((a) => `${a.id}: ${a.condition}`),
    ...weakDims.map((d) => `${d.name}: ${round(d.score, 1)}`)
  ];
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

  const active = (model.alerts || []).filter((a) => a.triggered);
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
  [...(model.dimensions || [])]
    .sort((a, b) => (b.contribution || 0) - (a.contribution || 0))
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
  (model.drivers || []).forEach((item) => {
    const card = document.createElement("article");
    card.className = "driver-card";
    card.innerHTML = `<h3>${escapeHtml(item.title)}</h3><p>${escapeHtml(item.text)}</p>`;
    drivers.appendChild(card);
  });

  renderKeyIndicators(model);
  renderLatestReportSummary(model);
  renderDimensionLayers(model.tables?.dimensions || [], model);
  renderObjectTable("dimensions-table", model.tables?.dimensions || []);
  renderObjectTable("inputs-table", model.tables?.inputs || []);
  renderObjectTable("indicators-table", model.tables?.indicators || []);
  renderObjectTable("scores-table", model.tables?.scores || []);
  renderObjectTable("alerts-table", model.tables?.alerts || []);
  renderWorkbookExplorer(model.workbook || {});
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
      <a class="report-open" href="daily-report.html?date=${encodeURIComponent(report.date)}">${getLang() === "zh" ? "打开" : "Open"}</a>
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

  const existing = await loadReport(date);
  const initial = existing?.text || generateDailyText(model, date, null);
  editor.value = initial;
  renderDailyReportPreview(model, date);

  renderObjectTable("daily-scores-table", model.tables?.scores || []);
  renderOnlineCheckTable(model.onlineCheck || []);
  renderReportLinks(await listReports());

  regenBtn?.addEventListener("click", () => {
    editor.value = generateDailyText(model, date, null);
    renderDailyReportPreview(model, date);
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

function buildGlossaryEntries(model) {
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

  return [...dims, ...sources];
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
  let model = await loadCurrentModel();
  renderDashboard(model);

  try {
    const buffer = await loadDefaultWorkbook();
    model = parseWorkbook(buffer);
    await saveCurrentModel(model);
    renderDashboard(model);
    if (status) status.textContent = "Auto-loaded: model.xlsx";
  } catch {
    if (status) status.textContent = getLang() === "zh" ? "使用本地缓存/样例数据。" : "Using saved/sample data.";
  }

  setupUpload((next) => {
    renderDashboard(next);
  });
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

  const page = document.body.dataset.page;
  const model = await ensureModelData(await loadCurrentModel());

  setupLangToggle(async () => {
    const currentModel = await loadCurrentModel();
    if (page === "dashboard") renderDashboard(currentModel);
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
