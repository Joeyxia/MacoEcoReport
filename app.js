const STORAGE_KEY = "macro-monitor-model";
const REPORT_PREFIX = "macro-monitor-report-";
const DEFAULT_MODEL_FILE = "./model.xlsx";

const sampleModel = {
  asOf: "2026-03-01",
  totalScore: 58.4,
  status: "Neutral Fragile",
  alerts: [
    { id: "A01", level: "YELLOW", condition: "HY OAS > 600bps", triggered: false },
    { id: "A02", level: "RED", condition: "VIX > 30", triggered: false },
    { id: "A03", level: "YELLOW", condition: "10Y-3M < -50bps", triggered: true }
  ],
  dimensions: [
    { name: "Monetary Policy & Liquidity", score: 46.2, contribution: 5.5 },
    { name: "Growth & Forward Signals", score: 57.8, contribution: 6.4 },
    { name: "Inflation & Price Pressure", score: 61.3, contribution: 6.1 },
    { name: "Labor & Households", score: 59.4, contribution: 5.9 },
    { name: "Financial Conditions", score: 52.8, contribution: 4.8 },
    { name: "External / Dollar Conditions", score: 54.6, contribution: 3.8 }
  ],
  drivers: [
    { title: "Primary Support", text: "Core inflation has moderated toward the target zone, easing policy pressure." },
    { title: "Primary Drag", text: "Inverted term structure and tight liquidity keep recession risk elevated." },
    { title: "Risk Trigger", text: "A sharp jump in volatility (VIX/MOVE) would likely move the model into defense mode." }
  ],
  tables: {
    dimensions: [],
    indicators: [],
    inputs: [],
    scores: [],
    alerts: []
  },
  workbook: {
    sheetNames: [],
    sheets: []
  }
};

const glossaryTerms = [
  { term: "Macro Composite Score", desc: "Weighted 0-100 aggregate score across the 14 dimensions." },
  { term: "Alert Trigger", desc: "Threshold-based risk event (e.g., VIX>30) used for tactical warning." },
  { term: "HigherBetter", desc: "Scoring mode where higher values improve the indicator score." },
  { term: "LowerBetter", desc: "Scoring mode where lower values improve the indicator score." },
  { term: "TargetBand", desc: "Scoring mode where a defined value range receives the highest score." },
  { term: "DimScore", desc: "Weighted average score of indicators inside one dimension." },
  { term: "WeightedContribution", desc: "Dimension contribution to total score after applying dimension weight." },
  { term: "Net Liquidity Proxy", desc: "Commonly modeled as Fed assets minus TGA minus RRP." },
  { term: "HY OAS", desc: "US high-yield option-adjusted spread, a stress gauge for credit risk." },
  { term: "MOVE Index", desc: "US Treasury volatility index, often used as a rates stress proxy." },
  { term: "DXY", desc: "US dollar index; strong moves can tighten global financial conditions." },
  { term: "FCI", desc: "Financial Conditions Index measuring overall ease/tightness of financing." },
  { term: "Defensive Zone", desc: "Model state generally associated with score band 30-45." },
  { term: "Crisis Zone", desc: "Model state generally associated with score band 0-30." },
  { term: "Theme Panel", desc: "Low-weight monitoring block for AI/Crypto cycle signals." }
];

function loadModel() {
  const stored = localStorage.getItem(STORAGE_KEY);
  if (!stored) return sampleModel;
  try {
    const parsed = JSON.parse(stored);
    return { ...sampleModel, ...parsed };
  } catch {
    return sampleModel;
  }
}

function saveModel(model) {
  localStorage.setItem(STORAGE_KEY, JSON.stringify(model));
}

function inferStatus(score) {
  if (score >= 75) return "Expansion Overheating";
  if (score >= 60) return "Moderate Expansion";
  if (score >= 45) return "Neutral Fragile";
  if (score >= 30) return "Defensive";
  return "Recession/Crisis";
}

function asText(v) {
  return v === undefined || v === null ? "" : String(v).trim();
}

function asNumber(v) {
  if (typeof v === "number") return Number.isFinite(v) ? v : null;
  const parsed = Number(String(v).replace(/,/g, "").trim());
  return Number.isFinite(parsed) ? parsed : null;
}

function pickKey(row, candidates) {
  const entries = Object.keys(row || {});
  for (const key of entries) {
    const lowered = key.toLowerCase();
    if (candidates.some((c) => lowered.includes(c))) {
      return key;
    }
  }
  return null;
}

function findSheet(workbook, candidates) {
  const names = workbook.SheetNames || [];
  const map = names.map((n) => ({ raw: n, normalized: n.toLowerCase() }));
  for (const c of candidates) {
    const match = map.find((n) => n.normalized.includes(c));
    if (match) return match.raw;
  }
  return null;
}

function objectRowsToColumns(rows) {
  const keys = new Set();
  rows.forEach((row) => Object.keys(row || {}).forEach((k) => keys.add(k)));
  return [...keys];
}

function cleanSheetRows(rows) {
  if (!rows || !rows.length) return [];

  const normalized = rows.map((row) => row.map((cell) => asText(cell)));
  const nonEmptyRows = normalized.filter((row) => row.some((cell) => cell !== ""));
  if (!nonEmptyRows.length) return [];

  let maxCol = 0;
  nonEmptyRows.forEach((row) => {
    for (let i = row.length - 1; i >= 0; i -= 1) {
      if (row[i] !== "") {
        maxCol = Math.max(maxCol, i + 1);
        break;
      }
    }
  });

  return nonEmptyRows.map((row) => row.slice(0, maxCol));
}

function parseWorkbook(arrayBuffer) {
  const workbook = XLSX.read(arrayBuffer, { type: "array" });

  const dimensionsSheetName = findSheet(workbook, ["dimensions", "维度"]);
  const indicatorsSheetName = findSheet(workbook, ["indicators", "指标"]);
  const inputsSheetName = findSheet(workbook, ["inputs", "输入"]);
  const scoresSheetName = findSheet(workbook, ["scores", "分数"]);
  const alertsSheetName = findSheet(workbook, ["alerts", "预警"]);

  const dimensionsRows = dimensionsSheetName
    ? XLSX.utils.sheet_to_json(workbook.Sheets[dimensionsSheetName], { defval: "" })
    : [];
  const indicatorsRows = indicatorsSheetName
    ? XLSX.utils.sheet_to_json(workbook.Sheets[indicatorsSheetName], { defval: "" })
    : [];
  const inputsRows = inputsSheetName
    ? XLSX.utils.sheet_to_json(workbook.Sheets[inputsSheetName], { defval: "" })
    : [];
  const scoresRows = scoresSheetName
    ? XLSX.utils.sheet_to_json(workbook.Sheets[scoresSheetName], { defval: "" })
    : [];
  const alertsRows = alertsSheetName
    ? XLSX.utils.sheet_to_json(workbook.Sheets[alertsSheetName], { defval: "" })
    : [];

  const workbookSheets = (workbook.SheetNames || []).map((name) => {
    const aoa = XLSX.utils.sheet_to_json(workbook.Sheets[name], {
      header: 1,
      defval: "",
      raw: false,
      blankrows: false
    });
    return { name, rows: cleanSheetRows(aoa) };
  });

  let asOf = "";
  for (const row of inputsRows) {
    const asOfKey = pickKey(row, ["asof", "更新", "date"]);
    const val = asText(row[asOfKey]);
    const rowValues = Object.values(row).map(asText);
    const dateLike = val || rowValues.find((x) => /^\d{4}-\d{2}-\d{2}$/.test(x));
    if (dateLike && /^\d{4}-\d{2}-\d{2}$/.test(dateLike)) {
      asOf = dateLike;
      break;
    }
  }

  const scoreCandidates = [];
  for (const row of scoresRows) {
    const nameKey = pickKey(row, ["dimensionname", "dimension", "维度名称", "维度"]);
    const scoreKey = pickKey(row, ["dimscore", "score", "维度分"]);
    const contributionKey = pickKey(row, ["weightedcontribution", "contribution", "贡献"]);
    if (!nameKey || !scoreKey) continue;

    const name = asText(row[nameKey]);
    const score = asNumber(row[scoreKey]);
    if (!name || score === null) continue;
    const contribution = asNumber(row[contributionKey]) ?? 0;
    scoreCandidates.push({ name, score, contribution });
  }

  const dimensions = scoreCandidates
    .filter((x) => x.name.toLowerCase() !== "total" && x.name !== "TOTAL")
    .slice(0, 14);

  const totalRow = scoreCandidates.find((x) => x.name.toLowerCase() === "total") || null;
  const totalScore = totalRow ? totalRow.score : average(dimensions.map((x) => x.score));

  const alerts = [];
  for (const row of alertsRows) {
    const idKey = pickKey(row, ["alertid", "id"]);
    const levelKey = pickKey(row, ["level", "等级"]);
    const conditionKey = pickKey(row, ["condition", "条件"]);
    const trigKey = pickKey(row, ["triggered", "触发"]);

    const id = asText(row[idKey]);
    const level = asText(row[levelKey]).toUpperCase();
    const condition = asText(row[conditionKey]);
    const trigRaw = asText(row[trigKey]).toLowerCase();
    if (!id || !condition) continue;

    const triggered = ["yes", "y", "true", "1", "触发"].includes(trigRaw);
    alerts.push({ id, level: level || "YELLOW", condition, triggered });
  }

  const alertCount = alerts.filter((a) => a.triggered).length;
  const drivers = buildDrivers(totalScore, alertCount, dimensions);

  return {
    asOf: asOf || new Date().toISOString().slice(0, 10),
    totalScore: round(totalScore, 1),
    status: inferStatus(totalScore),
    alerts,
    dimensions: dimensions.length ? dimensions : sampleModel.dimensions,
    drivers,
    tables: {
      dimensions: dimensionsRows,
      indicators: indicatorsRows,
      inputs: inputsRows,
      scores: scoresRows,
      alerts: alertsRows
    },
    workbook: {
      sheetNames: workbook.SheetNames || [],
      sheets: workbookSheets
    }
  };
}

function buildDrivers(totalScore, alertCount, dimensions) {
  const sorted = [...dimensions].sort((a, b) => b.score - a.score);
  const best = sorted[0];
  const worst = sorted[sorted.length - 1];

  return [
    {
      title: "Primary Support",
      text: best
        ? `${best.name} is the strongest block (${round(best.score, 1)}), helping stabilize the composite.`
        : "Leading dimensions are providing moderate support."
    },
    {
      title: "Primary Drag",
      text: worst
        ? `${worst.name} remains the key drag (${round(worst.score, 1)}), keeping downside risks alive.`
        : "Weak dimensions still cap upside for the overall score."
    },
    {
      title: "Risk Trigger",
      text:
        alertCount > 0
          ? `${alertCount} alert(s) are currently triggered. Focus on risk control until signals normalize.`
          : totalScore >= 60
            ? "No active alert. Tactical risk appetite can stay neutral-to-positive."
            : "No active alert, but the score remains soft. Keep positioning balanced."
    }
  ];
}

function average(arr) {
  if (!arr.length) return sampleModel.totalScore;
  return arr.reduce((sum, n) => sum + n, 0) / arr.length;
}

function round(v, d = 0) {
  const x = Number(v);
  if (!Number.isFinite(x)) return 0;
  const p = 10 ** d;
  return Math.round(x * p) / p;
}

function renderObjectTable(targetId, rows) {
  const root = document.getElementById(targetId);
  if (!root) return;

  const dataRows = Array.isArray(rows) ? rows : [];
  const columns = objectRowsToColumns(dataRows);

  if (!dataRows.length || !columns.length) {
    root.innerHTML = '<p class="table-empty">No data found.</p>';
    return;
  }

  const thead = `<thead><tr>${columns.map((c) => `<th>${escapeHtml(c)}</th>`).join("")}</tr></thead>`;
  const tbody = `<tbody>${dataRows
    .map(
      (row) =>
        `<tr>${columns
          .map((c) => `<td>${escapeHtml(asText(row[c]))}</td>`)
          .join("")}</tr>`
    )
    .join("")}</tbody>`;

  root.innerHTML = `<table class="data-table">${thead}${tbody}</table>`;
}

function renderSheetTable(targetId, rows) {
  const root = document.getElementById(targetId);
  if (!root) return;

  if (!rows || !rows.length) {
    root.innerHTML = '<p class="table-empty">No rows in this sheet.</p>';
    return;
  }

  const maxCols = rows.reduce((max, row) => Math.max(max, row.length), 0);
  const headerRow = rows[0] || [];
  const useFirstRowHeader = headerRow.some((c) => asText(c) !== "");

  const headers = Array.from({ length: maxCols }, (_, i) => {
    const cell = useFirstRowHeader ? asText(headerRow[i]) : "";
    return cell || `Col ${i + 1}`;
  });

  const bodyRows = useFirstRowHeader ? rows.slice(1) : rows;

  const thead = `<thead><tr>${headers.map((h) => `<th>${escapeHtml(h)}</th>`).join("")}</tr></thead>`;
  const tbody = `<tbody>${bodyRows
    .map(
      (row) =>
        `<tr>${Array.from({ length: maxCols }, (_, i) => `<td>${escapeHtml(asText(row[i]))}</td>`).join("")}</tr>`
    )
    .join("")}</tbody>`;

  root.innerHTML = `<table class="data-table">${thead}${tbody}</table>`;
}

function escapeHtml(input) {
  return asText(input)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function renderDashboard(model) {
  const totalScore = document.getElementById("total-score");
  const asOf = document.getElementById("as-of");
  const macroStatus = document.getElementById("macro-status");
  const alertList = document.getElementById("alert-list");
  const bars = document.getElementById("dimension-bars");
  const drivers = document.getElementById("drivers");

  if (!totalScore) return;

  totalScore.textContent = round(model.totalScore, 1).toFixed(1);
  asOf.textContent = model.asOf;
  macroStatus.textContent = model.status;

  const activeAlerts = (model.alerts || []).filter((a) => a.triggered);
  alertList.innerHTML = "";
  if (!activeAlerts.length) {
    const li = document.createElement("li");
    li.className = "alert-item none";
    li.textContent = "No active alerts. Conditions are currently stable by configured thresholds.";
    alertList.appendChild(li);
  } else {
    activeAlerts.forEach((a) => {
      const li = document.createElement("li");
      li.className = `alert-item ${a.level.toLowerCase()}`;
      li.innerHTML = `<strong>${escapeHtml(a.id)} (${escapeHtml(a.level)})</strong><br>${escapeHtml(a.condition)}`;
      alertList.appendChild(li);
    });
  }

  bars.innerHTML = "";
  [...(model.dimensions || [])]
    .sort((a, b) => (b.contribution || 0) - (a.contribution || 0))
    .slice(0, 8)
    .forEach((d) => {
      const row = document.createElement("div");
      row.className = "bar-row";
      const width = Math.max(0, Math.min(100, Number(d.score) || 0));
      row.innerHTML = `
        <div class="bar-top"><span>${escapeHtml(d.name)}</span><span>${round(d.score, 1)}</span></div>
        <div class="bar-track"><div class="bar-fill" style="width:${width}%"></div></div>
      `;
      bars.appendChild(row);
    });

  drivers.innerHTML = "";
  (model.drivers || []).forEach((d) => {
    const card = document.createElement("article");
    card.className = "driver-card";
    card.innerHTML = `<h3>${escapeHtml(d.title)}</h3><p>${escapeHtml(d.text)}</p>`;
    drivers.appendChild(card);
  });

  renderObjectTable("dimensions-table", model.tables?.dimensions || []);
  renderObjectTable("inputs-table", model.tables?.inputs || []);
  renderObjectTable("indicators-table", model.tables?.indicators || []);
  renderObjectTable("scores-table", model.tables?.scores || []);
  renderObjectTable("alerts-table", model.tables?.alerts || []);
  renderWorkbookExplorer(model.workbook);
}

function renderWorkbookExplorer(workbookData) {
  const tabsRoot = document.getElementById("sheet-tabs");
  const tableRoot = document.getElementById("sheet-table");
  if (!tabsRoot || !tableRoot) return;

  const sheets = workbookData?.sheets || [];
  tabsRoot.innerHTML = "";
  if (!sheets.length) {
    tableRoot.innerHTML = '<p class="table-empty">Upload or auto-load a workbook to explore all sheets.</p>';
    return;
  }

  let activeIndex = 0;

  const renderActive = () => {
    const selected = sheets[activeIndex];
    renderSheetTable("sheet-table", selected?.rows || []);
    [...tabsRoot.querySelectorAll(".sheet-tab")].forEach((btn, idx) => {
      btn.classList.toggle("active", idx === activeIndex);
    });
  };

  sheets.forEach((sheet, idx) => {
    const btn = document.createElement("button");
    btn.type = "button";
    btn.className = "sheet-tab";
    btn.textContent = `${sheet.name} (${sheet.rows.length})`;
    btn.addEventListener("click", () => {
      activeIndex = idx;
      renderActive();
    });
    tabsRoot.appendChild(btn);
  });

  renderActive();
}

function dailyReportKey(dateStr) {
  return `${REPORT_PREFIX}${dateStr}`;
}

function generateDailyText(model, reportDate) {
  const top = [...(model.dimensions || [])].sort((a, b) => b.score - a.score).slice(0, 3);
  const bottom = [...(model.dimensions || [])].sort((a, b) => a.score - b.score).slice(0, 3);
  const activeAlerts = (model.alerts || []).filter((a) => a.triggered);

  return [
    `Macro Monitoring Daily Report`,
    `Date: ${reportDate}`,
    `Model As-Of: ${model.asOf}`,
    `Composite Score: ${round(model.totalScore, 1)} (${model.status})`,
    ``,
    `Executive Summary`,
    `- The system remains in ${model.status.toLowerCase()} regime with a total score of ${round(model.totalScore, 1)}.`,
    `- ${activeAlerts.length} alert(s) are triggered under current threshold settings.`,
    `- Tactical focus: ${activeAlerts.length > 1 ? "risk mitigation and hedging discipline" : "selective risk with strict monitoring"}.`,
    ``,
    `Top Supporting Dimensions`,
    ...top.map((x) => `- ${x.name}: ${round(x.score, 1)}`),
    ``,
    `Top Dragging Dimensions`,
    ...bottom.map((x) => `- ${x.name}: ${round(x.score, 1)}`),
    ``,
    `Alert Watchlist`,
    ...(activeAlerts.length
      ? activeAlerts.map((a) => `- ${a.id} (${a.level}): ${a.condition}`)
      : ["- No active alerts today."]),
    ``,
    `Action Framework`,
    `- Strategic: Use the total score trend (5-day / 20-day) to calibrate macro stance.`,
    `- Tactical: Treat RED alerts as immediate de-risk signals and YELLOW alerts as caution signals.`,
    `- Data: Refresh Inputs sheet daily; verify source timestamps before publishing.`,
    ``
  ].join("\n");
}

function renderDailyReport(model) {
  const today = new Date().toISOString().slice(0, 10);
  const scoreEl = document.getElementById("report-score");
  const dateEl = document.getElementById("report-date");
  const statusEl = document.getElementById("report-status");
  const editor = document.getElementById("report-editor");
  const saveStatus = document.getElementById("save-status");
  const regenBtn = document.getElementById("generate-report");
  const saveBtn = document.getElementById("save-report");
  const dlBtn = document.getElementById("download-report");

  if (!editor) return;

  scoreEl.textContent = round(model.totalScore, 1).toFixed(1);
  dateEl.textContent = today;
  statusEl.textContent = model.status;

  const existing = localStorage.getItem(dailyReportKey(today));
  editor.value = existing || generateDailyText(model, today);

  regenBtn?.addEventListener("click", () => {
    editor.value = generateDailyText(model, today);
    saveStatus.textContent = "Draft regenerated.";
  });

  saveBtn?.addEventListener("click", () => {
    localStorage.setItem(dailyReportKey(today), editor.value);
    saveStatus.textContent = `Saved at ${new Date().toLocaleTimeString()}`;
  });

  dlBtn?.addEventListener("click", () => {
    const blob = new Blob([editor.value], { type: "text/plain;charset=utf-8" });
    const a = document.createElement("a");
    a.href = URL.createObjectURL(blob);
    a.download = `macro-daily-report-${today}.txt`;
    document.body.appendChild(a);
    a.click();
    a.remove();
    URL.revokeObjectURL(a.href);
  });
}

function renderGlossary() {
  const root = document.getElementById("glossary-grid");
  if (!root) return;

  glossaryTerms.forEach((item) => {
    const card = document.createElement("article");
    card.className = "glossary-card";
    card.innerHTML = `<h3>${escapeHtml(item.term)}</h3><p>${escapeHtml(item.desc)}</p>`;
    root.appendChild(card);
  });
}

async function loadDefaultWorkbook() {
  const response = await fetch(DEFAULT_MODEL_FILE, { cache: "no-cache" });
  if (!response.ok) {
    throw new Error(`Cannot load ${DEFAULT_MODEL_FILE}`);
  }
  return response.arrayBuffer();
}

function setupUpload(onModelLoaded) {
  const input = document.getElementById("xlsx-input");
  const fileStatus = document.getElementById("file-status");
  if (!input) return;

  input.addEventListener("change", async (e) => {
    const [file] = e.target.files || [];
    if (!file) return;

    try {
      const buf = await file.arrayBuffer();
      const model = parseWorkbook(buf);
      saveModel(model);
      fileStatus.textContent = `Loaded: ${file.name}`;
      onModelLoaded(model);
    } catch (err) {
      console.error(err);
      fileStatus.textContent = "Failed to parse file. Keep existing data or try another .xlsx file.";
    }
  });
}

async function initDashboard() {
  const fileStatus = document.getElementById("file-status");
  let model = loadModel();
  renderDashboard(model);

  try {
    const buf = await loadDefaultWorkbook();
    model = parseWorkbook(buf);
    saveModel(model);
    renderDashboard(model);
    if (fileStatus) fileStatus.textContent = "Auto-loaded: model.xlsx";
  } catch (err) {
    if (fileStatus) fileStatus.textContent = "Using saved/sample data. Upload workbook to refresh.";
  }

  setupUpload((nextModel) => {
    renderDashboard(nextModel);
  });
}

function init() {
  const page = document.body.dataset.page;
  const model = loadModel();

  if (page === "dashboard") {
    initDashboard();
  }

  if (page === "daily-report") {
    renderDailyReport(model);
  }

  if (page === "glossary") {
    renderGlossary();
  }
}

document.addEventListener("DOMContentLoaded", init);
