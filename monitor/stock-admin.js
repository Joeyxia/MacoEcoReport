const STOCK_ADM_LANG_KEY = "nexo-monitor-lang";

const admI18n = {
  zh: {
    title: "Nexo Monitor | 股票数据管理",
    admTitle: "股票 CSV 上传与模型管理",
    admDesc: "上传 CSV → 入库 → 训练模型 → 同步到主站股票预测页面。",
    pickFiles: "选择 CSV 文件",
    autoRefresh: "导入后自动训练",
    upload: "上传并导入",
    train: "仅训练模型",
    dbStatus: "数据库状态",
    tickerStatus: "Ticker 状态",
    uploadHistory: "上传与导入历史",
    loading: "加载中...",
    noData: "暂无数据",
  },
  en: {
    title: "Nexo Monitor | Stock Admin",
    admTitle: "Stock CSV Upload & Model Management",
    admDesc: "Upload CSV -> import DB -> train model -> sync to main stock page.",
    pickFiles: "Pick CSV Files",
    autoRefresh: "Auto train after import",
    upload: "Upload & Import",
    train: "Train Only",
    dbStatus: "Database Status",
    tickerStatus: "Ticker Status",
    uploadHistory: "Upload/Import History",
    loading: "Loading...",
    noData: "No data",
  },
};

function lang(){
  return localStorage.getItem(STOCK_ADM_LANG_KEY) === "en" ? "en" : "zh";
}
function t(k){
  return (admI18n[lang()] || admI18n.zh)[k] || k;
}

const api = {
  get: async (path) => {
    const r = await fetch(path, { credentials: "include" });
    if (!r.ok) return null;
    return r.json();
  },
  postJson: async (path, body = {}) => {
    const r = await fetch(path, {
      method: "POST",
      credentials: "include",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    let j = null;
    try { j = await r.json(); } catch (_) {}
    return { ok: r.ok, data: j, status: r.status };
  },
  postForm: async (path, formData) => {
    const r = await fetch(path, { method: "POST", credentials: "include", body: formData });
    let j = null;
    try { j = await r.json(); } catch (_) {}
    return { ok: r.ok, data: j, status: r.status };
  },
};

function el(id){ return document.getElementById(id); }
function setText(id, txt){ const n = el(id); if (n) n.textContent = txt; }

function ensureFooter(){
  if (document.querySelector(".site-footer-note")) return;
  const footer = document.createElement("footer");
  footer.className = "site-footer-note";
  footer.textContent = lang() === "en" ? "Powered by Nexo Marco Intelligence" : "由 Nexo Marco Intelligence 提供支持";
  document.body.appendChild(footer);
}

function applyI18n(){
  document.title = t("title");
  setText("adm-title", t("admTitle"));
  setText("adm-desc", t("admDesc"));
  setText("pick-files-label", t("pickFiles"));
  setText("auto-refresh-label", t("autoRefresh"));
  setText("upload-btn", t("upload"));
  setText("train-btn", t("train"));
  setText("data-status-title", t("dbStatus"));
  setText("ticker-status-title", t("tickerStatus"));
  setText("upload-history-title", t("uploadHistory"));
  const btn = el("lang-toggle");
  if (btn) btn.textContent = lang() === "zh" ? "EN" : "中文";
}

async function ensureAuth(){
  const me = await api.get("/monitor-api/auth/me");
  if (!me?.ok || !me?.user?.email){
    location.href = "./index.html";
    return null;
  }
  setText("user-email", me.user.email);
  return me.user;
}

function renderKV(rootId, obj){
  const root = el(rootId);
  if (!root) return;
  const keys = Object.keys(obj || {});
  if (!keys.length){
    root.innerHTML = `<p class="subtle">${t("noData")}</p>`;
    return;
  }
  root.innerHTML = keys.map((k) => `<div class="list-row"><strong>${k}</strong><span>${obj[k]}</span></div>`).join("");
}

function renderUploadHistory(rows){
  const root = el("upload-history-table");
  if (!root) return;
  if (!rows?.length){
    root.innerHTML = `<p class="subtle">${t("noData")}</p>`;
    return;
  }
  root.innerHTML = `
    <table>
      <thead><tr><th>ID</th><th>Ticker</th><th>File</th><th>Type</th><th>Status</th><th>Uploaded</th><th>Imported</th><th>Notes</th></tr></thead>
      <tbody>
        ${rows.map((r) => `
          <tr>
            <td>${r.id || ""}</td>
            <td>${r.ticker || ""}</td>
            <td>${r.file_name || ""}</td>
            <td>${r.file_type || ""}</td>
            <td>${r.file_status || ""}</td>
            <td>${r.uploaded_at || ""}</td>
            <td>${r.imported_at || ""}</td>
            <td>${r.notes || ""}</td>
          </tr>
        `).join("")}
      </tbody>
    </table>
  `;
}

async function refreshPanels(){
  const ticker = (el("ticker-input")?.value || "PDD").trim().toUpperCase();
  const [status, tStatus, history] = await Promise.all([
    api.get("/monitor-api/stocks/admin/data-status"),
    api.get(`/monitor-api/stocks/admin/tickers/${encodeURIComponent(ticker)}/status`),
    api.get(`/monitor-api/stocks/admin/upload-history?limit=60&ticker=${encodeURIComponent(ticker)}`),
  ]);
  renderKV("db-status-grid", {
    ...(status?.counts || {}),
    ...Object.fromEntries(Object.entries(status?.latest_dates || {}).map(([k, v]) => [`latest_${k}`, v])),
  });
  renderKV("ticker-status-grid", tStatus || {});
  renderUploadHistory(history?.rows || []);
}

async function doUpload(){
  const ticker = (el("ticker-input")?.value || "").trim().toUpperCase();
  const files = el("csv-files")?.files;
  if (!ticker){
    setText("upload-status", "ticker required");
    return;
  }
  if (!files?.length){
    setText("upload-status", "csv files required");
    return;
  }
  const fd = new FormData();
  fd.append("ticker", ticker);
  fd.append("autoRefresh", String(!!el("auto-refresh")?.checked));
  for (const f of files) fd.append("files", f);
  setText("upload-status", t("loading"));
  const res = await api.postForm("/monitor-api/stocks/admin/upload-csv", fd);
  if (!res.ok){
    setText("upload-status", `failed: ${res.data?.error || res.status}`);
    return;
  }
  setText("upload-status", `ok: imported=${res.data?.importedCount || 0}, failed=${res.data?.failedCount || 0}, rows=${res.data?.totalRows || 0}`);
  await refreshPanels();
}

async function doTrain(){
  const ticker = (el("ticker-input")?.value || "").trim().toUpperCase();
  if (!ticker){
    setText("upload-status", "ticker required");
    return;
  }
  setText("upload-status", t("loading"));
  const res = await api.postJson(`/monitor-api/stocks/admin/refresh/${encodeURIComponent(ticker)}`, {});
  if (!res.ok){
    setText("upload-status", `train failed: ${res.data?.error || res.status}`);
    return;
  }
  setText("upload-status", `train ok: runId=${res.data?.runId || 0}`);
  await refreshPanels();
}

function bindEvents(){
  el("upload-btn")?.addEventListener("click", doUpload);
  el("train-btn")?.addEventListener("click", doTrain);
  el("logout-btn")?.addEventListener("click", async () => {
    await api.postJson("/monitor-api/auth/logout", {});
    location.href = "./index.html";
  });
  el("lang-toggle")?.addEventListener("click", () => {
    localStorage.setItem(STOCK_ADM_LANG_KEY, lang() === "zh" ? "en" : "zh");
    applyI18n();
    ensureFooter();
  });
}

async function init(){
  applyI18n();
  ensureFooter();
  const ok = await ensureAuth();
  if (!ok) return;
  bindEvents();
  await refreshPanels();
}

document.addEventListener("DOMContentLoaded", init);
