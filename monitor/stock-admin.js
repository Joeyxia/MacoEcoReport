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
    trainHistory: "训练历史",
    recTitle: "文件识别结果",
    loadedFiles: "已加载文件",
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
    trainHistory: "Training History",
    recTitle: "File Recognition",
    loadedFiles: "Loaded Files",
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
  setText("train-history-title", t("trainHistory"));
  setText("file-rec-title", t("recTitle"));
  const loaded = el("loaded-files-box");
  if (loaded && !loaded.dataset.title) loaded.dataset.title = t("loadedFiles");
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

function renderTrainHistory(rows){
  const root = el("train-history-table");
  if (!root) return;
  if (!rows?.length){
    root.innerHTML = `<p class="subtle">${t("noData")}</p>`;
    return;
  }
  root.innerHTML = `
    <table>
      <thead><tr><th>ID</th><th>Ticker</th><th>Run Time</th><th>Version</th><th>Status</th><th>Samples</th><th>Features</th><th>Notes</th></tr></thead>
      <tbody>
        ${rows.map((r) => `
          <tr>
            <td>${r.id || ""}</td>
            <td>${r.ticker || ""}</td>
            <td>${r.run_time || ""}</td>
            <td>${r.model_version || ""}</td>
            <td>${r.status || ""}</td>
            <td>${r.sample_count || ""}</td>
            <td>${r.feature_count || ""}</td>
            <td>${r.notes || ""}</td>
          </tr>
        `).join("")}
      </tbody>
    </table>
  `;
}

function renderRecognition(rows){
  const root = el("file-rec-table");
  if (!root) return;
  if (!rows?.length){
    root.innerHTML = `<p class="subtle">${t("noData")}</p>`;
    return;
  }
  root.innerHTML = `
    <table>
      <thead><tr><th>File</th><th>Type</th><th>Rows</th><th>Columns</th><th>OK</th></tr></thead>
      <tbody>
        ${rows.map((r) => `
          <tr>
            <td>${r.file || ""}</td>
            <td>${r.type || ""}</td>
            <td>${r.rowCount || 0}</td>
            <td>${r.error ? (r.error || "") : (r.columns || []).length}</td>
            <td>${r.ok ? "Yes" : "No"}</td>
          </tr>
        `).join("")}
      </tbody>
    </table>
  `;
}

function renderLoadedFiles(files){
  const root = el("loaded-files-box");
  if (!root) return;
  const list = Array.from(files || []);
  if (!list.length){
    root.innerHTML = `<p class="subtle">${t("loadedFiles")}: ${t("noData")}</p>`;
    return;
  }
  root.innerHTML = `
    <p class="subtle">${t("loadedFiles")} (${list.length})</p>
    <div class="loaded-files-list">
      ${list.map((f) => `<span class="loaded-file-item">${f.name}</span>`).join("")}
    </div>
  `;
}

function inferTickerFromFiles(files){
  const list = Array.from(files || []);
  for (const f of list){
    const name = String(f?.name || "").trim();
    if (!name) continue;
    const base = name.split("/").pop() || name;
    const first = base.split(/[._\\-\\s]/)[0] || "";
    const code = first.toUpperCase().replace(/[^A-Z0-9]/g, "");
    if (/^[A-Z0-9]{1,8}$/.test(code)) return code;
  }
  return "";
}

function setProgress(pct){
  const p = Math.max(0, Math.min(100, Number(pct || 0)));
  const bar = el("upload-progress-bar");
  if (bar) bar.style.width = `${p}%`;
  setText("upload-progress-text", `${Math.round(p)}%`);
}

async function refreshPanels(){
  const ticker = (el("ticker-input")?.value || "PDD").trim().toUpperCase();
  const [status, tStatus, history, runs] = await Promise.all([
    api.get("/monitor-api/stocks/admin/data-status"),
    api.get(`/monitor-api/stocks/admin/tickers/${encodeURIComponent(ticker)}/status`),
    api.get(`/monitor-api/stocks/admin/upload-history?limit=60&ticker=${encodeURIComponent(ticker)}`),
    api.get(`/monitor-api/stocks/admin/train-history?limit=60&ticker=${encodeURIComponent(ticker)}`),
  ]);
  renderKV("db-status-grid", {
    ...(status?.counts || {}),
    ...Object.fromEntries(Object.entries(status?.latest_dates || {}).map(([k, v]) => [`latest_${k}`, v])),
  });
  renderKV("ticker-status-grid", tStatus || {});
  renderUploadHistory(history?.rows || []);
  renderTrainHistory(runs?.rows || []);
}

function uploadWithProgress(path, formData){
  return new Promise((resolve) => {
    const xhr = new XMLHttpRequest();
    xhr.open("POST", path, true);
    xhr.withCredentials = true;
    xhr.upload.onprogress = (evt) => {
      if (evt.lengthComputable) setProgress((evt.loaded / evt.total) * 100.0);
    };
    xhr.onreadystatechange = () => {
      if (xhr.readyState !== 4) return;
      let data = null;
      try { data = JSON.parse(xhr.responseText || "{}"); } catch (_) {}
      resolve({ ok: xhr.status >= 200 && xhr.status < 300, data, status: xhr.status });
    };
    xhr.send(formData);
  });
}

async function doUpload(){
  setProgress(0);
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
  const res = await uploadWithProgress("/monitor-api/stocks/admin/import-and-refresh", fd);
  setProgress(100);
  if (!res.ok){
    if (Number(res.status) === 413){
      setText("upload-status", "failed: 413 (payload too large, please reduce file size or increase server upload limit)");
    } else {
      setText("upload-status", `failed: ${res.data?.error || res.status}`);
    }
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

async function inspectFiles(){
  const files = el("csv-files")?.files;
  renderLoadedFiles(files);
  const guessed = inferTickerFromFiles(files);
  if (!el("ticker-input")?.value?.trim() && guessed){
    el("ticker-input").value = guessed;
  }
  const ticker = (el("ticker-input")?.value || guessed || "").trim().toUpperCase();
  if (!ticker || !files?.length){
    renderRecognition([]);
    return;
  }
  const fd = new FormData();
  fd.append("ticker", ticker);
  fd.append("mode", "inspect");
  for (const f of files) fd.append("files", f);
  const res = await api.postForm("/monitor-api/stocks/admin/upload-csv", fd);
  if (!res?.ok){
    renderRecognition([{ file: "request", type: "error", rowCount: 0, columns: [], ok: false, error: res?.data?.error || String(res?.status || "") }]);
    if (Number(res?.status) === 413){
      setText("upload-status", "inspect failed: 413 (payload too large)");
    } else {
      setText("upload-status", `inspect failed: ${res?.data?.error || res?.status || "unknown"}`);
    }
    return;
  }
  renderRecognition(res?.data?.recognized || []);
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
  el("csv-files")?.addEventListener("change", inspectFiles);
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
