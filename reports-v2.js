/* ───────────────────────────────────────────────
   reports-v2.js · Reports archive page renderer (step 2E)
   Renders the v2 sidebar+layout reports view from 3 backend endpoints.
   Reuses i18n (t() / getLang() / setLang()) from app.js. Does NOT trust
   AI-generated content for innerHTML — every aiAnalysis/meta string flows
   through escapeHtml() before injection (step 2A Brent fabrication +
   prompt-injection defence; AI authorship is not a trust signal).
   Co-exists with capital-warning.js renderDailyCapitalWarning via 6
   hidden #report-*-block shim divs declared in daily-report.html.
   ─────────────────────────────────────────────── */
(() => {
  if (document.body?.dataset?.page !== "daily-report") return;

  // ─── Sticky same-origin API base ───────────────────────────────────
  // Same idiom as PR #5 / #10 / dashboard-v2 / markets-v2 / geo-v2.
  const API = (() => {
    const host = location.hostname;
    if (host === "nexo.hk" || host === "www.nexo.hk" ||
        host === "localhost" || host === "127.0.0.1") return "";
    return (document.querySelector('meta[name="macro-api-base"]')?.content
      || "").trim() || "https://api.nexo.hk";
  })();

  // ─── DOM + i18n helpers ────────────────────────────────────────────
  const $ = (id) => document.getElementById(id);
  const $$ = (sel, root = document) => Array.from(root.querySelectorAll(sel));
  const isZh = () => (typeof getLang === "function" ? getLang() : "zh") === "zh";
  const tr = (key, fallback = "") => {
    if (typeof t === "function") {
      const v = t(key);
      if (v) return v;
    }
    return fallback;
  };
  const setText = (el, text) => { if (el) el.textContent = text; };
  const safe = (v) => (v === null || v === undefined || v === "" ? "--" : String(v));

  function timed(name, fn) {
    const t0 = performance.now();
    try { fn(); }
    finally { console.debug(`[reports-v2] ${name} ${Math.round(performance.now() - t0)}ms`); }
  }

  // ─── XSS defence ───────────────────────────────────────────────────
  // Even AI-generated content is untrusted: prompt-injection can plant
  // <script> in aiAnalysis fields, and step 2A surfaced backend data
  // fabrication (Brent prices) — so internal data is not trusted either.
  // Every string that reaches innerHTML MUST flow through escapeHtml first.
  function escapeHtml(str) {
    return String(str ?? "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }

  // Lite markdown: paragraph split on \n\n + **bold** → <strong>. NO heading
  // / list / link parsing — those would widen the XSS surface (raw href
  // would need URL scheme validation; heading IDs would let injected text
  // collide with our own IDs). Caller passes raw markdown; we escape first.
  function renderMarkdownLite(str) {
    const escaped = escapeHtml(str);
    return escaped
      .split(/\n{2,}/)
      .slice(0, 3)
      .filter((p) => p.trim())
      .map((p) => `<p>${p.replace(/\*\*(.+?)\*\*/g, "<strong>$1</strong>")}</p>`)
      .join("");
  }

  // ─── Status classifier ─────────────────────────────────────────────
  // Map a report row to one of 4 chip colours (orange/yellow/green/red).
  // Primary: max alert_level across reportPayload.alertSnapshots.
  // Fallback chain (when alertSnapshots absent on legacy rows): score
  // threshold buckets per m1 decision point f.
  function classifyStatus(row) {
    const snaps = row?.reportPayload?.alertSnapshots;
    if (Array.isArray(snaps) && snaps.length) {
      let maxIdx = 0;
      for (const s of snaps) {
        const lvl = String(s?.alert_level || "").toLowerCase();
        let n = 0;
        if (lvl === "red" || lvl === "critical" || lvl === "l3") n = 3;
        else if (lvl === "orange" || lvl === "high" || lvl === "l2") n = 2;
        else if (lvl === "yellow" || lvl === "medium" || lvl === "l1") n = 1;
        else if (lvl === "green" || lvl === "low" || lvl === "normal" || lvl === "l0") n = 0;
        else {
          const num = parseInt(lvl, 10);
          if (Number.isFinite(num)) n = Math.max(0, Math.min(3, num));
        }
        if (n > maxIdx) maxIdx = n;
      }
      return ["green", "yellow", "orange", "red"][maxIdx];
    }
    // Fallback: score thresholds (m1 decision point f, second branch).
    const score = Number(row?.meta?.score);
    if (!Number.isFinite(score)) return "yellow"; // neutral default
    if (score >= 75) return "green";
    if (score >= 60) return "yellow";
    if (score >= 50) return "orange";
    return "red";
  }

  // ─── Network ───────────────────────────────────────────────────────
  async function api(path) {
    const t0 = performance.now();
    const url = path.startsWith("/api/") ? path : API + path;
    try {
      const r = await fetch(url, { credentials: "include", cache: "no-store" });
      const dt = Math.round(performance.now() - t0);
      if (!r.ok) {
        console.debug(`[reports-v2] fetch ${path} failed ${r.status} in ${dt}ms`);
        return { _err: r.status };
      }
      const json = await r.json();
      console.debug(`[reports-v2] fetch ${path} ok in ${dt}ms`);
      return json;
    } catch (e) {
      const dt = Math.round(performance.now() - t0);
      console.debug(`[reports-v2] fetch ${path} threw in ${dt}ms: ${e?.message || e}`);
      return { _err: String(e?.message || e) };
    }
  }

  // ─── Fetch chain ───────────────────────────────────────────────────
  // 3 endpoints (m1 decision: simplified from 6 → 3 after probe-reports
  // confirmed /api/reports list already inlines full reportPayload).
  // /api/email-deliveries 404 is expected (issue #19) — _err short-circuits
  // renderAudit to keep the static graceful-empty defaults.
  async function fetchAll() {
    const t0 = performance.now();
    console.debug(`[reports-v2] fetchAll start`);

    const results = await Promise.allSettled([
      api("/api/reports?limit=60"),
      api("/api/subscribers"),
      api("/api/email-deliveries"),
    ]);
    const [reportsRes, subscribersRes, deliveriesRes] = results.map(
      (r) => (r.status === "fulfilled"
        ? r.value
        : { _err: String(r.reason?.message || r.reason) })
    );

    const errCount = [reportsRes, subscribersRes, deliveriesRes]
      .filter((x) => x?._err !== undefined).length;
    const dt = Math.round(performance.now() - t0);
    console.debug(`[reports-v2] fetchAll done in ${dt}ms, errors: ${errCount}/3 (email-deliveries 404 expected per issue #19)`);

    return {
      reports: Array.isArray(reportsRes?.reports) ? reportsRes.reports : [],
      subscribers: subscribersRes,
      deliveries: deliveriesRes,
    };
  }

  // ─── State ─────────────────────────────────────────────────────────
  const state = {
    reports: [],
    selectedDate: null,
    filters: { status: "all", search: "", dateFrom: "", dateTo: "" },
  };

  let searchDebounceTimer = null;

  // ─── Filter logic ──────────────────────────────────────────────────
  // In-memory, walks .rl-row[data-date|data-status|data-summary]. Search
  // is debounced 200ms via the input handler — at limit=60 even no-debounce
  // is fine, but keeping it cheap for future expansion to limit=200+.
  function applyFilters() {
    const { status, search, dateFrom, dateTo } = state.filters;
    const q = search.trim().toLowerCase();
    for (const row of $$("#reports-list .rl-row")) {
      const rowStatus = row.dataset.status || "yellow";
      const rowDate = row.dataset.date || "";
      const rowSummary = (row.dataset.summary || "").toLowerCase();
      let visible = true;
      if (status !== "all" && rowStatus !== status) visible = false;
      if (q && !rowSummary.includes(q)) visible = false;
      if (dateFrom && rowDate && rowDate < dateFrom) visible = false;
      if (dateTo && rowDate && rowDate > dateTo) visible = false;
      row.hidden = !visible;
    }
  }

  // ─── Renderers ─────────────────────────────────────────────────────

  function renderTopBar(data) {
    const r0 = data.reports[0];
    const last = data.reports[data.reports.length - 1];
    setText($("status-run-id"), safe(r0?.reportPayload?.runId));
    // Backend has no clean exposure for latency / fresh-count on this
    // page; match geo-v2 / markets-v2 behaviour (left as static "--").
    setText($("status-latency"), "--");
    setText($("status-fresh"), "--");

    setText($("meta-reports-total"), String(data.reports.length || "--"));
    setText($("meta-reports-since"), safe(last?.date));
  }

  function renderHero(data) {
    const r0 = data.reports[0];
    setText($("chip-reports-today"),
      r0 ? `#${safe(r0?.reportPayload?.runId)} · ${r0.date}` : "--");
    setText($("chip-reports-total"), String(data.reports.length || "--"));
    setText($("chip-subscribers"), safe(data.subscribers?.count));
    // chip-avg-ai: API has no aggregate AI latency field — keep "--"
    // (m1 graceful empty matrix row 2). Could be derived from per-row
    // aiAnalysis.generated_at deltas, but that's misleading without a
    // sampling strategy; leave to follow-up.
    setText($("chip-avg-ai"), "--");
  }

  function renderFilterRow(data) {
    // Date range defaults: span the visible result set (oldest → newest).
    const last = data.reports[data.reports.length - 1];
    const first = data.reports[0];
    const fromEl = $("filter-date-from");
    const toEl   = $("filter-date-to");
    if (fromEl && last?.date) { fromEl.value = last.date; state.filters.dateFrom = last.date; }
    if (toEl   && first?.date){ toEl.value   = first.date; state.filters.dateTo   = first.date; }

    // Search input — debounced 200ms.
    const search = $("filter-search");
    if (search && !search.dataset.reportsV2Bound) {
      search.dataset.reportsV2Bound = "1";
      search.addEventListener("input", () => {
        clearTimeout(searchDebounceTimer);
        searchDebounceTimer = setTimeout(() => {
          state.filters.search = search.value || "";
          applyFilters();
        }, 200);
      });
    }

    // Date inputs.
    if (fromEl && !fromEl.dataset.reportsV2Bound) {
      fromEl.dataset.reportsV2Bound = "1";
      fromEl.addEventListener("change", () => {
        state.filters.dateFrom = fromEl.value || "";
        applyFilters();
      });
    }
    if (toEl && !toEl.dataset.reportsV2Bound) {
      toEl.dataset.reportsV2Bound = "1";
      toEl.addEventListener("change", () => {
        state.filters.dateTo = toEl.value || "";
        applyFilters();
      });
    }

    // Status chips.
    for (const chip of $$(".filter-chip[data-status]")) {
      if (chip.dataset.reportsV2Bound) continue;
      chip.dataset.reportsV2Bound = "1";
      chip.addEventListener("click", () => {
        for (const c of $$(".filter-chip[data-status]")) c.classList.remove("active");
        chip.classList.add("active");
        state.filters.status = chip.dataset.status || "all";
        applyFilters();
      });
    }

    // Export button — no backend endpoint; stub for now (follow-up).
    const exportBtn = $("filter-export-btn");
    if (exportBtn && !exportBtn.dataset.reportsV2Bound) {
      exportBtn.dataset.reportsV2Bound = "1";
      exportBtn.addEventListener("click", () => {
        console.debug("[reports-v2] export csv click (no-op; needs follow-up)");
      });
    }
  }

  function renderList(data) {
    state.reports = data.reports;
    const tmpl = $("rl-row-template");
    const root = $("reports-list");
    if (!tmpl || !root) return;
    root.replaceChildren();

    data.reports.forEach((row, idx) => {
      const node = tmpl.content.firstElementChild.cloneNode(true);
      const status = classifyStatus(row);
      const regimeCode = row?.reportPayload?.regime?.regime_code || "--";
      const summary = String(row?.meta?.summary || "");
      const score = Number(row?.meta?.score);
      const runId = row?.reportPayload?.runId;

      node.dataset.date = row.date || "";
      node.dataset.status = status;
      // data-summary feeds in-memory search; lowercase for case-insensitive
      // includes() at filter time.
      node.dataset.summary = (regimeCode + " " + summary).toLowerCase();

      node.querySelector(".when").textContent = (row.date || "").slice(5) || "--";
      const pill = node.querySelector(".pill");
      pill.classList.remove("muted");
      pill.classList.add(
        status === "red" ? "bad" :
        status === "orange" ? "warn" :
        status === "yellow" ? "warn" :
        "muted"
      );
      pill.textContent = tr(`filter_${status}`, status);
      node.querySelector(".regime-text").textContent = regimeCode;
      node.querySelector(".score").textContent =
        `score ${Number.isFinite(score) ? score.toFixed(2) : "--"}` +
        (runId != null ? ` · #${runId}` : "");

      if (idx === 0) {
        node.classList.add("selected");
        state.selectedDate = row.date;
      }

      node.addEventListener("click", () => {
        for (const r of $$("#reports-list .rl-row")) r.classList.remove("selected");
        node.classList.add("selected");
        state.selectedDate = row.date;
        renderDetail(row);
      });

      root.appendChild(node);
    });

    if (data.reports[0]) renderDetail(data.reports[0]);
  }

  function renderDetail(row) {
    const root = $("report-detail");
    if (!root) return;

    const date = row.date || "--";
    const runId = row?.reportPayload?.runId;
    const score = Number(row?.meta?.score);
    const status = classifyStatus(row);
    const regimeCode = row?.reportPayload?.regime?.regime_code || row?.meta?.status || "--";
    const aiStatus = row?.aiAnalysis?.status || "--";
    const aiModel = row?.aiAnalysis?.model || "";
    const headline = isZh()
      ? (row?.aiAnalysis?.short_summary_zh || row?.aiAnalysis?.short_summary || row?.meta?.summary || "")
      : (row?.aiAnalysis?.short_summary_en || row?.aiAnalysis?.short_summary || row?.meta?.summary || "");
    const detailedMd = isZh()
      ? (row?.aiAnalysis?.detailed_interpretation_zh || row?.aiAnalysis?.detailed_interpretation || "")
      : (row?.aiAnalysis?.detailed_interpretation_en || row?.aiAnalysis?.detailed_interpretation || "");

    const drivers = Array.isArray(row?.reportPayload?.primaryDrivers)
      ? row.reportPayload.primaryDrivers.slice(0, 3) : [];
    const alerts = Array.isArray(row?.reportPayload?.triggerAlerts)
      ? row.reportPayload.triggerAlerts.slice(0, 3) : [];
    const bias = row?.reportPayload?.actionBias || null;

    const htmlPath = row.path
      ? `<a class="btn-ghost btn" href="${escapeHtml(row.path)}" target="_blank" rel="noopener" data-i18n="rd_btn_html">↗ html</a>`
      : `<button class="btn-ghost btn" type="button" disabled data-i18n="rd_btn_html">↗ html</button>`;

    const driversBlock = drivers.length ? `
      <div class="rd-section">
        <h4 data-i18n="rd_top_drivers">主要驱动因子</h4>
        <ul>${drivers.map((d) => `<li><strong>${escapeHtml(d?.title || "")}</strong> — ${escapeHtml(d?.text || "")}</li>`).join("")}</ul>
      </div>` : "";

    const alertsBlock = alerts.length ? `
      <div class="rd-section">
        <h4 data-i18n="rd_alerts">触发的警报</h4>
        <ul>${alerts.map((a) => `<li><span class="kbd">${escapeHtml(a?.id || "")}</span> ${escapeHtml(a?.condition || "")} → ${escapeHtml(a?.level || "")}${a?.triggered ? "" : ` <span class="muted">(${escapeHtml(isZh() ? "未触发" : "below")})</span>`}</li>`).join("")}</ul>
      </div>` : "";

    const stanceItems = bias ? [
      bias.overall_bias ? `<li><span data-i18n="rd_stance_direction">方向</span>：<strong>${escapeHtml(bias.overall_bias)}</strong></li>` : "",
      (Array.isArray(bias.favored_sectors) && bias.favored_sectors.length)
        ? `<li><span data-i18n="rd_stance_favored">偏好行业</span>：${bias.favored_sectors.map(escapeHtml).join(", ")}</li>` : "",
      (Array.isArray(bias.avoided_sectors) && bias.avoided_sectors.length)
        ? `<li><span data-i18n="rd_stance_avoided">回避</span>：${bias.avoided_sectors.map(escapeHtml).join(", ")}</li>` : "",
      (Array.isArray(bias.favored_styles) && bias.favored_styles.length)
        ? `<li><span data-i18n="rd_stance_hedge">对冲偏好</span>：${bias.favored_styles.map(escapeHtml).join(", ")}</li>` : "",
    ].filter(Boolean) : [];
    const stanceBlock = stanceItems.length ? `
      <div class="rd-section">
        <h4 data-i18n="rd_stance">推荐立场</h4>
        <ul>${stanceItems.join("")}</ul>
      </div>` : "";

    const alertColor = status === "red" ? "var(--bad)" :
                       status === "orange" ? "var(--warn)" :
                       status === "yellow" ? "var(--warn)" : "var(--good)";

    root.innerHTML = `
      <div class="rd-head">
        <h2><span data-i18n="rd_run">Daily report</span> <span class="date">${escapeHtml(date)}${runId != null ? ` · run #${escapeHtml(runId)}` : ""}</span></h2>
        <div class="rd-actions">
          ${htmlPath}
          <button class="btn-ghost btn" type="button" disabled data-i18n="rd_btn_pdf">↗ pdf</button>
          <button class="btn" type="button" id="rd-btn-rerun" data-i18n="rd_btn_rerun">重跑 · 重发邮件</button>
        </div>
      </div>
      ${headline ? `<div class="rd-headline">${escapeHtml(headline)}</div>` : ""}
      <div class="rd-meta">
        <div class="item"><div class="k" data-i18n="rd_meta_score">封顶后分数</div><div class="v">${Number.isFinite(score) ? score.toFixed(2) : "--"}</div></div>
        <div class="item"><div class="k" data-i18n="rd_meta_regime">Regime</div><div class="v" style="color: var(--warn)">${escapeHtml(regimeCode)}</div></div>
        <div class="item"><div class="k" data-i18n="rd_meta_alert">警报</div><div class="v" style="color: ${alertColor}">${escapeHtml(status)}</div></div>
        <div class="item"><div class="k" data-i18n="rd_meta_ai">AI 状态</div><div class="v" style="color: var(--good)">${escapeHtml(aiStatus)}${aiModel ? ` · ${escapeHtml(aiModel)}` : ""}</div></div>
      </div>
      ${detailedMd ? `<div class="rd-summary">${renderMarkdownLite(detailedMd)}</div>` : ""}
      ${driversBlock}
      ${alertsBlock}
      ${stanceBlock}
    `;

    // Re-translate freshly-injected [data-i18n] keys.
    applyI18n();

    // Rerun button: stub (no backend endpoint exposed for client-triggered
    // rerun + email resend). Logs for diagnostic; future scope.
    const rerun = $("rd-btn-rerun");
    if (rerun) {
      rerun.addEventListener("click", () => {
        console.debug(`[reports-v2] rerun click for ${date} (no-op; needs follow-up)`);
      });
    }
  }

  function renderEmail(data) {
    const btn = $("email-subscribe-btn");
    const input = $("email-input");
    if (!btn || !input || btn.dataset.reportsV2Bound) return;
    btn.dataset.reportsV2Bound = "1";

    btn.addEventListener("click", async () => {
      const email = String(input.value || "").trim();
      if (!email || !email.includes("@")) {
        console.debug("[reports-v2] subscribe: invalid email");
        input.focus();
        return;
      }
      const t0 = performance.now();
      btn.disabled = true;
      try {
        const r = await fetch(`${API}/api/subscribers`, {
          method: "POST",
          credentials: "include",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ email }),
        });
        const dt = Math.round(performance.now() - t0);
        if (r.ok) {
          console.debug(`[reports-v2] subscribe ${email} ok in ${dt}ms`);
          input.value = "";
        } else if (r.status === 409) {
          console.debug(`[reports-v2] subscribe ${email} already subscribed (409)`);
        } else if (r.status === 401) {
          console.debug(`[reports-v2] subscribe needs login (401)`);
        } else {
          console.debug(`[reports-v2] subscribe failed ${r.status} in ${dt}ms`);
        }
      } catch (e) {
        console.debug(`[reports-v2] subscribe threw: ${e?.message || e}`);
      } finally {
        btn.disabled = false;
      }
    });
  }

  function renderAudit(data) {
    // Issue #19: /api/email-deliveries returns 404. _err short-circuits us
    // to the static HTML defaults (warn header + 5 muted placeholder rows).
    // When the backend lands, the success branch below activates.
    if (data.deliveries?._err !== undefined) {
      // Subscriber count is still useful for audit meta — overwrite the
      // i18n default with a real count when subscribers endpoint worked.
      const count = data.subscribers?.count;
      const metaEl = $("audit-reports-meta");
      if (metaEl && count != null) {
        metaEl.removeAttribute("data-i18n");
        metaEl.textContent = isZh()
          ? `${count} 个有效订阅 · 今日 — 失败`
          : `${count} active subscribers · today — failed`;
      }
      return;
    }

    // Success branch: hide warn header, replace dispatch log.
    const warn = $("audit-email-warn");
    if (warn) warn.hidden = true;

    const log = $("audit-dispatch-log");
    const items = Array.isArray(data.deliveries?.deliveries)
      ? data.deliveries.deliveries.slice(0, 5) : [];
    if (log && items.length) {
      log.innerHTML = items.map((d) => {
        const date = escapeHtml(d?.date || "--");
        const sent = Number(d?.sent ?? 0);
        const failed = Number(d?.failed ?? 0);
        return `<span class="prompt">▸</span> <span class="key">${date}</span> · <span class="val">${sent} sent / ${failed} failed</span><br>`;
      }).join("");
    }

    const metaEl = $("audit-reports-meta");
    if (metaEl) {
      metaEl.removeAttribute("data-i18n");
      const active = data.deliveries?.active_subscribers ?? data.subscribers?.count ?? "--";
      const todayFailed = data.deliveries?.today_failed ?? 0;
      metaEl.textContent = isZh()
        ? `${active} 个有效订阅 · 今日 ${todayFailed} 失败`
        : `${active} active subscribers · today ${todayFailed} failed`;
    }
  }

  // ─── i18n + lang toggle ────────────────────────────────────────────
  function applyI18n() {
    if (typeof t !== "function") return;
    for (const el of $$("[data-i18n]")) {
      const key = el.getAttribute("data-i18n");
      const v = t(key);
      if (v) el.textContent = v;
    }
    for (const el of $$("[data-i18n-placeholder]")) {
      const key = el.getAttribute("data-i18n-placeholder");
      const v = t(key);
      if (v) el.setAttribute("placeholder", v);
    }
    const toggle = $("lang-toggle");
    if (toggle) toggle.textContent = isZh() ? "EN" : "中文";
  }

  // Piggyback ONLY — app.js's setupLangToggle is the canonical handler.
  // Binding our own setLang here would create the step 2C dual-handler
  // bug (handlers cancel each other out, net visible state never flips).
  // Schedule on next macrotask so getLang() reflects the post-click value.
  // Also re-render the currently-selected detail panel since some of its
  // dynamic strings (headline / drivers / etc.) source from *_zh vs *_en
  // and need a fresh render to swap language.
  function setupLangToggle() {
    const btn = $("lang-toggle");
    if (!btn || btn.dataset.reportsV2Bound) return;
    btn.dataset.reportsV2Bound = "1";
    btn.addEventListener("click", () => {
      setTimeout(() => {
        btn.textContent = isZh() ? "EN" : "中文";
        const cur = state.reports.find((r) => r.date === state.selectedDate);
        if (cur) renderDetail(cur);
      }, 0);
    });
  }

  // ─── Boot ──────────────────────────────────────────────────────────
  document.addEventListener("DOMContentLoaded", async () => {
    console.debug(`[reports-v2] boot start | API base = ${API || "<same-origin>"}`);
    applyI18n();
    setupLangToggle();
    try {
      const data = await fetchAll();
      console.debug("[reports-v2] render begin");
      timed("renderTopBar",    () => renderTopBar(data));
      timed("renderHero",      () => renderHero(data));
      timed("renderFilterRow", () => renderFilterRow(data));
      timed("renderList",      () => renderList(data));
      timed("renderEmail",     () => renderEmail(data));
      timed("renderAudit",     () => renderAudit(data));
      // Re-apply: renderList / renderDetail injected [data-i18n] keys via
      // innerHTML — sweep again to translate.
      applyI18n();
      console.debug("[reports-v2] render end");
    } catch (e) {
      console.error("[reports-v2] boot failed", e);
    } finally {
      document.body.setAttribute("data-render-state", "ready");
      console.debug("[reports-v2] data-render-state=ready");
    }
  });
})();
