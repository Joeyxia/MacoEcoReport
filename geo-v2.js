/* ───────────────────────────────────────────────
   geo-v2.js · Geo watch page renderer (step 2D-2)
   Renders the v2 sidebar+layout geo view from 5 backend endpoints.
   Reuses i18n (t() / getLang() / setLang()) from app.js. Does NOT
   request /api/geo/events / /api/markets/oil-microstructure /
   /api/markets/shipping (known 404 / SSL-timeout — issues #14, #15,
   #16) to avoid noise in console / verify-geo.py.
   No legacy capital-warning.js entanglement: data-page="geo" and the
   dispatch in capital-warning.js (line 1131-1138) has no `geo` branch,
   so the legacy init never fires here. No hidden shim divs needed.
   ─────────────────────────────────────────────── */
(() => {
  if (document.body?.dataset?.page !== "geo") return;

  // ─── Sticky same-origin API base ───────────────────────────────────
  // Same idiom as PR #5 / #10 / dashboard-v2 / markets-v2: prod and
  // local dev fall through to same-origin; cross-subdomain api.nexo.hk
  // only as defensive fallback for unknown hosts (preview/staging).
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
  const safeNum = (v, digits = 1) => {
    const n = Number(v);
    return Number.isFinite(n) ? n.toFixed(digits) : "--";
  };
  const safeRound = (v) => {
    const n = Number(v);
    return Number.isFinite(n) ? String(Math.round(n)) : "--";
  };

  function timed(name, fn) {
    const t0 = performance.now();
    try { fn(); }
    finally { console.debug(`[geo-v2] ${name} ${Math.round(performance.now() - t0)}ms`); }
  }

  // ─── Network ───────────────────────────────────────────────────────
  async function api(path) {
    const t0 = performance.now();
    const url = path.startsWith("/api/") ? path : API + path;
    try {
      const r = await fetch(url, { credentials: "include", cache: "no-store" });
      const dt = Math.round(performance.now() - t0);
      if (!r.ok) {
        console.debug(`[geo-v2] fetch ${path} failed ${r.status} in ${dt}ms`);
        return { _err: r.status };
      }
      const json = await r.json();
      console.debug(`[geo-v2] fetch ${path} ok in ${dt}ms`);
      return json;
    } catch (e) {
      const dt = Math.round(performance.now() - t0);
      console.debug(`[geo-v2] fetch ${path} threw in ${dt}ms: ${e?.message || e}`);
      return { _err: String(e?.message || e) };
    }
  }

  function unwrap(payload) {
    if (!payload || typeof payload !== "object") return {};
    if (payload._err !== undefined) return payload;
    if (payload.item && typeof payload.item === "object") return payload.item;
    return payload;
  }

  // Parse "level_2" / "L2" / "S2" / "2" → 2. Returns null if no digit found.
  function parseLevel(raw) {
    if (raw == null) return null;
    const s = String(raw);
    const m = s.match(/(\d+)/);
    if (!m) return null;
    const n = parseInt(m[1], 10);
    return Number.isFinite(n) ? n : null;
  }

  // ─── Fetch chain ───────────────────────────────────────────────────
  // 5 endpoints. Speculative ones (geo/events, oil-microstructure,
  // shipping) deliberately NOT requested — confirmed 404 / SSL-timeout
  // by 3-round probe (issues #14 #15 #16); requesting them would
  // pollute console and verify-geo.py logs without producing data.
  async function fetchAll() {
    const today = new Date().toISOString().slice(0, 10);
    const t0 = performance.now();
    console.debug(`[geo-v2] fetchAll start, runDate=${today}`);

    const results = await Promise.allSettled([
      api("/api/geopolitical-overlay/latest"),
      api(`/api/run/${today}/geopolitical-overlay`),
      api(`/api/run/${today}/overlay-decision`),
      api("/api/transmission/latest"),
      api("/api/latest-run"),
    ]);
    const [overlayLatest, geoRun, overlay, transmission, latestRun] = results.map(
      (r) => (r.status === "fulfilled"
        ? unwrap(r.value)
        : { _err: String(r.reason?.message || r.reason) })
    );

    const errCount = [overlayLatest, geoRun, overlay, transmission, latestRun]
      .filter((x) => x?._err !== undefined).length;
    const dt = Math.round(performance.now() - t0);
    console.debug(`[geo-v2] fetchAll done in ${dt}ms, errors: ${errCount}/5`);

    return { overlayLatest, geoRun, overlay, transmission, latestRun, runDate: today };
  }

  // ─── Renderers ─────────────────────────────────────────────────────

  function renderTopBar(data) {
    const { latestRun, overlay, geoRun } = data;
    setText($("status-run-id"), safe(latestRun?.run_id ?? overlay?.run_id));
    // status-latency / status-fresh: backend has no clean exposure. Mockup
    // illustrates them; we leave "--" placeholders. (Same as markets.)
    setText($("status-latency"), "--");
    setText($("status-fresh"), "--");

    // top-bar meta: D11 score + overlay level + time
    setText($("meta-d11-score"), safeRound(geoRun?.conflict_intensity_score));

    const lvl = overlay?.overlay_level;
    // "level_2" → "L2"; "S2" → "S2"; "2" → "L2"
    let lvlDisplay = "--";
    if (lvl) {
      const n = parseLevel(lvl);
      lvlDisplay = n != null ? `L${n}` : String(lvl);
    }
    setText($("meta-overlay-level"), lvlDisplay);

    // Time stamp — backend doesn't surface a clean HH:MM string for the
    // run; fall back to as_of_date (date only, no time). Keep "--" if
    // both endpoints failed.
    const ts = latestRun?.as_of_date ?? overlay?.as_of_date ?? data.runDate;
    setText($("meta-time"), safe(ts));
  }

  function renderHero(data) {
    const { overlay, overlayLatest, geoRun } = data;

    // chip-overlay-level: e.g., "level_2"
    const ovLvl = overlay?.overlay_level ?? overlayLatest?.overlay_level;
    setText($("chip-overlay-level"), safe(ovLvl));

    // 3 sub-level chips driven by /api/geopolitical-overlay/latest enums
    // (low / medium / high / etc). Localise via t("geo_v_<value>") if a
    // matching key exists in app.js; fall back to raw enum string.
    // We update both textContent AND data-i18n attr so app.js's applyI18n
    // re-translates on subsequent lang toggles.
    const setLevelChip = (id, level) => {
      if (!level) return;
      const el = $(id);
      if (!el) return;
      el.setAttribute("data-i18n", `geo_v_${level}`);
      el.textContent = tr(`geo_v_${level}`, String(level));
    };
    setLevelChip("chip-conflict-level", overlayLatest?.conflict_level);
    setLevelChip("chip-supply-level",   overlayLatest?.supply_disruption_level);
    setLevelChip("chip-shipping-level", overlayLatest?.shipping_risk_level);

    // chip-d11-score: rounded conflict_intensity_score
    setText($("chip-d11-score"), safeRound(geoRun?.conflict_intensity_score));
  }

  function renderLevelMeter(data) {
    const { overlay } = data;
    const lvlIdx = parseLevel(overlay?.overlay_level);
    if (lvlIdx == null) return; // leave static .active untouched

    // Swap .active onto matching .lv card; strip "（当前）/(current)" marker
    // from the previously-active card and append it to the new one.
    const cards = $$(".level-meter .lv");
    for (const lv of cards) {
      const idx = parseInt(lv.dataset.level, 10);
      lv.classList.toggle("active", idx === lvlIdx);

      const num = lv.querySelector(".num");
      if (!num) continue;
      // Drop any existing trailing "current" marker span + "·" separator
      // (mockup static HTML embeds `· <span data-i18n="geo_lv2_desc_current">`
      // inside L2's .num).
      const prevMarker = num.querySelector('[data-i18n="geo_lv2_desc_current"]');
      if (prevMarker) prevMarker.remove();
      num.innerHTML = num.innerHTML.replace(/\s*·\s*$/, "");

      if (idx === lvlIdx) {
        // i18n key reused even though name has "lv2" — text content is
        // generic ("（当前）" / "(current)") and works for any level.
        num.appendChild(document.createTextNode(" · "));
        const span = document.createElement("span");
        span.setAttribute("data-i18n", "geo_lv2_desc_current");
        span.textContent = tr("geo_lv2_desc_current", isZh() ? "（当前）" : "(current)");
        num.appendChild(span);
      }
    }
  }

  function renderTimeline() {
    // Static placeholder ("--" + data-i18n="loading_failed") is the
    // rendered final state per m2 decision #2 — backend timeline
    // endpoints are 404 / SSL-timeout (issues #14). No data fetch, no
    // DOM mutation. app.js's applyI18n at boot end translates the
    // placeholder text. Stub kept for symmetry with other render fns;
    // when issue #14 lands, replace with real timeline rendering.
  }

  // Resolve (asset, metric) → backend value. Returns null if backend
  // doesn't carry this metric (the row stays at "--" muted in the HTML).
  function valueOf(asset, metric, data) {
    const { overlayLatest, geoRun, transmission } = data;
    const payload = overlayLatest?.payload_json || {};

    if (asset === "supply") {
      if (metric === "hormuz_status") return geoRun?.hormuz_status;
      // Other supply rows: backend doesn't expose specifics
      // (probed 2026-05-09; only hormuz_status survives).
      return null;
    }
    if (asset === "shipping") {
      // All 4 shipping rows blocked on issue #16
      // (/api/markets/shipping SSL-timeout). Aggregate
      // shipping_insurance_score is on geoRun but doesn't map cleanly
      // to any of the 4 mockup rows; leave all "--".
      return null;
    }
    if (asset === "energy") {
      if (metric === "wti") return payload.wti ?? geoRun?.wti;
      // brent: SKIP overwrite — see CONTRACT in caller (renderIgRows).
      // Other energy rows blocked on issue #15
      // (/api/markets/oil-microstructure 404 confirmed).
      return null;
    }
    if (asset === "macro") {
      if (metric === "vix") return payload.vix ?? transmission?.vix;
      // Other macro rows: backend doesn't aggregate.
      return null;
    }
    return null;
  }

  function renderIgRows(data) {
    for (const row of $$(".ig-row[data-asset][data-metric]")) {
      const asset = row.dataset.asset;
      const metric = row.dataset.metric;

      // CONTRACT (m2 decision honored): commodities Brent row keeps its
      // `data-i18n="ac_unavailable"` / "尚未接入" placeholder + .unavail
      // class. Skip overwrite so future drive-bys don't clobber the step
      // 2A "Brent unavailable" idiom.
      if (asset === "energy" && metric === "brent") continue;

      const v = valueOf(asset, metric, data);
      const vEl = row.querySelector(".v");
      if (!vEl) continue;

      if (v === null || v === undefined || v === "") {
        // Stay at "--". Ensure .muted class so the visual state matches
        // the data-absent intent.
        vEl.classList.add("muted");
        continue;
      }

      // Has value. Numbers → format with 1 decimal; strings → try i18n
      // localisation via t("geo_v_<value>"), fall back to raw.
      let display;
      if (typeof v === "number") {
        display = safeNum(v, 2);
      } else {
        const s = String(v);
        // For enum-like strings (e.g., "stressed", "stable"), localise
        // and tag for app.js's applyI18n on subsequent lang toggles.
        vEl.setAttribute("data-i18n", `geo_v_${s}`);
        display = tr(`geo_v_${s}`, s);
      }
      vEl.textContent = display;
      vEl.classList.remove("muted");
    }
  }

  function renderAction(data) {
    const { overlay } = data;
    const cap = Number(overlay?.score_cap);
    const el = $("action-size-cap");
    if (!el) return;
    if (Number.isFinite(cap)) {
      el.textContent = `size cap = ${Math.round(cap)}%`;
    }
    // else leave "size cap = --" from static HTML
  }

  function renderAudit(data) {
    const { overlay, overlayLatest, geoRun, latestRun } = data;

    // overlay_id: synthesise from run_id + as_of_date when no explicit
    // field exists (probe didn't surface an `overlay_id` key — mockup's
    // "geo_2026-04-28" is illustrative).
    const asOf = latestRun?.as_of_date ?? overlay?.as_of_date ?? geoRun?.as_of_date;
    const runId = latestRun?.run_id ?? overlay?.run_id ?? geoRun?.run_id;
    setText($("audit-overlay-id"),
      asOf ? `geo_${asOf}` : "--");
    setText($("audit-level"), safe(overlay?.overlay_level));
    setText($("audit-conflict-score"),  safeNum(geoRun?.conflict_intensity_score, 1));
    setText($("audit-supply-score"),    safeNum(geoRun?.supply_disruption_score, 1));
    setText($("audit-shipping-score"),  safeNum(geoRun?.shipping_insurance_score, 1));
    setText($("audit-energy-score"),    safeNum(geoRun?.energy_microstructure_score, 1));
    setText($("audit-macro-score"),     safeNum(geoRun?.macro_transmission_score, 1));

    // Data health: brent_price === null is the trigger (per m3 spec —
    // strict equality, not == null and not falsy, because backend uses
    // literal JSON null to signal "not wired" vs undefined for missing
    // field vs 0 for actual zero price).
    const dh = $("audit-data-health");
    if (dh) {
      if (geoRun?.brent_price === null) {
        // Static HTML already has data-i18n="audit_geo_brent_unavail"
        // + style color: var(--bad). Keep as-is — confirms the warn
        // state. No change needed; defensive idempotent reset:
        dh.setAttribute("data-i18n", "audit_geo_brent_unavail");
        dh.style.color = "var(--bad)";
      } else if (geoRun?.brent_price !== undefined) {
        // Brent now wired (issue resolved). Flip to OK label.
        dh.removeAttribute("data-i18n");
        dh.textContent = isZh() ? "数据正常" : "OK";
        dh.style.color = "var(--good)";
      }
      // else: geoRun missing entirely — leave whatever was there.
    }

    setText($("audit-as-of"), safe(asOf));
    setText($("audit-run-id"), safe(runId));
  }

  // ─── i18n + lang toggle ────────────────────────────────────────────
  // Standard sweep: replace [data-i18n] textContent via t().
  function applyI18n() {
    if (typeof t !== "function") return;
    for (const el of $$("[data-i18n]")) {
      const key = el.getAttribute("data-i18n");
      const v = t(key);
      if (v) el.textContent = v;
    }
    const toggle = $("lang-toggle");
    if (toggle) toggle.textContent = isZh() ? "EN" : "中文";
  }

  // Lang toggle: piggyback ONLY. app.js setupLangToggle (line 765-775)
  // is the canonical handler — it does setLang + applyI18n. If we also
  // bind a click that calls setLang here, the two handlers fire in DOM
  // order and cancel each other out (each toggles relative to the
  // other's already-flipped value), so net visible state never changes.
  // This was caught + fixed in step 2C markets-v2.js (commit 1fe374f);
  // documented here so step 2D / 2E implementers don't repeat the bug.
  //
  // We piggyback to flip #lang-toggle's OWN button label since app.js's
  // applyI18n only translates [data-i18n] nodes and the bare button is
  // not. Schedule on next macrotask so getLang() reflects the post-click
  // value (app.js's handler runs first because it bound first).
  function setupLangToggle() {
    const btn = $("lang-toggle");
    if (!btn || btn.dataset.geoV2Bound) return;
    btn.dataset.geoV2Bound = "1";
    btn.addEventListener("click", () => {
      setTimeout(() => { btn.textContent = isZh() ? "EN" : "中文"; }, 0);
    });
  }

  // ─── Boot ──────────────────────────────────────────────────────────
  document.addEventListener("DOMContentLoaded", async () => {
    console.debug(`[geo-v2] boot start | API base = ${API || "<same-origin>"}`);
    applyI18n();
    setupLangToggle();
    try {
      const data = await fetchAll();
      console.debug("[geo-v2] render begin");
      timed("renderTopBar",     () => renderTopBar(data));
      timed("renderHero",       () => renderHero(data));
      timed("renderLevelMeter", () => renderLevelMeter(data));
      timed("renderTimeline",   () => renderTimeline());
      timed("renderIgRows",     () => renderIgRows(data));
      timed("renderAction",     () => renderAction(data));
      timed("renderAudit",      () => renderAudit(data));
      // Re-apply: renderHero / renderIgRows mutate [data-i18n] attrs on
      // chips and ig-row .v elements — sweep again so the new attrs get
      // their first translation pass.
      applyI18n();
      console.debug("[geo-v2] render end");
    } catch (e) {
      console.error("[geo-v2] boot failed", e);
    } finally {
      // Always set ready, even if a render fn threw — verify-geo.py and
      // visual checks need this signal regardless of partial-failure
      // state. Render fns above already guard for ._err per data slice.
      document.body.setAttribute("data-render-state", "ready");
      console.debug("[geo-v2] data-render-state=ready");
    }
  });
})();
