/* ───────────────────────────────────────────────
   markets-v2.js · Markets page renderer (step 2C)
   Renders the v2 sidebar+layout markets view from 6 backend endpoints.
   Reuses i18n (t() / getLang() / setLang()) from app.js.
   The legacy capital-warning.js initRegimeTransmissionPage() is gated
   off by data-page="markets" and additionally backstopped by hidden
   #rt-* shim divs in the HTML — see m2 task notes.
   ─────────────────────────────────────────────── */
(() => {
  if (document.body?.dataset?.page !== "markets") return;

  // ─── Sticky same-origin API base ───────────────────────────────────
  // Mirrors hot-fix #5 / #10 idiom: on production hosts (nexo.hk) plus
  // local dev, fall through to same-origin so the host-only session
  // cookie is sent. Cross-subdomain api.nexo.hk is only used as a
  // defensive fallback for unknown hosts (preview/staging without a
  // <meta name="macro-api-base"> override).
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
  const escapeHtml = (s) => String(s ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");

  // Lightweight timer: wraps a sync fn and logs duration. Errors propagate.
  function timed(name, fn) {
    const t0 = performance.now();
    try { fn(); }
    finally { console.debug(`[markets-v2] ${name} ${Math.round(performance.now() - t0)}ms`); }
  }

  // ─── Network ───────────────────────────────────────────────────────
  async function api(path) {
    const t0 = performance.now();
    // Same-origin idiom: paths starting with /api/ stay relative so the
    // session cookie is carried on prod nexo.hk + local dev-proxy.
    const url = path.startsWith("/api/") ? path : API + path;
    try {
      const r = await fetch(url, { credentials: "include", cache: "no-store" });
      const dt = Math.round(performance.now() - t0);
      if (!r.ok) {
        console.debug(`[markets-v2] fetch ${path} failed ${r.status} in ${dt}ms`);
        return { _err: r.status };
      }
      const json = await r.json();
      console.debug(`[markets-v2] fetch ${path} ok in ${dt}ms`);
      return json;
    } catch (e) {
      const dt = Math.round(performance.now() - t0);
      console.debug(`[markets-v2] fetch ${path} threw in ${dt}ms: ${e?.message || e}`);
      return { _err: String(e?.message || e) };
    }
  }

  // Several endpoints wrap their payload under `.item` (transmission,
  // overlay-decision, regime/latest, action-bias/latest). Unwrap; pass
  // through if absent. Preserves _err sentinel from api().
  function unwrap(payload) {
    if (!payload || typeof payload !== "object") return {};
    if (payload._err !== undefined) return payload;
    if (payload.item && typeof payload.item === "object") return payload.item;
    return payload;
  }

  // ─── Fetch chain ───────────────────────────────────────────────────
  // Promise.allSettled — a single 502/timeout doesn't tank the whole
  // render; each render function checks for ._err on its slice and
  // falls back to the static HTML's mockup-faithful placeholders.
  async function fetchAll() {
    const today = new Date().toISOString().slice(0, 10); // YYYY-MM-DD
    const t0 = performance.now();
    console.debug(`[markets-v2] fetchAll start, runDate=${today}`);

    const results = await Promise.allSettled([
      api("/api/transmission/latest"),
      api("/api/regime/latest"),
      api(`/api/run/${today}/regime-layers`),
      api(`/api/run/${today}/overlay-decision`),
      api("/api/action-bias/latest"),
      api("/api/latest-run"),
    ]);
    const [transmission, regime, layersRaw, overlay, bias, latestRun] = results.map(
      (r) => (r.status === "fulfilled"
        ? unwrap(r.value)
        : { _err: String(r.reason?.message || r.reason) })
    );

    // regime-layers wraps under `.items` (list of 3: shock/tactical/cyclical),
    // not `.item` — handle separately. unwrap() returned the top-level dict.
    const layersList = Array.isArray(layersRaw?.items) ? layersRaw.items : [];

    const errCount = [transmission, regime, layersRaw, overlay, bias, latestRun]
      .filter((x) => x?._err !== undefined).length;
    const dt = Math.round(performance.now() - t0);
    console.debug(`[markets-v2] fetchAll done in ${dt}ms, errors: ${errCount}/6`);

    return { transmission, regime, layers: layersList, overlay, bias, latestRun, runDate: today };
  }

  // ─── Renderers ─────────────────────────────────────────────────────

  function renderTopBar(data) {
    const { latestRun, overlay } = data;
    setText($("status-run-id"), safe(latestRun?.run_id ?? overlay?.run_id));
    // status-latency / status-fresh: backend exposes nothing matching the
    // mockup's "11.4s" / "23/45 indicators". Leave "--" — cosmetic, noted
    // as part of step 2C graceful empties (issue #12).
    setText($("status-latency"), "--");
    setText($("status-fresh"), "--");
    setText($("meta-date"), safe(latestRun?.as_of_date ?? overlay?.as_of_date ?? data.runDate));
    setText($("meta-regime"), safe(latestRun?.final_regime ?? overlay?.regime_override));
  }

  function renderHero(data) {
    const { overlay, bias } = data;
    // Punch text + 3/4 chips are pure i18n (markets_punch_*, chip_*).
    // Only data-driven slot is #chip-size-cap-val.
    //
    // NOTE: action_size_cap (position size) and score_cap (regime score
    // cap) are NOT semantically equivalent — using overlay.score_cap as
    // fallback because we need *some* numeric cap to render. The chip
    // label "仓位上限 / Size cap" maps cleanly to action_size_cap;
    // falling back to score_cap is a degraded UX, not a true synonym.
    const sizeCap = bias?.action_size_cap ?? overlay?.score_cap ?? null;
    const capNum = Number(sizeCap);
    setText($("chip-size-cap-val"),
      Number.isFinite(capNum) ? `${Math.round(capNum)}%` : "--");
  }

  // Parse "D11=50.18" / "D11:50" / "D11 50" → { dim, score }.
  function parseTriggeredSignal(s) {
    const m = String(s ?? "").match(/^(D\d{2})\s*[=:\s]\s*([0-9.+-]+)/);
    if (!m) return null;
    return { dim: m[1].toUpperCase(), score: Number(m[2]) };
  }

  // Heuristic bias → severity classifier.
  // Known limitation: backend currently emits free-form strings; once
  // backend defines a stable enum (issue: TBD), replace with switch.
  // Strings checked are case-insensitive substring matches.
  function classifyBias(bias) {
    const b = (bias || "").toLowerCase();
    // bad: structurally negative tilts
    if (/\b(fragile|tightening|risk[_ ]off|defensive[_ ]short)\b/.test(b)) return "bad";
    // warn: defensive but not panicked
    if (/\b(hedge|selective|caution|stress)\b/.test(b)) return "warn";
    // good: pro-risk / expansion
    if (/\b(supportive|expansion|risk[_ ]on|favorable)\b/.test(b)) return "good";
    return "neutral";
  }

  function renderChain(data) {
    const { overlay } = data;
    // Build D-code → score map from overlay.triggered_signals (probed
    // shape: ["D11=50.18", "D03=67.0", ...]). This is the cleanest signal
    // of per-dimension state without diving into the raw model.
    const dimScores = {};
    const sigs = Array.isArray(overlay?.triggered_signals) ? overlay.triggered_signals : [];
    for (const raw of sigs) {
      const parsed = parseTriggeredSignal(raw);
      if (parsed) dimScores[parsed.dim] = parsed.score;
    }

    // Fill score cells. Effect cells deliberately untouched — they keep
    // their data-i18n fallback per m2 decision 1 (mockup-faithful copy).
    for (const dim of ["D11", "D03", "D09", "D07"]) {
      const row = document.querySelector(`.chain-row[data-row="${dim.toLowerCase()}"]`);
      if (!row) continue;
      const scoreCell = row.querySelector('[data-field="score"]');
      if (!scoreCell) continue;
      const score = dimScores[dim];
      setText(scoreCell, Number.isFinite(score) ? safeNum(score, 1) : "--");
    }
  }

  function renderAssetClasses(data) {
    const { transmission } = data;
    if (!transmission || transmission._err !== undefined) {
      // Leave all default mockup placeholders untouched — bias chips stay
      // at "--" (already in static HTML), metric rows stay at "--".
      return;
    }

    // Shape sanity: warn (don't crash) if asset_classes_json contains an
    // entry that doesn't carry a .bias field. Cheap per-render check —
    // catches backend schema drift the moment it ships.
    ["rates", "equities", "credit", "usd", "commodities", "crypto"].forEach((k) => {
      const v = transmission?.asset_classes_json?.[k];
      if (v && typeof v === "object" && !("bias" in v)) {
        console.warn(`[markets-v2] asset_classes_json.${k} shape mismatch, expected .bias`);
      }
    });

    // Map asset → bias source. Prefer flat "<asset>_bias" (probed:
    // rates_bias / equities_bias / credit_bias / commodities_bias /
    // crypto_bias). usd_bias may not be top-level (probe truncated at
    // index 8); fall through to asset_classes_json.usd.bias.
    const biasSource = {
      rates: transmission.rates_bias,
      equities: transmission.equities_bias,
      credit: transmission.credit_bias,
      commodities: transmission.commodities_bias,
      crypto: transmission.crypto_bias,
      usd: transmission.usd_bias ?? transmission.asset_classes_json?.usd?.bias,
    };

    for (const card of $$(".ac-card")) {
      const asset = card.dataset.asset;
      const chip = card.querySelector("[data-bias-target]");
      const biasVal = biasSource[asset];
      if (chip && biasVal) {
        // Localise via t("bias_<value>") if available; else show raw enum.
        chip.textContent = tr(`bias_${biasVal}`, String(biasVal));
        chip.classList.remove("neutral", "warn", "good", "bad");
        chip.classList.add(classifyBias(biasVal));
      }

      // Per-asset metric rows: backend doesn't expose 10Y-3M / S&P fwd PE
      // / HY OAS / DXY / WTI / BTC etc. as explicit fields. Static HTML
      // leaves them at "--"; we don't overwrite.
      //
      // CONTRACT (m2 decision 2): commodities → Brent row keeps its
      // `data-i18n="ac_unavailable"` placeholder (step 2A "尚未接入"
      // pattern). Skip it explicitly here so future drive-bys that try
      // to populate metric rows don't clobber the contract.
      for (const cell of $$(`[data-metric]`, card)) {
        if (asset === "commodities" && cell.dataset.metric === "brent") continue;
        // Intentional no-op: leave cell at "--" until issue #12 lands a
        // backend metric-values endpoint.
      }
    }
  }

  function renderHeatmap(data) {
    const { transmission } = data;
    const container = $("sector-heatmap-rows");
    if (!container) return;

    // Contract (per m2 HTML comment, restated here for future drive-bys):
    //  - At least sectors_json.favored.length + avoided.length rows.
    //  - If transmission/latest itself failed, render exactly 1
    //    placeholder .heat-row with i18n "loading_failed" / fallback
    //    "数据未就绪" so the section is never visually empty.
    if (!transmission || transmission._err !== undefined) {
      container.innerHTML = "";
      const row = document.createElement("div");
      row.className = "heat-row muted";
      row.innerHTML = `<div class="name"><span data-i18n="loading_failed">${escapeHtml(tr("loading_failed"))}</span></div><div></div>`;
      container.appendChild(row);
      return;
    }

    const sectors = transmission.sectors_json || {};
    const favored = Array.isArray(sectors.favored) ? sectors.favored : [];
    const avoided = Array.isArray(sectors.avoided) ? sectors.avoided : [];
    container.innerHTML = "";

    if (favored.length === 0 && avoided.length === 0) {
      const row = document.createElement("div");
      row.className = "heat-row muted";
      row.innerHTML = `<div class="name"><span data-i18n="loading_failed">${escapeHtml(tr("loading_failed"))}</span></div><div></div>`;
      container.appendChild(row);
      return;
    }

    // Render favored (positive bars, green) then avoided (negative bars,
    // red). z-scores aren't in /api/transmission/latest — show "--" with
    // class muted. When backend issue #12 lands, swap sectors_json for
    // /api/markets/sector-heatmap with real per-ETF z_score values.
    const renderRow = (label, kind /* "pos" | "neg" */) => {
      const row = document.createElement("div");
      row.className = "heat-row";
      const fillStyle = kind === "pos"
        ? "width: 14%;"
        : "right: 50%; width: 14%; left: auto;";
      row.innerHTML =
        `<div class="name"><span>${escapeHtml(label)}</span></div>` +
        `<div class="heat-bar"><div class="heat-fill ${kind}" style="${fillStyle}"></div>` +
        `<span class="val ${kind} muted">--</span></div>`;
      container.appendChild(row);
    };
    favored.forEach((s) => renderRow(s, "pos"));
    avoided.forEach((s) => renderRow(s, "neg"));
  }

  function renderStyles(data) {
    const { bias } = data;
    if (!bias || bias._err !== undefined) return;

    const norm = (s) => String(s ?? "").toLowerCase().replace(/[^a-z0-9_]/g, "");
    const favoredSet = new Set((Array.isArray(bias.favored_styles_json)
      ? bias.favored_styles_json : []).map(norm));
    const avoidedSet = new Set((Array.isArray(bias.avoided_styles_json)
      ? bias.avoided_styles_json : []).map(norm));

    // Mockup data-style keys (quality / low_vol / value / momentum / size /
    // growth) match the strings backend tends to use. Membership test is
    // by substring — accepts "low_vol" matching "low_volatility", etc.
    const memberOf = (set, key) =>
      [...set].some((s) => s.includes(key) || key.includes(s));

    for (const stat of $$(".stat[data-style]")) {
      const style = norm(stat.dataset.style);
      const valEl = stat.querySelector("[data-style-value]");
      if (!valEl) continue;
      let dirKey, color;
      if (memberOf(favoredSet, style)) {
        dirKey = "style_v_favor"; color = "var(--good)";
      } else if (memberOf(avoidedSet, style)) {
        dirKey = "style_v_avoid"; color = "var(--bad)";
      } else {
        dirKey = "style_v_neutral"; color = "";
      }
      // z-scores not exposed; show direction-only with "--" magnitude.
      // Re-applyI18n at boot end re-translates the inner span's data-i18n.
      valEl.classList.toggle("muted", dirKey === "style_v_neutral");
      valEl.style.color = color;
      valEl.innerHTML = `<span data-i18n="${dirKey}">${escapeHtml(tr(dirKey, ""))}</span> --`;
    }
  }

  function renderAudit(data) {
    const { latestRun, overlay, transmission } = data;
    setText($("audit-as-of"), safe(latestRun?.as_of_date ?? overlay?.as_of_date));
    setText($("audit-rates"), safe(transmission?.rates_bias));
    setText($("audit-commodities"), safe(transmission?.commodities_bias));
    setText($("audit-overlay"), safe(overlay?.overlay_level));
    const cap = Number(overlay?.score_cap);
    setText($("audit-score-cap"), Number.isFinite(cap) ? String(Math.round(cap)) : "--");
    setText($("audit-run-id"), safe(latestRun?.run_id ?? overlay?.run_id));
  }

  // ─── i18n + lang toggle ────────────────────────────────────────────
  // Reuses app.js's getLang() / setLang() / t() globals. applyI18n scans
  // every [data-i18n] and rewrites textContent via t(). mockup-v2/markets
  // doesn't use [data-i18n-placeholder] / [data-i18n-attr] on this page,
  // so a textContent-only sweep is sufficient.
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

  function setupLangToggle() {
    // app.js's setupLangToggle (app.js:765-775) is already bound to
    // #lang-toggle and is the canonical handler — it does setLang +
    // applyI18n. If we ALSO bind a click that calls setLang here, the
    // two handlers fire in DOM order and cancel each other out (each
    // toggles relative to the other's already-flipped value), so the
    // net visible state never changes. Mirrors dashboard-v2.js
    // bindLangToggle (line 662-676) which deliberately avoids touching
    // setLang for the same reason.
    //
    // We still piggyback on click to flip the toggle button's OWN
    // label, since app.js's applyI18n only translates [data-i18n]
    // nodes and #lang-toggle is a bare button. Schedule on the next
    // microtask so getLang() reflects the post-click value (app.js's
    // handler runs first because it bound first).
    const btn = $("lang-toggle");
    if (!btn || btn.dataset.marketsV2Bound) return;
    btn.dataset.marketsV2Bound = "1";
    btn.addEventListener("click", () => {
      setTimeout(() => { btn.textContent = isZh() ? "EN" : "中文"; }, 0);
    });
  }

  // ─── Boot ──────────────────────────────────────────────────────────
  document.addEventListener("DOMContentLoaded", async () => {
    console.debug(`[markets-v2] boot start | API base = ${API || "<same-origin>"}`);
    applyI18n();
    setupLangToggle();
    try {
      const data = await fetchAll();
      console.debug("[markets-v2] render begin");
      timed("renderTopBar",       () => renderTopBar(data));
      timed("renderHero",         () => renderHero(data));
      timed("renderChain",        () => renderChain(data));
      timed("renderAssetClasses", () => renderAssetClasses(data));
      timed("renderHeatmap",      () => renderHeatmap(data));
      timed("renderStyles",       () => renderStyles(data));
      timed("renderAudit",        () => renderAudit(data));
      // Re-apply: renderHeatmap inserts new [data-i18n] nodes, renderStyles
      // rewrites innerHTML and re-introduces [data-i18n] spans.
      applyI18n();
      console.debug("[markets-v2] render end");
    } catch (e) {
      // Defensive — any uncaught throw inside render lands here. Ready
      // signal still fires via finally so verify-markets.py doesn't hang.
      console.error("[markets-v2] boot failed", e);
    } finally {
      // Always set ready, even if some render fn threw — verify and visual
      // checks need this signal regardless of partial-failure state.
      document.body.setAttribute("data-render-state", "ready");
      console.debug("[markets-v2] data-render-state=ready");
    }
  });
})();
