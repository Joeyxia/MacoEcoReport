/* ───────────────────────────────────────────────
   portfolio-v2.js · Portfolio risk page renderer (step 2D)
   Renders the v2 sidebar+layout portfolio view from 5 backend endpoints
   in 2 stages (auth.me + watchlists + tickers → positions + dynamic-risk).
   Reuses i18n (t() / getLang() / setLang()) from app.js. Does NOT trust
   AI-generated content or backend strings for innerHTML — every string
   flows through escapeHtml before injection (step 2A Brent fabrication +
   prompt-injection defence).
   Co-exists with capital-warning.js initPortfolioWatchlistPage via 11
   hidden shim elements declared in portfolio-watchlist.html.
   ─────────────────────────────────────────────── */
(() => {
  if (document.body?.dataset?.page !== "portfolio-watchlist") return;

  // ─── Sticky same-origin API base ───────────────────────────────────
  // Same idiom as PR #5 / #10 / dashboard-v2 / markets-v2 / geo-v2 /
  // reports-v2.
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
    finally { console.debug(`[portfolio-v2] ${name} ${Math.round(performance.now() - t0)}ms`); }
  }

  // ─── XSS defence ───────────────────────────────────────────────────
  // Every string that reaches innerHTML MUST flow through escapeHtml first.
  // AI content + backend enums + ticker symbols all treated as untrusted.
  function escapeHtml(str) {
    return String(str ?? "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }

  // ─── Classifiers (decision c / risk thresholds / action mapping) ──
  // Heat: backend macro_exposure_* fields are enum {"low","medium","high"}
  // (probed step 2D round 1). The .heat.g (beneficial) class is NOT
  // implemented yet — backend has no direction field (issue #22). Other
  // values fall through to .l muted with "--" text.
  function classifyHeat(value) {
    const v = String(value || "").toLowerCase();
    if (v === "high")   return { cls: "h", label: "H" };
    if (v === "medium") return { cls: "m", label: "M" };
    if (v === "low")    return { cls: "l", label: "L" };
    // Anchor for future direction-aware values (issue #22): "benefit" / "g"
    // would map to { cls: "g", label: "G" } here.
    return { cls: "l muted", label: "--" };
  }

  // Risk-cell colour. macro_risk_score is 0..100 in probe (63.0 for 0175.HK).
  function classifyRisk(score) {
    const n = Number(score);
    if (!Number.isFinite(n)) return { cls: "muted", text: "--" };
    let cls = "muted";
    if (n >= 70) cls = "red";
    else if (n >= 50) cls = "amber";
    else if (n >= 30) cls = "muted";
    else cls = "green";
    return { cls, text: n.toFixed(0) };
  }

  // Action pill. Backend portfolio_recommendation.recommended_action enum
  // observed values map to mockup's 4 rec classes (.reduce/.trim/.keep/.add)
  // + i18n key (rec_reduce/_trim/_keep/_add already in app.js).
  function classifyAction(value) {
    const v = String(value || "").toLowerCase();
    if (v.includes("sell") || v.includes("reduce")) {
      return { cls: "reduce", i18nKey: "rec_reduce" };
    }
    if (v.includes("trim") || v.includes("lighten")) {
      return { cls: "trim", i18nKey: "rec_trim" };
    }
    if (v.includes("add") || v.includes("buy") || v.includes("accumulate")) {
      return { cls: "add", i18nKey: "rec_add" };
    }
    // Default: hold / keep / unknown
    return { cls: "keep", i18nKey: "rec_keep" };
  }

  // ─── Network ───────────────────────────────────────────────────────
  async function api(path) {
    const t0 = performance.now();
    const url = path.startsWith("/api/") ? path : API + path;
    try {
      const r = await fetch(url, { credentials: "include", cache: "no-store" });
      const dt = Math.round(performance.now() - t0);
      if (!r.ok) {
        console.debug(`[portfolio-v2] fetch ${path} failed ${r.status} in ${dt}ms`);
        return { _err: r.status };
      }
      const json = await r.json();
      console.debug(`[portfolio-v2] fetch ${path} ok in ${dt}ms`);
      return json;
    } catch (e) {
      const dt = Math.round(performance.now() - t0);
      console.debug(`[portfolio-v2] fetch ${path} threw in ${dt}ms: ${e?.message || e}`);
      return { _err: String(e?.message || e) };
    }
  }

  function settled(results) {
    return results.map((r) =>
      r.status === "fulfilled"
        ? r.value
        : { _err: String(r.reason?.message || r.reason) }
    );
  }

  // ─── State ─────────────────────────────────────────────────────────
  const state = {
    userEmail: "",
    activeWid: null,
    data: {
      me: null,
      watchlists: [],
      tickers: [],
      positions: [],
      summary: null,
      dynamicRisk: null,
    },
  };

  // Sequence counter to drop stale tab-switch results when the user
  // rapid-clicks watchlist tabs.
  let refreshSeq = 0;

  // ─── Fetch chain (two-stage) ───────────────────────────────────────
  // Stage 1 (parallel): auth.me + watchlists + tickers — none of these
  //   need user_email or wid in the path.
  // Stage 2 (parallel): positions[<wid>] + dynamic-risk[<email>] +
  //   risk-summary[<wid>] — need stage 1 to resolve identifiers.
  // If stage 1 fails to resolve user_email OR wid, stage 2 is skipped
  // and graceful-empty defaults remain in the static HTML.
  async function fetchAll() {
    const t0 = performance.now();
    console.debug("[portfolio-v2] fetchAll start (stage 1)");

    const [meRes, watchlistsRes, tickersRes] = settled(
      await Promise.allSettled([
        api("/api/auth/me"),
        api("/api/portfolio/watchlists"),
        api("/api/stocks/tickers"),
      ])
    );

    const userEmail = String(meRes?.user?.email || meRes?.email || "").trim();
    const watchlists = Array.isArray(watchlistsRes?.items) ? watchlistsRes.items : [];
    const tickers = Array.isArray(tickersRes?.tickers) ? tickersRes.tickers : [];

    state.userEmail = userEmail;
    state.data.me = meRes;
    state.data.watchlists = watchlists;
    state.data.tickers = tickers;

    if (!userEmail || !watchlists.length) {
      const dt = Math.round(performance.now() - t0);
      console.debug(`[portfolio-v2] fetchAll stage 1 done in ${dt}ms — `
        + `skipping stage 2 (userEmail=${!!userEmail}, watchlists=${watchlists.length})`);
      state.activeWid = null;
      return state.data;
    }

    if (state.activeWid == null) {
      state.activeWid = watchlists[0].id ?? watchlists[0].watchlist_id ?? null;
    }
    if (state.activeWid == null) {
      const dt = Math.round(performance.now() - t0);
      console.debug(`[portfolio-v2] fetchAll: stage 2 skipped — no wid resolvable`);
      return state.data;
    }

    console.debug(`[portfolio-v2] stage 2 (wid=${state.activeWid}, email=${userEmail})`);
    const [positionsRes, dynamicRiskRes, summaryRes] = settled(
      await Promise.allSettled([
        api(`/api/portfolio/watchlists/${state.activeWid}/positions`),
        api(`/api/portfolio/${encodeURIComponent(userEmail)}/dynamic-risk`),
        api(`/api/portfolio/risk-summary?watchlist_id=${state.activeWid}`),
      ])
    );

    state.data.positions = Array.isArray(positionsRes?.items) ? positionsRes.items : [];
    state.data.dynamicRisk = dynamicRiskRes;
    state.data.summary = summaryRes?.summary || null;

    const dt = Math.round(performance.now() - t0);
    const errs = [meRes, watchlistsRes, tickersRes, positionsRes, dynamicRiskRes, summaryRes]
      .filter((x) => x?._err !== undefined).length;
    console.debug(`[portfolio-v2] fetchAll done in ${dt}ms, errors ${errs}/6`);
    return state.data;
  }

  // Re-fetch only stage 2 on watchlist tab click. Drops stale responses
  // when user clicks tabs faster than the network can return.
  async function refreshActiveWatchlist() {
    if (!state.userEmail || state.activeWid == null) return;
    const mySeq = ++refreshSeq;
    const t0 = performance.now();
    console.debug(`[portfolio-v2] refresh wid=${state.activeWid} seq=${mySeq}`);

    const [positionsRes, dynamicRiskRes, summaryRes] = settled(
      await Promise.allSettled([
        api(`/api/portfolio/watchlists/${state.activeWid}/positions`),
        api(`/api/portfolio/${encodeURIComponent(state.userEmail)}/dynamic-risk`),
        api(`/api/portfolio/risk-summary?watchlist_id=${state.activeWid}`),
      ])
    );

    if (mySeq !== refreshSeq) {
      console.debug(`[portfolio-v2] refresh seq ${mySeq} stale (current=${refreshSeq}) — dropping`);
      return;
    }
    state.data.positions = Array.isArray(positionsRes?.items) ? positionsRes.items : [];
    state.data.dynamicRisk = dynamicRiskRes;
    state.data.summary = summaryRes?.summary || null;
    const dt = Math.round(performance.now() - t0);
    console.debug(`[portfolio-v2] refresh done in ${dt}ms`);

    renderAllWatchlistScoped();
  }

  function renderAllWatchlistScoped() {
    timed("renderTopBar",         () => renderTopBar(state.data));
    timed("renderHero",           () => renderHero(state.data));
    timed("renderWatchlistTabs",  () => renderWatchlistTabs(state.data));
    timed("renderStats",          () => renderStats(state.data));
    timed("renderMatrix",         () => renderMatrix(state.data));
    timed("renderConcentration",  () => renderConcentration(state.data));
    timed("renderWhy",            () => renderWhy(state.data));
    timed("renderAudit",          () => renderAudit(state.data));
    applyI18n();
  }

  // ─── Aggregation helpers ───────────────────────────────────────────

  function totalMarketValue(positions) {
    let total = 0;
    for (const p of positions) {
      const mv = Number(p?.market_value || 0);
      if (Number.isFinite(mv) && mv > 0) total += mv;
    }
    return total;
  }

  // HHI: Σ(weight²) where weight = market_value / total. 0 = perfectly
  // diversified, 1 = single position. Returns null if total <= 0 (can't
  // compute weights — graceful empty per decision h). Issue #24 tracks
  // backend exposing this directly.
  function computeHHI(positions) {
    const total = totalMarketValue(positions);
    if (total <= 0) return null;
    let hhi = 0;
    for (const p of positions) {
      const mv = Number(p?.market_value || 0);
      if (!Number.isFinite(mv) || mv <= 0) continue;
      const w = mv / total;
      hhi += w * w;
    }
    return hhi;
  }

  function fmtUSD(value) {
    const n = Number(value);
    if (!Number.isFinite(n) || n <= 0) return "--";
    if (n >= 1000) return `$${(n / 1000).toFixed(1)}k`;
    return `$${n.toFixed(0)}`;
  }

  // ─── Renderers ─────────────────────────────────────────────────────

  function renderTopBar(data) {
    const summary = data.summary;
    setText($("status-run-id"), safe(data.dynamicRisk?.item?.run_id));
    setText($("status-latency"), "--");
    setText($("status-fresh"), "--");
    setText($("meta-positions-count"), String(data.positions.length || "--"));
    const score = summary?.average_macro_risk_score;
    setText($("meta-avg-risk"), Number.isFinite(Number(score)) ? Math.round(Number(score)) : "--");
    const action = summary?.dominant_action_bias;
    setText($("meta-action"), action ? tr(`action_v_${action}`, action) : "--");
  }

  function renderHero(data) {
    const summary = data.summary;
    const dynRisk = data.dynamicRisk?.item;
    const score = Number(summary?.average_macro_risk_score);
    setText($("hero-risk-score"), Number.isFinite(score) ? Math.round(score) : "--");
    const action = summary?.dominant_action_bias;
    setText($("chip-stance"), action ? tr(`action_v_${action}`, action) : "--");
    const sizeCap = Number(dynRisk?.action_size_cap);
    setText($("chip-size-cap"), Number.isFinite(sizeCap) ? `${Math.round(sizeCap * 100)}%` : "--");
    setText($("chip-positions"), String(data.positions.length || "--"));
    setText($("chip-total-value"), fmtUSD(totalMarketValue(data.positions)));
  }

  function renderWatchlistTabs(data) {
    const root = $("wl-tabs");
    const tmpl = $("wl-tab-template");
    if (!root || !tmpl) return;
    root.replaceChildren();
    for (const wl of data.watchlists) {
      const node = tmpl.content.firstElementChild.cloneNode(true);
      const wid = wl.id ?? wl.watchlist_id;
      node.dataset.wid = String(wid);
      node.querySelector(".name").textContent = wl.list_name || "--";
      const isActive = String(wid) === String(state.activeWid);
      if (isActive) {
        node.classList.add("active");
        // Only the active watchlist has fetched positions; other tabs
        // show "--" until clicked (avoids N preflight fetches).
        node.querySelector(".ct").textContent = String(data.positions.length || 0);
      } else {
        node.querySelector(".ct").textContent = "--";
      }
      node.addEventListener("click", async () => {
        if (String(wid) === String(state.activeWid)) return;
        state.activeWid = wid;
        await refreshActiveWatchlist();
      });
      root.appendChild(node);
    }
  }

  function renderStats(data) {
    const summary = data.summary;
    const positions = data.positions;

    // 1. avg risk
    const avgRisk = Number(summary?.average_macro_risk_score);
    const statAvg = $("stat-avg-risk");
    if (statAvg) {
      statAvg.classList.remove("warn", "bad");
      if (Number.isFinite(avgRisk)) {
        if (avgRisk >= 70) statAvg.classList.add("bad");
        else if (avgRisk >= 50) statAvg.classList.add("warn");
      }
      const v = statAvg.querySelector(".v");
      if (v) v.textContent = Number.isFinite(avgRisk) ? Math.round(avgRisk) : "--";
      // .sub: "上周为 --" — last-week comparison not in API → keep static template
    }

    // 2. high-risk count: positions with macro_risk_score >= 70
    const highRisk = positions
      .filter((p) => Number(p?.macro_signal?.macro_risk_score) >= 70)
      .slice(0, 3);
    const statHigh = $("stat-high-risk");
    if (statHigh) {
      statHigh.querySelector(".v").textContent = String(highRisk.length);
      statHigh.querySelector(".sub").textContent =
        highRisk.length ? highRisk.map((p) => p.ticker).join(" · ") : "--";
    }

    // 3. benefit count: positions where recommended_action ~ "add"
    const benefit = positions
      .filter((p) => /add|buy|accumulate/i.test(p?.portfolio_recommendation?.recommended_action || ""))
      .slice(0, 3);
    const statBen = $("stat-benefit");
    if (statBen) {
      statBen.querySelector(".v").textContent = String(benefit.length);
      statBen.querySelector(".sub").textContent =
        benefit.length ? benefit.map((p) => p.ticker).join(" · ") : "--";
    }

    // 4. diversification (HHI)
    const hhi = computeHHI(positions);
    const statDiv = $("stat-diversification");
    if (statDiv) {
      statDiv.querySelector(".v").textContent = hhi != null ? hhi.toFixed(2) : "--";
    }
    setText($("conc-sector-hhi"), hhi != null ? hhi.toFixed(2) : "--");
  }

  // 8 factors aligned with mockup matrix column order + backend
  // macro_exposure field names ("_sensitivity" suffix).
  const MATRIX_FACTORS = [
    "interest_rate", "growth", "inflation", "oil",
    "credit", "usd", "volatility", "geopolitics",
  ];

  function renderMatrix(data) {
    const body = $("matrix-body");
    const tmpl = $("matrix-row-template");
    if (!body || !tmpl) return;
    body.replaceChildren();

    if (!data.positions.length) {
      // Graceful empty
      const tr = document.createElement("tr");
      tr.innerHTML = `<td colspan="11" class="muted" style="text-align:center;padding:18px;">${escapeHtml(isZh() ? "暂无持仓 · 点击 + 添加持仓" : "no positions · click + add position")}</td>`;
      body.appendChild(tr);
      return;
    }

    for (const p of data.positions) {
      const node = tmpl.content.firstElementChild.cloneNode(true);
      const ticker = String(p?.ticker || "--").toUpperCase();
      const qty = Number(p?.quantity || 0);
      const cost = Number(p?.cost_basis || 0);
      const score = Number(p?.macro_signal?.macro_risk_score);

      node.dataset.ticker = ticker;
      const riskCls = classifyRisk(score);
      node.dataset.status = riskCls.cls;

      // Col 1: ticker + qty/cost
      node.querySelector(".tk").textContent = ticker;
      const qtyEl = node.querySelector(".qty");
      qtyEl.textContent = Number.isFinite(qty) && qty
        ? `${qty} sh${Number.isFinite(cost) && cost ? ` · $${cost.toFixed(2)}` : ""}`
        : "--";

      // Cols 2-9: 8 heat cells
      const exposure = p?.macro_exposure || {};
      for (const factor of MATRIX_FACTORS) {
        const td = node.querySelector(`td[data-factor="${factor}"]`);
        if (!td) continue;
        const value = exposure[`${factor}_sensitivity`];
        const heat = classifyHeat(value);
        const span = td.querySelector(".heat");
        span.className = `heat ${heat.cls}`;
        span.textContent = heat.label;
      }

      // Col 10: risk
      const riskTd = node.querySelector(".risk-cell");
      riskTd.className = `risk-cell ${riskCls.cls}`;
      riskTd.textContent = riskCls.text;

      // Col 11: action pill
      const actionCls = classifyAction(p?.portfolio_recommendation?.recommended_action);
      const recSpan = node.querySelector(".rec");
      recSpan.className = `rec ${actionCls.cls}`;
      recSpan.setAttribute("data-i18n", actionCls.i18nKey);
      recSpan.textContent = tr(actionCls.i18nKey, actionCls.cls);

      body.appendChild(node);
    }
  }

  // Common subset of yfinance sectors → mockup conc_sector_* i18n keys.
  // Unmapped sectors render their raw English label (e.g., "Financial
  // Services") with no i18n key.
  const SECTOR_I18N = {
    "Technology": "conc_sector_tech",
    "Consumer Cyclical": "conc_sector_disc",
    "Energy": "conc_sector_energy",
    "Healthcare": "conc_sector_health",
  };

  function renderConcentration(data) {
    renderConcSector(data);
    renderConcFactor(data);
  }

  function renderConcSector(data) {
    const root = $("conc-by-sector");
    const tmpl = $("conc-row-template");
    if (!root || !tmpl) return;
    root.replaceChildren();

    const tickerMap = new Map(
      (data.tickers || []).map((t) => [String(t.ticker || "").toUpperCase(), t])
    );
    const total = totalMarketValue(data.positions);
    if (total <= 0) {
      root.innerHTML = `<div class="conc-row muted" style="padding:14px;">${escapeHtml(isZh() ? "暂无数据" : "no data")}</div>`;
      return;
    }
    const groups = new Map();
    for (const p of data.positions) {
      const mv = Number(p?.market_value || 0);
      if (!Number.isFinite(mv) || mv <= 0) continue;
      const meta = tickerMap.get(String(p?.ticker || "").toUpperCase());
      const sector = (meta?.sector || "").trim() || (isZh() ? "其它" : "Other");
      const cur = groups.get(sector) || { sum: 0, tickers: [] };
      cur.sum += mv;
      cur.tickers.push(p.ticker);
      groups.set(sector, cur);
    }
    const rows = [...groups.entries()]
      .map(([sector, g]) => ({ key: sector, pct: g.sum / total, tickers: g.tickers }))
      .sort((a, b) => b.pct - a.pct)
      .slice(0, 5);
    const maxPct = rows[0]?.pct || 1;

    for (const row of rows) {
      const node = tmpl.content.firstElementChild.cloneNode(true);
      node.dataset.key = row.key;
      const nameEl = node.querySelector(".name");
      const i18nKey = SECTOR_I18N[row.key];
      if (i18nKey) {
        nameEl.innerHTML = `<span data-i18n="${i18nKey}">${escapeHtml(row.key)}</span> <span class="sub">${escapeHtml(row.tickers.slice(0, 3).join(" · "))}</span>`;
      } else {
        nameEl.innerHTML = `${escapeHtml(row.key)} <span class="sub">${escapeHtml(row.tickers.slice(0, 3).join(" · "))}</span>`;
      }
      const fill = node.querySelector(".conc-fill");
      const widthPct = Math.max(8, Math.round((row.pct / maxPct) * 100));
      fill.style.width = `${widthPct}%`;
      fill.className = "conc-fill" + (row.pct >= 0.30 ? " bad" : row.pct >= 0.20 ? " warn" : "");
      node.querySelector(".pct").textContent = `${Math.round(row.pct * 100)}%`;
      root.appendChild(node);
    }
  }

  // 5 factor rows aligned with mockup conc-by-factor section + i18n keys.
  const FACTOR_ROWS = [
    { factor: "growth",        i18n: "conc_factor_growth" },
    { factor: "interest_rate", i18n: "conc_factor_rate" },
    { factor: "volatility",    i18n: "conc_factor_vol" },
    { factor: "usd",           i18n: "conc_factor_usd" },
    { factor: "oil",           i18n: "conc_factor_oil" },
  ];

  function renderConcFactor(data) {
    const root = $("conc-by-factor");
    const tmpl = $("conc-row-template");
    if (!root || !tmpl) return;
    root.replaceChildren();
    const total = data.positions.length;
    if (total === 0) {
      root.innerHTML = `<div class="conc-row muted" style="padding:14px;">${escapeHtml(isZh() ? "暂无数据" : "no data")}</div>`;
      return;
    }
    for (const row of FACTOR_ROWS) {
      const node = tmpl.content.firstElementChild.cloneNode(true);
      node.dataset.key = row.factor;
      const highCount = data.positions.filter(
        (p) => String(p?.macro_exposure?.[`${row.factor}_sensitivity`] || "").toLowerCase() === "high"
      ).length;
      const pct = total > 0 ? highCount / total : 0;
      const nameEl = node.querySelector(".name");
      nameEl.innerHTML = `<span data-i18n="${row.i18n}">${escapeHtml(row.factor)}</span>`;
      const fill = node.querySelector(".conc-fill");
      fill.style.width = `${Math.max(6, Math.round(pct * 100))}%`;
      fill.className = "conc-fill" + (pct >= 0.50 ? " bad" : pct >= 0.30 ? " warn" : "");
      node.querySelector(".pct").textContent = `${highCount}/${total} H`;
      root.appendChild(node);
    }
  }

  function renderWhy(data) {
    const trace = $("why-trace");
    if (!trace) return;
    const top = Array.isArray(data?.dynamicRisk?.item?.dynamic_top_risk_positions)
      ? data.dynamicRisk.item.dynamic_top_risk_positions : [];
    // payload.summary in dynamic-risk turned out to be a dict, not a string
    // (probe shape was deep enough only to see it's a dict). risk-summary
    // .summary.advice is the confirmed-string source — prefer it. Defensive
    // typeof check on the payload.summary fallback prevents "[object Object]"
    // from leaking into innerHTML.
    const aiQuote =
      (typeof data?.summary?.advice === "string" && data.summary.advice) ||
      (typeof data?.dynamicRisk?.item?.payload?.summary === "string"
        && data.dynamicRisk.item.payload.summary) ||
      "";

    if (!top.length && !aiQuote) {
      trace.innerHTML = `<span class="label">--</span><span class="muted">${escapeHtml(isZh() ? "数据未就绪" : "data unavailable")}</span>`;
      return;
    }

    const items = top.slice(0, 3).map((p) => {
      const ticker = escapeHtml(p?.ticker || "--");
      const score = escapeHtml(p?.macro_risk_score ?? "--");
      const action = escapeHtml(p?.recommended_action || "--");
      const reason = escapeHtml(p?.reason || "");
      return `<span class="label">${ticker}</span>` +
        `<span><code>risk ${score}</code> · <code>${action}</code> ${reason}</span>`;
    });

    if (aiQuote) {
      items.push(
        `<span class="label">AI</span>` +
        `<span class="ai-quote">${escapeHtml(aiQuote)}</span>`
      );
    }

    trace.innerHTML = items.join("");
  }

  function renderAudit(data) {
    const positions = data.positions || [];
    const wlItem = (data.watchlists || []).find(
      (w) => String(w.id ?? w.watchlist_id) === String(state.activeWid)
    ) || data.watchlists?.[0] || null;

    setText($("audit-user-email"), state.userEmail || "--");
    setText($("audit-watchlist-id"), wlItem?.id ?? wlItem?.watchlist_id ?? state.activeWid ?? "--");

    const missing = positions.filter((p) => !p?.macro_exposure).map((p) => p.ticker);
    setText($("audit-missing-exposure"), missing.length ? `[${missing.join(", ")}]` : "[]");

    // Stale: macro_signal.created_at older than 7 days. Missing timestamp
    // counts as stale too (defensive).
    const now = Date.now();
    const WEEK = 7 * 24 * 60 * 60 * 1000;
    const stale = positions.filter((p) => {
      const ts = p?.macro_signal?.created_at || p?.macro_signal?.as_of_date;
      if (!ts) return true;
      const d = new Date(ts).getTime();
      return !Number.isFinite(d) || (now - d > WEEK);
    }).map((p) => p.ticker);
    setText($("audit-stale-signals"), stale.length ? `[${stale.join(", ")}]` : "[]");

    const computedAt = data?.dynamicRisk?.item?.updated_at
      || data?.dynamicRisk?.item?.created_at;
    setText($("audit-computed-at"), computedAt || "--");

    const metaEl = $("audit-pf-meta");
    if (metaEl) {
      const total = positions.length;
      const withExp = positions.filter((p) => p?.macro_exposure).length;
      metaEl.removeAttribute("data-i18n");
      metaEl.textContent = isZh()
        ? `${total} 个持仓 · ${withExp} 个有模型暴露数据`
        : `${total} positions · ${withExp} with exposure data`;
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
    const toggle = $("lang-toggle");
    if (toggle) toggle.textContent = isZh() ? "EN" : "中文";
  }

  // Piggyback ONLY — app.js's setupLangToggle is the canonical handler.
  // Step 2C dual-handler bug: if we also bind a click that calls setLang,
  // both handlers fire in DOM order and cancel each other out. Here we
  // only flip the button's own label and re-render strings sourced from
  // *_zh vs *_en (audit meta, graceful-empty muted text).
  function setupLangToggle() {
    const btn = $("lang-toggle");
    if (!btn || btn.dataset.portfolioV2Bound) return;
    btn.dataset.portfolioV2Bound = "1";
    btn.addEventListener("click", () => {
      setTimeout(() => {
        btn.textContent = isZh() ? "EN" : "中文";
        // Re-render strings that branch on language (audit meta, empty
        // placeholders inside innerHTML-injected blocks).
        renderAllWatchlistScoped();
      }, 0);
    });
  }

  // ─── Add-position UX (decision d) ──────────────────────────────────
  // Minimal prompt() flow: ticker + qty → POST. Toast UX is follow-up
  // (issue #20). Same flow capital-warning.js uses internally; here we
  // just hook the visible v2 button instead of the hidden shim.
  function setupAddPosition() {
    const btn = $("wl-add-position-btn");
    if (!btn || btn.dataset.portfolioV2Bound) return;
    btn.dataset.portfolioV2Bound = "1";
    btn.addEventListener("click", async () => {
      if (state.activeWid == null) {
        console.debug("[portfolio-v2] add-position: no active watchlist");
        return;
      }
      const tickerRaw = window.prompt(isZh() ? "ticker（如 AAPL）" : "Ticker (e.g. AAPL)");
      if (!tickerRaw) return;
      const ticker = tickerRaw.trim().toUpperCase();
      const qtyRaw = window.prompt(isZh() ? "数量" : "Quantity");
      if (qtyRaw === null) return;
      const qty = Number(String(qtyRaw).trim());
      if (!Number.isFinite(qty) || qty <= 0) {
        console.warn(`[portfolio-v2] add-position: invalid qty=${qtyRaw}`);
        return;
      }
      try {
        const r = await fetch(
          `${API}/api/portfolio/watchlists/${state.activeWid}/positions`,
          {
            method: "POST",
            credentials: "include",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ ticker, quantity: qty }),
          }
        );
        if (r.ok) {
          console.debug(`[portfolio-v2] add-position ${ticker} qty=${qty} ok`);
          await refreshActiveWatchlist();
        } else {
          console.warn(`[portfolio-v2] add-position failed ${r.status}`);
        }
      } catch (e) {
        console.warn(`[portfolio-v2] add-position threw: ${e?.message || e}`);
      }
    });
  }

  // ─── Boot ──────────────────────────────────────────────────────────
  document.addEventListener("DOMContentLoaded", async () => {
    console.debug(`[portfolio-v2] boot start | API base = ${API || "<same-origin>"}`);
    applyI18n();
    setupLangToggle();
    setupAddPosition();
    try {
      await fetchAll();
      console.debug("[portfolio-v2] render begin");
      renderAllWatchlistScoped();
      console.debug("[portfolio-v2] render end");
    } catch (e) {
      console.error("[portfolio-v2] boot failed", e);
    } finally {
      document.body.setAttribute("data-render-state", "ready");
      console.debug("[portfolio-v2] data-render-state=ready");
    }
  });
})();
