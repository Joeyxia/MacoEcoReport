/* ───────────────────────────────────────────────
   dashboard-v2.js · Today page renderer
   Renders the v2 Today layout from 7 backend endpoints.
   Reuses i18n (t()) from app.js and helpers from
   capital-warning.js (cwFmtNum / cwFmtTop) when present.
   Old IDs in #legacy-shim are populated by the existing
   renderDashboard() / capital-warning.js paths and ignored.
   ─────────────────────────────────────────────── */
(() => {
  if (document.body?.dataset?.page !== "dashboard") return;

  const API = (document.querySelector('meta[name="macro-api-base"]')?.content || "").trim() || "https://api.nexo.hk";

  // ─── Helpers ─────────────────────────────────────────────
  const $ = (id) => document.getElementById(id);
  const isZh = () => (typeof getLang === "function" ? getLang() : "zh") === "zh";
  const tr = (key, fallback = "") => {
    if (typeof t === "function") {
      const v = t(key);
      if (v) return v;
    }
    return fallback;
  };
  const fmtNum = (v, digits = 2) => {
    if (typeof window.cwFmtNum === "function") return window.cwFmtNum(v, digits);
    if (v === null || v === undefined || v === "") return "—";
    const n = Number(v);
    return Number.isFinite(n) ? n.toFixed(digits) : "—";
  };
  const safe = (v) => (v === null || v === undefined || v === "" ? "—" : String(v));
  const setText = (el, text) => { if (el) el.textContent = text; };
  const setHTML = (el, html) => { if (el) el.innerHTML = html; };
  const escapeHtml = (s) =>
    String(s ?? "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");

  async function api(path) {
    const t0 = performance.now();
    // Keep /api/* on same origin so the session cookie is always sent.
    // Crossing to api.nexo.hk subdomain fails because Flask's
    // SESSION_COOKIE_DOMAIN is unset (host-only cookie). Mirrors the
    // cwApi (capital-warning.js:302) and apiFetch (app.js:742) idiom
    // introduced in commit eaaea53.
    const url = path.startsWith("/api/") ? path : API + path;
    try {
      const r = await fetch(url, { credentials: "include", cache: "no-store" });
      const dt = Math.round(performance.now() - t0);
      if (!r.ok) {
        console.debug(`[dashboard-v2] fetch ${path} failed ${r.status} in ${dt}ms`);
        return { _err: r.status };
      }
      const json = await r.json();
      console.debug(`[dashboard-v2] fetch ${path} ok in ${dt}ms`);
      return json;
    } catch (e) {
      const dt = Math.round(performance.now() - t0);
      console.debug(`[dashboard-v2] fetch ${path} threw in ${dt}ms: ${e?.message || e}`);
      return { _err: String(e?.message || e) };
    }
  }

  function parseJson(maybe) {
    if (!maybe) return null;
    if (typeof maybe === "object") return maybe;
    try { return JSON.parse(maybe); } catch { return null; }
  }

  // Several endpoints wrap their actual payload under `.item` (transmission,
  // action-bias, geopolitical-overlay). Unwrap if present, otherwise pass
  // through. Robust to either-or shape.
  function unwrap(payload) {
    if (payload && typeof payload === "object" && payload.item && typeof payload.item === "object") {
      return payload.item;
    }
    return payload || {};
  }

  // Extract trailing digit from overlay-level strings like "level_2", "S2",
  // "L2", "2". Returns null if no digit found.
  function parseLevel(raw) {
    if (raw == null) return null;
    const m = String(raw).match(/(\d+)/);
    return m ? Number(m[1]) : null;
  }

  // keyIndicatorsSnapshot in /api/model/current is an ARRAY of
  // {label, title, value, source} — not the dict-by-label some callers
  // assumed. This converts the array to a label→value map for O(1) lookup.
  function indexSnapshotByLabel(snap) {
    if (!Array.isArray(snap)) return snap || {};
    const out = {};
    for (const row of snap) {
      const k = row && row.label;
      if (k != null) out[k] = row.value;
    }
    return out;
  }

  // ─── Section renderers ───────────────────────────────────
  function renderTopBar(latestRun, latestAnalysis) {
    const asOf = latestRun?.as_of_date || latestAnalysis?.as_of_date || latestAnalysis?.report_date;
    setText($("tb-as-of"), asOf ? `${asOf} ${isZh() ? "" : ""}`.trim() : "—");
    const score = latestRun?.total_score ?? latestAnalysis?.score_background;
    setText($("tb-score"), score != null ? fmtNum(score, 2) : "—");
    const delta = latestRun?.score_delta ?? latestAnalysis?.score_delta;
    const deltaEl = $("tb-delta");
    if (deltaEl) {
      if (delta == null || !Number.isFinite(Number(delta))) {
        deltaEl.textContent = "";
        deltaEl.className = "";
      } else {
        const n = Number(delta);
        deltaEl.textContent = `${n >= 0 ? "+" : ""}${n.toFixed(2)}`;
        deltaEl.className = n > 0 ? "delta-up" : n < 0 ? "delta-down" : "";
      }
    }
  }

  function renderSidebarStatus(latestRun, model) {
    setText($("sb-run-id"), safe(latestRun?.run_id ?? latestRun?.id ?? "—"));
    const generatedAt = latestRun?.generated_at || latestRun?.created_at || model?.generated_at;
    if (generatedAt) {
      const ms = Date.now() - Date.parse(generatedAt);
      const sec = Math.max(0, Math.round(ms / 1000));
      const txt = sec < 90 ? `${sec}s` : sec < 3600 ? `${Math.round(sec / 60)}m` : `${Math.round(sec / 3600)}h`;
      setText($("sb-latency"), txt);
    } else {
      setText($("sb-latency"), "—");
    }
    const fresh = model?.indicators_fresh ?? model?.refreshed_count;
    const total = model?.indicators_total ?? model?.tracked_count;
    if (fresh != null && total != null) setText($("sb-fresh"), `${fresh}/${total}`);
    else setText($("sb-fresh"), "—");

    const alert = (latestRun?.final_alert_level || "").toLowerCase();
    const pillToday = $("sb-pill-today");
    if (pillToday) {
      if (alert && alert !== "green" && alert !== "none") {
        pillToday.hidden = false;
        pillToday.textContent = isZh() ? alertZhLabel(alert) : alert;
      } else {
        pillToday.hidden = true;
      }
    }
  }

  function alertZhLabel(level) {
    return ({ orange: "橙色", red: "红色", yellow: "黄色", green: "绿色" })[level] || level;
  }

  function renderHero(latestAnalysis, latestRun) {
    const card = $("hero-card");
    const punchEl = $("hero-punch");
    const chipsEl = $("hero-chips");
    if (!card || !punchEl || !chipsEl) return;

    // Hero accent class derived from alert level
    const alert = (latestRun?.final_alert_level || latestAnalysis?.alert_level || "").toLowerCase();
    card.classList.remove("danger", "calm");
    if (alert === "red") card.classList.add("danger");
    else if (alert === "green" || !alert) card.classList.add("calm");

    // Punch line: prefer headline, fall back to executive_summary first line.
    // The API returns both `_zh` and `_en` variants; pick the one matching
    // current UI language so EN toggle actually translates the punch line.
    const langSfx = isZh() ? "zh" : "en";
    const headline = latestAnalysis?.[`headline_${langSfx}`]
      || latestAnalysis?.headline
      || latestRun?.topline_message
      || "";
    const exec = latestAnalysis?.[`executive_summary_${langSfx}`]
      || latestAnalysis?.executive_summary
      || "";
    const summary = latestAnalysis?.summary || "";
    let punch = headline || exec.split(/[。.\n]/)[0] || summary.split(/[。.\n]/)[0];
    punch = String(punch || "").trim();
    if (!punch) punch = isZh() ? "暂无 headline · 等待今日 run" : "Headline pending · awaiting today's run";
    punchEl.textContent = punch;

    // Chips
    const chips = [];
    const regime = latestRun?.final_regime;
    if (regime) chips.push(chip("warn", "regime", regime, true));
    if (alert) chips.push(chip(alertChipClass(alert), "alert", alert));
    const decision = latestRun?.decision_priority || latestAnalysis?.decision_priority;
    if (decision) chips.push(chip("purple", tr("chip_decision", "决策"), decision));
    const conf = latestRun?.signal_confidence_score ?? latestAnalysis?.signal_confidence_score;
    if (conf != null) chips.push(chip("muted", tr("chip_conf", "置信度"), `${Math.round(Number(conf) * (Number(conf) <= 1 ? 100 : 1))}%`));
    if (latestRun?.score_cap_applied != null) chips.push(chip("muted", tr("chip_cap", "封顶"), fmtNum(latestRun.score_cap_applied, 0)));
    chipsEl.innerHTML = chips.join("");
  }

  function alertChipClass(alert) {
    return alert === "red" ? "bad" : alert === "orange" || alert === "yellow" ? "warn" : "good";
  }

  function chip(cls, k, v, withDot = false) {
    const dot = withDot ? `<span class="dot" style="background: currentColor"></span>` : "";
    return `<span class="chip ${escapeHtml(cls)}">${dot}<span class="k">${escapeHtml(k)}</span> ${escapeHtml(v)}</span>`;
  }

  function renderWhyDrawer(latestAnalysis, model, overlayDecision) {
    const trace = $("why-trace");
    const meta = $("why-meta");
    if (!trace) return;
    const rows = [];
    // /api/model/current.keyIndicatorsSnapshot is an array of {label, value}
    // rows; convert to a label→value dict before lookup.
    const snap = indexSnapshotByLabel(model?.keyIndicatorsSnapshot || model?.key_indicators_snapshot);
    const inputs = [];
    const wti = snap.WTI ?? snap.wti ?? snap.wti_price;
    const vix = snap.VIX ?? snap.vix;
    const hy = snap.HY_OAS ?? snap.hy_oas;
    if (wti != null) inputs.push(`WTI <code>${fmtNum(wti, 2)}</code>`);
    if (vix != null) inputs.push(`VIX <code>${fmtNum(vix, 2)}</code>`);
    if (hy != null) inputs.push(`HY OAS <code>${fmtNum(hy, 0)}</code>`);
    // Dimension headlines
    const dims = Array.isArray(model?.dimensions) ? model.dimensions : [];
    const byId = Object.fromEntries(dims.map((d) => [String(d.id || d.code || "").toUpperCase(), d.score ?? d.value]));
    ["D11", "D03", "D09"].forEach((id) => {
      if (byId[id] != null) inputs.push(`${id} <code>${fmtNum(byId[id], 0)}</code>`);
    });
    if (inputs.length) {
      rows.push(`<span class="label">${escapeHtml(tr("why_inputs", "输入"))}</span><span>${inputs.join(" · ")}</span>`);
    }

    // Rules from overlayDecision.triggered_signals
    const triggered = overlayDecision?.triggered_signals || overlayDecision?.rules || [];
    triggered.slice(0, 3).forEach((sig, i) => {
      const cond = sig?.condition || sig?.rule || sig?.expr || "";
      const text = sig?.text || sig?.description || sig?.label || "";
      rows.push(
        `<span class="label">${escapeHtml(tr(`why_rule_${i + 1}`, `规则 ${i + 1}`))}</span>` +
        `<span>${cond ? `<code>${escapeHtml(cond)}</code> ` : ""}${escapeHtml(text)}</span>`,
      );
    });

    // AI quote: first paragraph of executive_summary, lang-aware.
    const langSfx = isZh() ? "zh" : "en";
    const exec = String(
      latestAnalysis?.[`executive_summary_${langSfx}`]
        || latestAnalysis?.executive_summary
        || "",
    ).trim();
    if (exec) {
      const first = exec.split(/\n\n+/)[0].slice(0, 280);
      rows.push(`<span class="label">${escapeHtml(tr("why_ai", "AI"))}</span><span class="ai-quote">${escapeHtml(first)}</span>`);
    }

    trace.innerHTML = rows.join("") || `<span class="label">—</span><span class="muted">${escapeHtml(isZh() ? "暂无推理痕迹" : "No trace yet")}</span>`;

    if (meta) {
      const ruleCount = triggered.length;
      const inputCount = inputs.length;
      const aiModel = latestAnalysis?.ai_model || latestAnalysis?.model || "—";
      meta.textContent = isZh()
        ? `命中 ${ruleCount} 条规则 · ${inputCount} 个数据源 · ${aiModel}`
        : `${ruleCount} rule${ruleCount === 1 ? "" : "s"} · ${inputCount} input${inputCount === 1 ? "" : "s"} · ${aiModel}`;
    }
  }

  function dimColor(score, isD11) {
    const s = Number(score);
    if (!Number.isFinite(s)) return "amber";
    if (isD11 && s >= 80) return "red";
    if (s >= 70) return "green";
    if (s >= 50) return "amber";
    return "red";
  }

  function renderDimGrid(model) {
    const grid = $("dim-grid");
    const note = $("dim-note");
    if (!grid) return;
    const dims = Array.isArray(model?.dimensions) ? model.dimensions : [];
    if (!dims.length) {
      grid.innerHTML = `<span class="muted">${escapeHtml(isZh() ? "暂无维度数据" : "No dimension data")}</span>`;
      if (note) note.textContent = "";
      return;
    }
    // Sort by id D01..D14
    const sorted = [...dims].sort((a, b) => String(a.id || a.code || "").localeCompare(String(b.id || b.code || "")));
    grid.innerHTML = sorted
      .map((d) => {
        const id = String(d.id || d.code || "").toUpperCase();
        const score = Number(d.score ?? d.value ?? 0);
        const isD11 = id === "D11";
        const cls = dimColor(score, isD11);
        const delta = d.delta ?? d.score_delta;
        const deltaTxt = delta == null || !Number.isFinite(Number(delta))
          ? "— —"
          : Number(delta) > 0
            ? `▲ ${Math.abs(Number(delta)).toFixed(1)}`
            : Number(delta) < 0
              ? `▼ ${Math.abs(Number(delta)).toFixed(1)}`
              : "— 0.0";
        const deltaCls = delta == null ? "" : Number(delta) > 0 ? "up" : Number(delta) < 0 ? "down" : "";
        const heightPct = Math.max(8, Math.min(100, score));
        const scoreCls = cls === "red" ? "alert" : "";
        return (
          `<div class="dim" title="${escapeHtml(id)} · ${escapeHtml(String(score))}">` +
          `<div class="dim-code">${escapeHtml(id)}</div>` +
          `<div class="dim-bar-wrap"><div class="dim-bar ${cls}" style="height:${heightPct}%"></div></div>` +
          `<div class="dim-score ${scoreCls}">${escapeHtml(String(Math.round(score)))}</div>` +
          `<div class="dim-delta ${deltaCls}">${escapeHtml(deltaTxt)}</div>` +
          `</div>`
        );
      })
      .join("");

    if (note) {
      const top = sorted
        .filter((d) => Number(d.score) >= 80 || String(d.id).toUpperCase() === "D11")
        .slice(0, 2);
      if (top.length) {
        const parts = top.map((d) => {
          const id = String(d.id || d.code || "").toUpperCase();
          const score = Math.round(Number(d.score || 0));
          const color = dimColor(score, id === "D11") === "red" ? "var(--bad)" : "var(--warn)";
          return `<strong style="color:${color}">${escapeHtml(id)}</strong> @ ${score}`;
        });
        note.innerHTML = (isZh() ? "突出维度：" : "Outliers: ") + parts.join(" · ") + (isZh() ? "。点击维度可下钻到指标级。" : ". Click any dim to drill into indicator level.");
      } else {
        note.textContent = isZh() ? "全部维度处于 70 分以上，今日无异常。" : "All dimensions ≥ 70. No outliers today.";
      }
    }
  }

  function renderTransmission(trans) {
    const grid = $("tx-grid");
    const noteText = $("tx-note-text");
    if (!grid) return;
    if (!trans || trans._err) {
      grid.innerHTML = `<span class="muted">${escapeHtml(isZh() ? "传导数据暂不可用" : "Transmission data unavailable")}</span>`;
      if (noteText) noteText.textContent = "";
      return;
    }
    const t = unwrap(trans);
    const tiles = [
      ["rates", t.rates_bias, "tx_rates"],
      ["equities", t.equities_bias, "tx_equities"],
      ["credit", t.credit_bias, "tx_credit"],
      ["usd", t.usd_bias, "tx_usd"],
      ["commodities", t.commodities_bias, "tx_commodities"],
      ["crypto", t.crypto_bias, "tx_crypto"],
    ];
    grid.innerHTML = tiles
      .map(([key, val, i18nKey]) => {
        const v = String(val || "").trim();
        const lower = v.toLowerCase();
        const isAlert = /(inflation_hedge|hedge|defensive|fragile|stress)/.test(lower);
        const cls = isAlert ? "alert" : "";
        return (
          `<div class="tx-tile ${cls}" data-key="${escapeHtml(key)}">` +
          `<div class="k">${escapeHtml(tr(i18nKey, key))}</div>` +
          `<div class="v">${escapeHtml(v || "—")}</div>` +
          `<div class="arrow">→</div>` +
          `</div>`
        );
      })
      .join("");

    if (noteText) {
      const sectors = parseJson(t.sectors_json) || t.sectors_json || t.sectors || {};
      const fav = sectors.favored || sectors.favor || [];
      const avoid = sectors.avoided || sectors.avoid || [];
      const parts = [];
      if (fav.length) parts.push(`${tr("tx_note_favored", "偏好：")}<span style="color:var(--good)">${escapeHtml(fav.slice(0, 3).join(" · "))}</span>`);
      if (avoid.length) parts.push(`${tr("tx_note_avoided", "回避：")}<span style="color:var(--bad)">${escapeHtml(avoid.slice(0, 3).join(" · "))}</span>`);
      noteText.innerHTML = parts.join(" ") + (parts.length ? " " : "");
    }
  }

  function classifyRisk(score) {
    const s = Number(score);
    if (!Number.isFinite(s)) return "muted";
    if (s >= 70) return "bad";
    if (s >= 55) return "warn";
    if (s <= 40) return "good";
    return "muted";
  }

  function recLabel(rec) {
    const r = String(rec || "").toLowerCase();
    return tr(`rec_${r}`, rec || "");
  }

  function posRows(items, kind) {
    const arr = Array.isArray(items) ? items : [];
    if (!arr.length) {
      const empty = isZh() ? (kind === "risk" ? "暂无高风险持仓" : "暂无受益持仓") : (kind === "risk" ? "No high-risk positions" : "No benefit positions");
      return `<div class="pos-empty">${escapeHtml(empty)}</div>`;
    }
    return arr
      .slice(0, 4)
      .map((p) => {
        const ticker = p.ticker || p.symbol || "—";
        const score = p.macro_risk_score ?? p.composite_risk_score ?? p.risk_score ?? p.score;
        const cls = kind === "benefit" ? "good" : classifyRisk(score);
        const rec = p.recommended_action || p.action_bias || p.recommendation || p.action ||
          (kind === "benefit" ? "favorable" : "trim");
        const scoreTxt = score != null ? `risk ${Math.round(Number(score))}` : "";
        return (
          `<div class="pos-row ${cls}" title="${escapeHtml(ticker)}">` +
          `<span class="ticker">${escapeHtml(ticker)}</span>` +
          `<span class="meta">${escapeHtml(scoreTxt)}${scoreTxt ? " · " : ""}<span>${escapeHtml(recLabel(rec))}</span></span>` +
          `</div>`
        );
      })
      .join("");
  }

  function renderPortfolio(portfolio) {
    const riskEl = $("pos-risk");
    const benefitEl = $("pos-benefit");
    const noteText = $("pos-note-text");
    // /api/portfolio/risk-summary returns {ok, positions, summary, watchlists}.
    // The lists we care about (top_risk / top_benefit) live under .summary.
    const summary = portfolio?.summary || portfolio || {};
    const topRisk = summary.top_risk_positions || portfolio?.top_risk_positions;
    const topBenefit = summary.top_benefit_positions || portfolio?.top_benefit_positions;
    const positionsArr = portfolio?.positions || [];
    const isAuthOk = portfolio && !portfolio._err && (topRisk || topBenefit || positionsArr.length);
    if (!isAuthOk) {
      const msg = isZh() ? "未登录或暂无 watchlist · " : "Not signed in or no watchlist · ";
      if (riskEl) riskEl.innerHTML = `<div class="pos-empty">${escapeHtml(msg)}</div>`;
      if (benefitEl) benefitEl.innerHTML = `<div class="pos-empty">${escapeHtml(isZh() ? "请到持仓风险页登录后查看" : "Sign in on portfolio page to view")}</div>`;
      if (noteText) noteText.textContent = "";
      return;
    }
    if (riskEl) riskEl.innerHTML = posRows(topRisk, "risk");
    if (benefitEl) benefitEl.innerHTML = posRows(topBenefit, "benefit");
    if (noteText) {
      const avg = summary.average_macro_risk_score ?? summary.average_risk_score
        ?? portfolio?.avg_risk_score ?? portfolio?.average_risk;
      if (avg != null) {
        const html = `${escapeHtml(tr("pos_note_avg", "平均宏观风险分"))} <span class="key mono">${escapeHtml(String(Math.round(Number(avg))))}</span>。 `;
        noteText.innerHTML = html;
      } else {
        noteText.textContent = "";
      }
    }
  }

  function renderActionPlan(actionBias, latestAnalysis) {
    const stats = $("action-stats");
    const text = $("action-text");
    if (!stats) return;
    if (!actionBias || actionBias._err) {
      stats.innerHTML = `<div class="muted">${escapeHtml(isZh() ? "动作建议数据暂不可用" : "Action bias unavailable")}</div>`;
      if (text) text.textContent = "";
      return;
    }
    // /api/action-bias/latest is wrapped in { ok, item }. size_cap and
    // hedge_preference live on /api/latest-analysis (not on action-bias).
    const a = unwrap(actionBias);
    const la = latestAnalysis || {};
    const laItem = unwrap(la);

    const dir = a.overall_bias || a.direction || "—";
    const sizeCap = la.action_size_cap ?? laItem.action_size_cap
      ?? a.size_cap ?? a.position_size_cap;
    const hedge = laItem.hedge_preference || la.recommended_stance
      || a.hedge_preference || "—";
    const sizeTxt = sizeCap == null ? "—" : `${Math.round(Number(sizeCap) * (Number(sizeCap) <= 1 ? 100 : 1))}%`;
    stats.innerHTML =
      `<div class="stat"><div class="k">${escapeHtml(tr("action_direction", "方向"))}</div><div class="v">${escapeHtml(dir)}</div></div>` +
      `<div class="stat"><div class="k">${escapeHtml(tr("action_size_cap_k", "仓位上限"))}</div><div class="v">${escapeHtml(sizeTxt)}</div></div>` +
      `<div class="stat"><div class="k">${escapeHtml(tr("action_hedge", "对冲偏好"))}</div><div class="v">${escapeHtml(hedge)}</div></div>`;
    if (text) {
      const favored = (a.favored_styles_json || a.favored_styles || []).slice(0, 3);
      const avoided = (a.avoided_styles_json || a.avoided_styles || []).slice(0, 3);
      const note = a.summary || a.action_note || a.note;
      const lines = [];
      if (avoided.length) lines.push(`${isZh() ? "减仓" : "Reduce"}：<span style="color:var(--text)">${escapeHtml(avoided.join(" · "))}</span>。`);
      if (favored.length) lines.push(`${isZh() ? "偏好" : "Favor"}：<span style="color:var(--text)">${escapeHtml(favored.join(" · "))}</span>。`);
      if (note) lines.push(escapeHtml(note));
      text.innerHTML = lines.join(" ") || (isZh() ? "无补充动作描述。" : "No supplemental action note.");
    }
  }

  function geoLevelLabel(rawLevel) {
    const n = parseLevel(rawLevel);
    if (n != null) {
      const zh = ["零级", "一级", "二级", "三级"][n] || `${n} 级`;
      return isZh() ? zh : `level ${n}`;
    }
    return safe(rawLevel);
  }

  function geoBannerClass(rawLevel) {
    const n = parseLevel(rawLevel);
    if (n == null) return "calm";
    if (n >= 3) return "bad";
    if (n >= 1) return "";
    return "calm";
  }

  function geoSubLevelText(v) {
    const s = String(v || "").toLowerCase();
    const zh = { low: "低", medium: "中等", high: "高", critical: "极高", none: "无" };
    return isZh() ? zh[s] || (v || "—") : v || "—";
  }

  function renderGeoBanner(geo, latestRun) {
    const banner = $("geo-banner");
    if (!banner) return;
    if (!geo || geo._err) {
      setText($("geo-lvl"), "—");
      setText($("geo-title"), tr("geo_banner_title", "—"));
      setText($("geo-conflict"), "—");
      setText($("geo-supply"), "—");
      setText($("geo-shipping"), "—");
      setText($("geo-oil"), "—");
      banner.classList.remove("bad", "calm");
      banner.classList.add("calm");
      return;
    }
    // /api/geopolitical-overlay/latest is wrapped in { ok, item }. The
    // canonical numeric overlay level lives on /api/latest-run.overlay_level
    // ("level_2"); the geo endpoint exposes sub-levels (conflict, supply,
    // shipping) plus the oil_shock_scenario string ("S2").
    const g = unwrap(geo);
    const rawLevel = latestRun?.overlay_level ?? g.overlay_level
      ?? g.oil_shock_scenario ?? g.conflict_level;
    const cls = geoBannerClass(rawLevel);
    banner.classList.remove("bad", "calm");
    if (cls) banner.classList.add(cls);
    setText($("geo-lvl"), geoLevelLabel(rawLevel));
    setText($("geo-title"), g.scenario_label || g.summary || g.oil_shock_scenario
      || tr("geo_banner_title", "局部供给冲击 + 油价压力"));
    setText($("geo-conflict"), geoSubLevelText(g.conflict_level));
    setText($("geo-supply"), geoSubLevelText(g.supply_disruption_level));
    setText($("geo-shipping"), geoSubLevelText(g.shipping_risk_level));
    const payload = parseJson(g.payload_json) || g.payload || {};
    const wti = payload.wti ?? payload.WTI ?? g.wti;
    const brent = payload.brent ?? payload.Brent ?? g.brent;
    const wtiTxt = wti != null ? fmtNum(wti, 2) : "—";
    const brentTxt = brent != null ? fmtNum(brent, 2) : "—";
    setText($("geo-oil"), `${brentTxt} · ${wtiTxt}`);
  }

  function renderAudit(latestRun, latestAnalysis, geo) {
    const body = $("audit-body");
    const meta = $("audit-meta");
    if (!body) return;
    const rows = [
      [tr("audit_run_id", "run_id"), latestRun?.run_id ?? latestRun?.id],
      [tr("audit_as_of", "as_of_date"), latestRun?.as_of_date ?? latestAnalysis?.report_date],
      [tr("audit_score_bg", "背景分"), latestRun?.total_score != null ? fmtNum(latestRun.total_score, 2) : null],
      [tr("audit_overlay_level", "叠加层等级"),
       latestRun?.overlay_level ?? unwrap(geo)?.oil_shock_scenario ?? null],
      [tr("audit_override", "已接管"), String(!!latestRun?.overlay_override_applied)],
      [tr("audit_score_cap", "分数封顶"), latestRun?.score_cap_applied != null ? fmtNum(latestRun.score_cap_applied, 1) : null],
      [tr("audit_final_regime", "最终 regime"), latestRun?.final_regime],
      [tr("audit_alert_label", "警报"), latestRun?.final_alert_level],
      [tr("audit_ai_status", "AI 状态"), latestAnalysis?.ai_status || latestAnalysis?.status],
      [tr("audit_ai_model", "AI 模型"), latestAnalysis?.ai_model || latestAnalysis?.model],
      [tr("audit_prompt_version", "Prompt 版本"), latestAnalysis?.prompt_version],
    ];
    body.innerHTML = rows
      .filter(([, v]) => v != null && v !== "")
      .map(([k, v]) => `<span class="prompt">▸</span> <span class="key">${escapeHtml(k)}</span>: <span class="val">${escapeHtml(String(v))}</span>`)
      .join("<br>");
    if (meta) {
      const fresh = latestRun?.indicators_fresh;
      const failed = latestRun?.indicators_failed;
      const elapsed = latestRun?.elapsed_seconds;
      const parts = [];
      if (fresh != null) parts.push(`${fresh} ${isZh() ? "项指标" : "indicators"}`);
      if (failed != null) parts.push(`${failed} ${isZh() ? "项失败" : "failed"}`);
      if (elapsed != null) parts.push(`${fmtNum(elapsed, 1)} ${isZh() ? "秒" : "s"}`);
      meta.textContent = parts.join(" · ");
    }
  }

  async function renderAuth() {
    const slot = $("sb-auth-line");
    if (!slot) return;
    const me = await api("/api/auth/me");
    if (!me || me._err || !me.authenticated) {
      slot.hidden = true;
      return slot;
    }
    const email = String(me.user?.email || "").trim();
    if (!email) { slot.hidden = true; return slot; }
    slot.hidden = false;
    slot.innerHTML =
      `${escapeHtml(isZh() ? "已登录：" : "Signed in: ")}<span style="color:var(--text)">${escapeHtml(email)}</span>` +
      ` <button type="button" class="signout">${escapeHtml(isZh() ? "退出" : "Sign out")}</button>`;
    const btn = slot.querySelector(".signout");
    btn?.addEventListener("click", async () => {
      btn.disabled = true;
      try {
        await fetch(API + "/api/auth/logout", { method: "POST", credentials: "include", headers: { "Content-Type": "application/json" } });
      } catch (_) { /* ignore */ }
      location.replace("/register.html");
    });
    return me;
  }

  // ─── Main render orchestration ───────────────────────────
  let lastSnapshots = null;

  async function fetchAll() {
    const me = await renderAuth();
    const userEmail = me?.user?.email ? `?user_email=${encodeURIComponent(me.user.email)}` : "";
    const [model, latestRun, latestAnalysis, transmission, portfolio, actionBias, geo] = await Promise.all([
      api("/api/model/current"),
      api("/api/latest-run"),
      api("/api/latest-analysis"),
      api("/api/transmission/latest"),
      userEmail ? api("/api/portfolio/risk-summary" + userEmail) : Promise.resolve({ _err: "no_user" }),
      api("/api/action-bias/latest"),
      api("/api/geopolitical-overlay/latest"),
    ]);
    // Run-day-scoped overlay decision (best effort for why drawer)
    const asOf = latestRun?.as_of_date || latestAnalysis?.report_date;
    const overlayDecision = asOf ? await api(`/api/run/${asOf}/overlay-decision`) : null;
    return { model, latestRun, latestAnalysis, transmission, portfolio, actionBias, geo, overlayDecision };
  }

  function renderAll(s) {
    if (!s) return;
    lastSnapshots = s;
    renderTopBar(s.latestRun, s.latestAnalysis);
    renderSidebarStatus(s.latestRun, s.model);
    renderHero(s.latestAnalysis, s.latestRun);
    renderWhyDrawer(s.latestAnalysis, s.model, s.overlayDecision);
    renderDimGrid(s.model);
    renderTransmission(s.transmission);
    renderPortfolio(s.portfolio);
    renderActionPlan(s.actionBias, s.latestAnalysis);
    renderGeoBanner(s.geo, s.latestRun);
    renderAudit(s.latestRun, s.latestAnalysis, s.geo);
  }

  // ─── Boot + lang switch wiring ──────────────────────────
  async function boot() {
    const tStart = performance.now();
    console.debug(`[dashboard-v2] boot start | API base = ${API || "(empty/relative)"}`);
    try {
      const snapshots = await fetchAll();
      console.debug(`[dashboard-v2] fetchAll done in ${Math.round(performance.now() - tStart)}ms`);
      const tRender = performance.now();
      renderAll(snapshots);
      console.debug(`[dashboard-v2] renderAll done in ${Math.round(performance.now() - tRender)}ms`);
    } catch (e) {
      console.error("[dashboard-v2] boot failed", e?.stack || e);
    }
  }

  function bindLangToggle() {
    const btn = document.getElementById("lang-toggle");
    if (!btn || btn.dataset.dashboardV2Bound) return;
    btn.dataset.dashboardV2Bound = "1";
    btn.addEventListener("click", () => {
      // app.js setupLangToggle handles localStorage + applyI18n; we re-render
      // our sections shortly after so chip / note text catches the new lang.
      // If lastSnapshots is null the first boot is still in flight — it will
      // renderAll itself when fetchAll resolves, no need to spawn a 2nd boot
      // (would race against the in-flight one and double the API load).
      setTimeout(() => {
        if (lastSnapshots) renderAll(lastSnapshots);
      }, 30);
    });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", () => { boot(); bindLangToggle(); });
  } else {
    boot();
    bindLangToggle();
  }
})();
