const CW_API_BASE = (document.querySelector('meta[name="macro-api-base"]')?.content || "").trim() || "https://api.nexo.hk";

function cwApi(path) {
  return `${CW_API_BASE}${path.startsWith("/") ? path : `/${path}`}`;
}

async function cwGet(path) {
  try {
    const res = await fetch(cwApi(path), { credentials: "omit" });
    if (!res.ok) return null;
    return await res.json();
  } catch {
    return null;
  }
}

function cwText(el, html) {
  if (el) el.innerHTML = html;
}

function cwEsc(v) {
  return String(v ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}

function cwList(items) {
  if (!items?.length) return "<p class='subtle'>--</p>";
  return `<ul class="preview-list">${items.map((x) => `<li>${cwEsc(x)}</li>`).join("")}</ul>`;
}

function cwObjTable(obj) {
  if (!obj || typeof obj !== "object") return "<p class='subtle'>--</p>";
  const rows = Object.entries(obj);
  return `<table class="data-table"><tbody>${rows.map(([k, v]) => `<tr><th>${cwEsc(k)}</th><td>${cwEsc(Array.isArray(v) ? v.join(", ") : (typeof v === "object" ? JSON.stringify(v) : v))}</td></tr>`).join("")}</tbody></table>`;
}

async function renderDashboardCapitalWarning() {
  const [
    regimeRes,
    overlayRes,
    transRes,
    biasRes,
    alertsRes,
    summaryRes,
  ] = await Promise.all([
    cwGet("/api/regime/latest"),
    cwGet("/api/geopolitical-overlay/latest"),
    cwGet("/api/transmission/latest"),
    cwGet("/api/action-bias/latest"),
    cwGet("/api/alerts/latest"),
    cwGet("/api/portfolio/risk-summary?user_email=xiayiping@gmail.com"),
  ]);
  const regime = regimeRes?.item;
  const overlay = overlayRes?.item;
  const trans = transRes?.item;
  const bias = biasRes?.item;
  const alerts = alertsRes?.items || [];
  const portfolio = summaryRes?.summary;

  cwText(document.getElementById("regime-engine-card"),
    regime ? `
      <div class="summary-score">${cwEsc(regime.regime_label)}</div>
      <div class="summary-line">Code: ${cwEsc(regime.regime_code)}</div>
      <div class="summary-line">Confidence: ${cwEsc(regime.confidence)}</div>
      <div class="summary-line">Score: ${cwEsc(regime.score)} / Delta: ${cwEsc(regime.score_delta)}</div>
      ${cwList(regime.drivers_json || [])}
    ` : "<p class='subtle'>No regime snapshot.</p>"
  );
  cwText(document.getElementById("geopolitical-overlay-card"),
    overlay ? `
      <div class="summary-score">${cwEsc(overlay.oil_shock_scenario || "--")}</div>
      <div class="summary-line">Conflict: ${cwEsc(overlay.conflict_level || "--")}</div>
      <div class="summary-line">Supply: ${cwEsc(overlay.supply_disruption_level || "--")}</div>
      <div class="summary-line">Summary: ${cwEsc(overlay.summary || "--")}</div>
    ` : "<p class='subtle'>No overlay snapshot.</p>"
  );
  cwText(document.getElementById("action-bias-card"),
    bias ? `
      <div class="summary-score">${cwEsc(bias.overall_bias || "--")}</div>
      <div class="summary-line">Favored styles: ${cwEsc((bias.favored_styles_json || []).join(", "))}</div>
      <div class="summary-line">Avoided styles: ${cwEsc((bias.avoided_styles_json || []).join(", "))}</div>
      <div class="summary-line">${cwEsc(bias.summary || "--")}</div>
    ` : "<p class='subtle'>No action bias snapshot.</p>"
  );
  cwText(document.getElementById("portfolio-macro-risk-card"),
    portfolio ? `
      <div class="summary-score">${cwEsc(portfolio.average_macro_risk_score || 0)}</div>
      <div class="summary-line">Positions: ${cwEsc(portfolio.count || 0)}</div>
      <div class="summary-line">Top risk: ${cwEsc((portfolio.top_risk_positions || []).map((x) => x.ticker).join(", ") || "--")}</div>
      <div class="summary-line">Top benefit: ${cwEsc((portfolio.top_benefit_positions || []).map((x) => x.ticker).join(", ") || "--")}</div>
    ` : `
      <div class="summary-score">${alerts.length}</div>
      <div class="summary-line">Active market alerts</div>
      <div class="summary-line">${cwEsc(alerts.slice(0, 5).map((x) => x.alert_code).join(", "))}</div>
    `
  );
  cwText(document.getElementById("transmission-heatmap"),
    trans ? cwObjTable({
      rates_bias: trans.rates_bias,
      equities_bias: trans.equities_bias,
      credit_bias: trans.credit_bias,
      usd_bias: trans.usd_bias,
      commodities_bias: trans.commodities_bias,
      crypto_bias: trans.crypto_bias,
      favored_sectors: (trans.sectors_json || {}).favored || [],
      avoided_sectors: (trans.sectors_json || {}).avoided || [],
    }) : "<p class='subtle'>No transmission snapshot.</p>"
  );
}

async function renderDailyCapitalWarning() {
  const params = new URLSearchParams(location.search);
  const reportDate = params.get("date") || new Date().toISOString().slice(0, 10);
  const [regimeRes, impactRes, overlayRes, transRes, biasRes, watchRes] = await Promise.all([
    cwGet(`/api/reports/${reportDate}/regime`),
    cwGet(`/api/reports/${reportDate}/portfolio-impact?user_email=xiayiping@gmail.com`),
    cwGet("/api/geopolitical-overlay/latest"),
    cwGet("/api/transmission/latest"),
    cwGet("/api/action-bias/latest"),
    cwGet("/api/portfolio/risk-summary?user_email=xiayiping@gmail.com"),
  ]);
  const regime = regimeRes?.item;
  const impact = impactRes;
  const overlay = overlayRes?.item;
  const trans = transRes?.item;
  const bias = biasRes?.item;
  const watch = watchRes?.summary;
  cwText(document.getElementById("report-regime-block"),
    regime ? `
      <div class="summary-score">${cwEsc(regime.regime_label)}</div>
      <div class="summary-line">Confidence: ${cwEsc(regime.confidence)}</div>
      ${cwList(regime.drivers_json || [])}
    ` : "<p class='subtle'>--</p>");
  cwText(document.getElementById("report-transmission-block"),
    trans ? `
      <div class="summary-line">Rates: ${cwEsc(trans.rates_bias)}</div>
      <div class="summary-line">Equities: ${cwEsc(trans.equities_bias)}</div>
      <div class="summary-line">Credit: ${cwEsc(trans.credit_bias)}</div>
      <div class="summary-line">USD: ${cwEsc(trans.usd_bias)}</div>
    ` : "<p class='subtle'>--</p>");
  cwText(document.getElementById("report-portfolio-impact-block"),
    impact ? `
      <div class="summary-score">${cwEsc(impact.average_macro_risk_score || 0)}</div>
      <div class="summary-line">${cwEsc(impact.summary_text || "--")}</div>
    ` : "<p class='subtle'>--</p>");
  cwText(document.getElementById("report-action-bias-block"),
    bias ? `
      <div class="summary-score">${cwEsc(bias.overall_bias)}</div>
      <div class="summary-line">Favored sectors: ${cwEsc((bias.favored_sectors_json || []).join(", "))}</div>
      <div class="summary-line">Avoided sectors: ${cwEsc((bias.avoided_sectors_json || []).join(", "))}</div>
      <div class="summary-line">${cwEsc(bias.summary || "--")}</div>
    ` : "<p class='subtle'>--</p>");
  cwText(document.getElementById("report-overlay-block"),
    overlay ? `
      <div class="summary-score">${cwEsc(overlay.oil_shock_scenario)}</div>
      <div class="summary-line">Inflation impact: ${cwEsc(overlay.inflation_impact)}</div>
      <div class="summary-line">Risk asset impact: ${cwEsc(overlay.risk_asset_impact)}</div>
      <div class="summary-line">${cwEsc(overlay.summary || "--")}</div>
    ` : "<p class='subtle'>--</p>");
  cwText(document.getElementById("report-watchlist-block"),
    watch ? `
      <div class="summary-line">Top risk: ${cwEsc((watch.top_risk_positions || []).map((x) => x.ticker).join(", ") || "--")}</div>
      <div class="summary-line">Top benefit: ${cwEsc((watch.top_benefit_positions || []).map((x) => x.ticker).join(", ") || "--")}</div>
    ` : "<p class='subtle'>--</p>");
}

async function renderStockCapitalWarning() {
  const input = document.getElementById("stock-ticker-input");
  const ticker = String(input?.value || "PDD").trim().toUpperCase();
  const [exposureRes, signalRes] = await Promise.all([
    cwGet(`/api/stocks/${encodeURIComponent(ticker)}/macro-exposure`),
    cwGet(`/api/stocks/${encodeURIComponent(ticker)}/macro-signal/latest`),
  ]);
  cwText(document.getElementById("stock-macro-exposure"), exposureRes?.item ? cwObjTable({
    interest_rate: exposureRes.item.interest_rate_sensitivity,
    growth: exposureRes.item.growth_sensitivity,
    inflation: exposureRes.item.inflation_sensitivity,
    oil: exposureRes.item.oil_sensitivity,
    credit: exposureRes.item.credit_sensitivity,
    usd: exposureRes.item.usd_sensitivity,
    geopolitics: exposureRes.item.geopolitics_sensitivity,
    volatility: exposureRes.item.volatility_sensitivity,
  }) : "<p class='subtle'>No macro exposure data.</p>");
  cwText(document.getElementById("stock-macro-signal"), signalRes?.item ? `
    <div class="summary-score">${cwEsc(signalRes.item.signal || "--")}</div>
    <div class="summary-line">Action Bias: ${cwEsc(signalRes.item.action_bias || "--")}</div>
    <div class="summary-line">Macro Risk Score: ${cwEsc(signalRes.item.macro_risk_score || "--")}</div>
    <div class="summary-line">${cwEsc(signalRes.item.explanation_short || "--")}</div>
  ` : "<p class='subtle'>No macro signal data.</p>");
}

async function initPortfolioWatchlistPage() {
  const listRoot = document.getElementById("watchlist-list");
  const posRoot = document.getElementById("watchlist-positions");
  const riskRoot = document.getElementById("portfolio-risk-summary");
  const emailInput = document.getElementById("watchlist-email");
  const nameInput = document.getElementById("watchlist-name");
  const tickerInput = document.getElementById("position-ticker");
  const qtyInput = document.getElementById("position-qty");
  let currentWatchlistId = null;

  async function refreshSummary() {
    const email = String(emailInput?.value || "").trim().toLowerCase();
    if (!email) {
      cwText(riskRoot, "<p class='subtle'>Enter email to view risk summary.</p>");
      return;
    }
    const res = await cwGet(`/api/portfolio/risk-summary?user_email=${encodeURIComponent(email)}`);
    const s = res?.summary;
    cwText(riskRoot, s ? `
      <div class="summary-score">${cwEsc(s.average_macro_risk_score || 0)}</div>
      <div class="summary-line">Positions: ${cwEsc(s.count || 0)}</div>
      <div class="summary-line">Top risk: ${cwEsc((s.top_risk_positions || []).map((x) => x.ticker).join(", ") || "--")}</div>
      <div class="summary-line">Top benefit: ${cwEsc((s.top_benefit_positions || []).map((x) => x.ticker).join(", ") || "--")}</div>
    ` : "<p class='subtle'>No summary.</p>");
  }

  async function refreshPositions() {
    if (!currentWatchlistId) {
      cwText(posRoot, "<p class='subtle'>Create or select a watchlist first.</p>");
      return;
    }
    const res = await cwGet(`/api/portfolio/watchlists/${currentWatchlistId}/positions`);
    const rows = res?.items || [];
    if (!rows.length) {
      cwText(posRoot, "<p class='subtle'>No positions yet.</p>");
      return;
    }
    cwText(posRoot, `<table class="data-table"><thead><tr><th>Ticker</th><th>Qty</th><th>Macro Risk</th><th>Action</th></tr></thead><tbody>${
      rows.map((x) => `<tr><td>${cwEsc(x.ticker)}</td><td>${cwEsc(x.quantity)}</td><td>${cwEsc(x.macro_signal?.macro_risk_score ?? "--")}</td><td>${cwEsc(x.macro_signal?.action_bias ?? "--")}</td></tr>`).join("")
    }</tbody></table>`);
  }

  async function refreshLists() {
    const email = String(emailInput?.value || "").trim().toLowerCase();
    const res = await cwGet(`/api/portfolio/watchlists${email ? `?user_email=${encodeURIComponent(email)}` : ""}`);
    const rows = res?.items || [];
    if (!rows.length) {
      cwText(listRoot, "<p class='subtle'>No watchlists.</p>");
      return;
    }
    cwText(listRoot, rows.map((x) => `<button class="report-link" data-watchlist="${x.id}">${cwEsc(x.list_name)} · ${cwEsc(x.user_email)}</button>`).join(""));
    listRoot.querySelectorAll("[data-watchlist]").forEach((btn) => {
      btn.addEventListener("click", async () => {
        currentWatchlistId = btn.getAttribute("data-watchlist");
        await refreshPositions();
      });
    });
    if (!currentWatchlistId && rows[0]) currentWatchlistId = rows[0].id;
    await refreshPositions();
    await refreshSummary();
  }

  document.getElementById("watchlist-create")?.addEventListener("click", async () => {
    const email = String(emailInput?.value || "").trim().toLowerCase();
    const name = String(nameInput?.value || "").trim();
    if (!email || !name) return;
    await fetch(cwApi("/api/portfolio/watchlists"), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ user_email: email, list_name: name }),
    });
    await refreshLists();
  });

  document.getElementById("position-add")?.addEventListener("click", async () => {
    if (!currentWatchlistId) return;
    const ticker = String(tickerInput?.value || "").trim().toUpperCase();
    if (!ticker) return;
    await fetch(cwApi(`/api/portfolio/watchlists/${currentWatchlistId}/positions`), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ ticker, quantity: Number(qtyInput?.value || 0) || 0 }),
    });
    await refreshPositions();
    await refreshSummary();
  });

  await refreshLists();
}

async function initRegimeTransmissionPage() {
  const [regimeRes, histRes, overlayRes, transRes, biasRes] = await Promise.all([
    cwGet("/api/regime/latest"),
    cwGet("/api/regime/history?days=90"),
    cwGet("/api/geopolitical-overlay/latest"),
    cwGet("/api/transmission/latest"),
    cwGet("/api/action-bias/latest"),
  ]);
  const regime = regimeRes?.item;
  const hist = histRes?.items || [];
  const overlay = overlayRes?.item;
  const trans = transRes?.item;
  const bias = biasRes?.item;
  cwText(document.getElementById("rt-regime-card"), regime ? `
    <div class="summary-score">${cwEsc(regime.regime_label)}</div>
    <div class="summary-line">Code: ${cwEsc(regime.regime_code)}</div>
    <div class="summary-line">Confidence: ${cwEsc(regime.confidence)}</div>
    ${cwList(regime.drivers_json || [])}
  ` : "<p class='subtle'>--</p>");
  cwText(document.getElementById("rt-overlay-card"), overlay ? `
    <div class="summary-score">${cwEsc(overlay.oil_shock_scenario)}</div>
    <div class="summary-line">Conflict: ${cwEsc(overlay.conflict_level)}</div>
    <div class="summary-line">Supply: ${cwEsc(overlay.supply_disruption_level)}</div>
    <div class="summary-line">${cwEsc(overlay.summary || "--")}</div>
  ` : "<p class='subtle'>--</p>");
  cwText(document.getElementById("rt-regime-history"), hist.length ? `
    <table class="data-table"><thead><tr><th>Date</th><th>Regime</th><th>Score</th><th>Delta</th></tr></thead><tbody>${
      hist.map((x) => `<tr><td>${cwEsc(x.as_of_date)}</td><td>${cwEsc(x.regime_label)}</td><td>${cwEsc(x.score)}</td><td>${cwEsc(x.score_delta)}</td></tr>`).join("")
    }</tbody></table>` : "<p class='subtle'>No history.</p>");
  cwText(document.getElementById("rt-action-bias"), bias ? `
    <div class="summary-score">${cwEsc(bias.overall_bias)}</div>
    <div class="summary-line">Favored styles: ${cwEsc((bias.favored_styles_json || []).join(", "))}</div>
    <div class="summary-line">Avoided styles: ${cwEsc((bias.avoided_styles_json || []).join(", "))}</div>
    <div class="summary-line">${cwEsc(bias.summary || "--")}</div>
  ` : "<p class='subtle'>--</p>");
  cwText(document.getElementById("rt-transmission"), trans ? cwObjTable({
    rates: trans.rates_bias,
    equities: trans.equities_bias,
    credit: trans.credit_bias,
    usd: trans.usd_bias,
    commodities: trans.commodities_bias,
    crypto: trans.crypto_bias,
    favored: (trans.sectors_json || {}).favored || [],
    avoided: (trans.sectors_json || {}).avoided || [],
  }) : "<p class='subtle'>--</p>");
}

document.addEventListener("DOMContentLoaded", async () => {
  const page = document.body?.dataset?.page || "";
  if (page === "dashboard") await renderDashboardCapitalWarning();
  if (page === "daily-report") await renderDailyCapitalWarning();
  if (page === "stock-prediction") {
    await renderStockCapitalWarning();
    document.getElementById("stock-refresh")?.addEventListener("click", () => setTimeout(renderStockCapitalWarning, 50));
  }
  if (page === "portfolio-watchlist") await initPortfolioWatchlistPage();
  if (page === "regime-transmission") await initRegimeTransmissionPage();
});
