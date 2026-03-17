const api = {
  get: async (p) => {
    const r = await fetch(p, { credentials: 'include' });
    if (!r.ok) return null;
    return r.json();
  },
  post: async (p, body = {}) => {
    const r = await fetch(p, { method: 'POST', credentials: 'include', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
    if (!r.ok) return null;
    return r.json();
  },
  del: async (p) => {
    const r = await fetch(p, { method: 'DELETE', credentials: 'include' });
    if (!r.ok) return null;
    return r.json();
  },
  put: async (p, body = {}) => {
    const r = await fetch(p, { method: 'PUT', credentials: 'include', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
    if (!r.ok) return null;
    return r.json();
  }
};

const OPS_REFRESH_MS = 60 * 1000;
const OPS_WINDOW_MINUTES = 1440;
const DATA_API_BASE = 'https://api.nexo.hk';
const MONITOR_LANG_KEY = 'nexo-monitor-lang';
let visitsChart = null;
let tokensChart = null;
let humanAnswer = null;

function q(id){ return document.getElementById(id); }

const monitorI18n = {
  zh: {
    title_login: 'Nexo Monitor | 登录',
    title_ops: 'Nexo Monitor | 运维看板',
    title_subscribers: 'Nexo Monitor | 业务运营',
    title_forms: 'Nexo Monitor | 数据表单',
    title_form: 'Nexo Monitor | 表单详情',
    title_tool: 'Nexo Monitor | 数据生成工具',
    title_polymarket_risk: 'Nexo Monitor | Polymarket 风控配置',
    nav_ops: '运维看板',
    nav_business: '业务运营',
    nav_tool: '数据生成工具',
    nav_forms: '数据表单',
    nav_polymarket_risk: 'Polymarket 风控',
    logout: '退出',
    login_title: 'Nexo Monitor Console',
    login_sub: '运维、业务与数据表单分析',
    login_google: 'Google 登录',
    login_failed: 'Google 登录失败，请重试。',
    login_forbidden: '当前 Google 账号不在白名单内。',
    login_oauth_missing: '服务器尚未配置 Google OAuth。',
    human_question: '人类验证',
    human_placeholder: '请输入答案',
    human_verify: '验证',
    human_needed: '请先完成人类验证。',
    human_pass: '验证通过，可以登录。',
    human_fail: '答案错误，请重试。',
    ops_page_visits: '页面访问量',
    ops_since_launch: '上线累计',
    ops_input_tokens: '输入 Token',
    ops_output_tokens: '输出 Token',
    ops_total_tokens: '总 Token',
    ops_visits_by_minute: '分钟级访问量',
    ops_tokens_by_minute: '分钟级 Token 用量',
    ops_top_pages: '热门页面',
    subscribers_title: '订阅用户',
    forms_title: '数据表单目录',
    form_title: '表单',
    no_data: '暂无数据。',
    no_reports: '暂无已保存报告。',
    active_subscribers: '当前有效订阅数',
    sub_time: '订阅时间',
    sub_email: '订阅邮箱地址',
    sub_welcome: '订阅成功邮件是否发送',
    sub_daily: '当日日报是否发送',
    sub_delete: '删除邮箱地址',
    yes: '是',
    no: '否',
    delete_btn: '删除',
    delete_confirm: '确认删除订阅邮箱',
    delete_failed: '删除失败，请重试。',
    tool_regen: '重新生成草稿',
    tool_finalize: '生成最终报告',
    tool_save: '保存',
    tool_download: '下载 .txt',
    tool_run_check: '生成最终报告前执行在线数据校验',
    tool_archive_title: '每日报告归档',
    tool_archive_desc: '每个保存日期都提供直接访问链接。',
    tool_check_title: '在线数据校验结果',
    tool_indicator_verification: '指标在线校验状态',
    tool_status_score: '综合评分',
    tool_status_signal: '信号',
    tool_open: '打开',
    tool_regened: '草稿已重新生成。',
    tool_checking: '正在执行在线数据校验...',
    tool_final_saved: '最终报告已生成并保存。',
    tool_saved: '已保存。',
    tool_save_failed: '保存失败，请重试。',
    chart_visits: '访问量',
    chart_input: '输入',
    chart_output: '输出',
    pm_risk_title: 'Polymarket 风控模板配置',
    pm_account: '账户',
    pm_load: '加载当前配置',
    pm_use_template: '使用默认模板',
    pm_save: '保存配置',
    pm_saved: '风控配置已保存。',
    pm_save_failed: '保存失败，请重试。',
    pm_no_account: '暂无 Polymarket 账户，请先完成账户连接。',
    pm_loaded: '已加载当前风控配置。'
  },
  en: {
    title_login: 'Nexo Monitor | Login',
    title_ops: 'Nexo Monitor | Ops Dashboard',
    title_subscribers: 'Nexo Monitor | Business',
    title_forms: 'Nexo Monitor | Data Forms',
    title_form: 'Nexo Monitor | Form Detail',
    title_tool: 'Nexo Monitor | Data Tool',
    title_polymarket_risk: 'Nexo Monitor | Polymarket Risk Config',
    nav_ops: 'Ops Dashboard',
    nav_business: 'Business',
    nav_tool: 'Data Tool',
    nav_forms: 'Data Forms',
    nav_polymarket_risk: 'Polymarket Risk',
    logout: 'Logout',
    login_title: 'Nexo Monitor Console',
    login_sub: 'Operations, business and data-form analytics.',
    login_google: 'Sign in with Google',
    login_failed: 'Google login failed, please retry.',
    login_forbidden: 'Your Google account is not authorized.',
    login_oauth_missing: 'Google OAuth is not configured on server yet.',
    human_question: 'Human Verification',
    human_placeholder: 'Enter answer',
    human_verify: 'Verify',
    human_needed: 'Please complete human verification first.',
    human_pass: 'Verification passed. You can sign in now.',
    human_fail: 'Wrong answer, please try again.',
    ops_page_visits: 'Page Visits',
    ops_since_launch: 'Since Launch',
    ops_input_tokens: 'Input Tokens',
    ops_output_tokens: 'Output Tokens',
    ops_total_tokens: 'Total Tokens',
    ops_visits_by_minute: 'Visits by Minute',
    ops_tokens_by_minute: 'Token Usage by Minute',
    ops_top_pages: 'Top Pages',
    subscribers_title: 'Subscribed Users',
    forms_title: 'Data Form Directory',
    form_title: 'Form',
    no_data: 'No data.',
    no_reports: 'No saved reports.',
    active_subscribers: 'Active subscribers',
    sub_time: 'Subscribed At',
    sub_email: 'Email',
    sub_welcome: 'Welcome Email Sent',
    sub_daily: 'Daily Report Sent Today',
    sub_delete: 'Delete',
    yes: 'Yes',
    no: 'No',
    delete_btn: 'Delete',
    delete_confirm: 'Delete subscriber',
    delete_failed: 'Delete failed, please retry.',
    tool_regen: 'Regenerate Draft',
    tool_finalize: 'Generate Final Report',
    tool_save: 'Save',
    tool_download: 'Download .txt',
    tool_run_check: 'Run online data check before final report',
    tool_archive_title: 'Daily Report Archive',
    tool_archive_desc: 'Each saved date has a direct link.',
    tool_check_title: 'Online Data Check Results',
    tool_indicator_verification: 'Indicator Verification Status',
    tool_status_score: 'Score',
    tool_status_signal: 'Signal',
    tool_open: 'Open',
    tool_regened: 'Draft regenerated.',
    tool_checking: 'Running online data check...',
    tool_final_saved: 'Final report generated and saved.',
    tool_saved: 'Saved.',
    tool_save_failed: 'Save failed, please retry.',
    chart_visits: 'Visits',
    chart_input: 'Input',
    chart_output: 'Output',
    pm_risk_title: 'Polymarket Risk Template Config',
    pm_account: 'Account',
    pm_load: 'Load Current',
    pm_use_template: 'Use Default Template',
    pm_save: 'Save Config',
    pm_saved: 'Risk limits saved.',
    pm_save_failed: 'Save failed, please retry.',
    pm_no_account: 'No Polymarket account found. Please connect account first.',
    pm_loaded: 'Current risk limits loaded.'
  }
};

function getMonitorLang(){
  if (document.body?.dataset?.page === 'login') return 'en';
  const raw = String(localStorage.getItem(MONITOR_LANG_KEY) || '').toLowerCase();
  return raw === 'en' ? 'en' : 'zh';
}

function setMonitorLang(lang){
  localStorage.setItem(MONITOR_LANG_KEY, lang === 'en' ? 'en' : 'zh');
}

function mt(key){
  const lang = getMonitorLang();
  return (monitorI18n[lang] || monitorI18n.zh)[key] || (monitorI18n.zh[key] || key);
}

function applyMonitorI18n(){
  const page = document.body?.dataset?.page || '';
  if(page === 'login') document.title = mt('title_login');
  if(page === 'ops') document.title = mt('title_ops');
  if(page === 'subscribers') document.title = mt('title_subscribers');
  if(page === 'forms') document.title = mt('title_forms');
  if(page === 'form-detail') document.title = mt('title_form');
  if(page === 'data-tool') document.title = mt('title_tool');
  if(page === 'polymarket-risk') document.title = mt('title_polymarket_risk');
  document.documentElement.lang = getMonitorLang() === 'en' ? 'en' : 'zh-CN';
  document.querySelectorAll('[data-i18n]').forEach((el)=>{
    const key = el.getAttribute('data-i18n');
    if(!key) return;
    el.textContent = mt(key);
  });
  document.querySelectorAll('[data-i18n-placeholder]').forEach((el)=>{
    const key = el.getAttribute('data-i18n-placeholder');
    if(!key) return;
    el.setAttribute('placeholder', mt(key));
  });
  const toggle = q('lang-toggle');
  if(toggle) toggle.textContent = getMonitorLang() === 'zh' ? 'EN' : '中文';
}

function setupMonitorLangToggle(){
  const toggle = q('lang-toggle');
  if(!toggle) return;
  toggle.addEventListener('click', ()=>{
    setMonitorLang(getMonitorLang() === 'zh' ? 'en' : 'zh');
    applyMonitorI18n();
    location.reload();
  });
}

function ensureSiteFooter(){
  if(document.querySelector('.site-footer-note')) return;
  const footer = document.createElement('footer');
  footer.className = 'site-footer-note';
  footer.textContent = getMonitorLang() === 'en'
    ? 'Powered by Nexo Marco Intelligence'
    : '由 Nexo Marco Intelligence 提供支持';
  document.body.appendChild(footer);
}

function dataApiUrl(path){
  const clean = String(path || '').startsWith('/') ? String(path) : `/${String(path || '')}`;
  return `${DATA_API_BASE}${clean}`;
}

async function dataGet(path){
  try{
    const r = await fetch(dataApiUrl(path), { credentials: 'omit' });
    if(!r.ok) return null;
    return await r.json();
  }catch(_err){
    return null;
  }
}

async function dataPost(path, body = {}){
  try{
    const r = await fetch(dataApiUrl(path), {
      method: 'POST',
      credentials: 'omit',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body)
    });
    if(!r.ok) return null;
    return await r.json();
  }catch(_err){
    return null;
  }
}

function trackMonitorPageView(){
  const payload = { path: `${location.pathname}${location.search || ''}`, referrer: document.referrer || '' };
  fetch('/monitor-api/track/page', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload) }).catch(()=>{});
}

function tableFromRows(rows){
  if(!rows || !rows.length) return `<p class="muted">${mt('no_data')}</p>`;
  const cols=[...new Set(rows.flatMap(r=>Object.keys(r||{})))];
  const thead='<tr>'+cols.map(c=>`<th>${c}</th>`).join('')+'</tr>';
  const tbody=rows.map(r=>'<tr>'+cols.map(c=>`<td>${String(r[c]??'')}</td>`).join('')+'</tr>').join('');
  return `<div class="table"><table><thead>${thead}</thead><tbody>${tbody}</tbody></table></div>`;
}

function objectRowsToColumns(rows){
  const cols = new Set();
  (rows || []).forEach((row)=>Object.keys(row || {}).forEach((k)=>cols.add(k)));
  return [...cols];
}

function renderObjectTable(targetId, rows){
  const root = q(targetId);
  if(!root) return;
  const dataRows = Array.isArray(rows) ? rows : [];
  const columns = objectRowsToColumns(dataRows);
  if(!dataRows.length || !columns.length){
    root.innerHTML = `<p class="table-empty">${mt('no_data')}</p>`;
    return;
  }
  const head = `<thead><tr>${columns.map((c)=>`<th>${c}</th>`).join('')}</tr></thead>`;
  const body = `<tbody>${dataRows.map((row)=>`<tr>${columns.map((c)=>`<td>${asText(row[c])}</td>`).join('')}</tr>`).join('')}</tbody>`;
  root.innerHTML = `<table class="data-table">${head}${body}</table>`;
}

function asText(v){ return v == null ? '' : String(v); }
function round(v, d = 1){
  const n = Number(v);
  if(!Number.isFinite(n)) return 0;
  const p = 10 ** d;
  return Math.round(n * p) / p;
}
function todayISO(){ return new Date().toISOString().slice(0,10); }
function keyByIncludes(row, patterns){
  const keys = Object.keys(row || {});
  return keys.find((k)=>patterns.some((p)=>k.toLowerCase().includes(p))) || null;
}
function pickInputCodeKey(inputs){
  const first = (inputs || [])[0] || {};
  return keyByIncludes(first, ['indicatorcode','code']) || 'IndicatorCode';
}
function pickInputValueKey(inputs){
  const first = (inputs || [])[0] || {};
  return keyByIncludes(first, ['latestvalue','value']) || 'LatestValue';
}
function extractSingleSeriesCode(raw){
  const text = asText(raw);
  const m = text.match(/\b[A-Z][A-Z0-9_]{1,20}\b/);
  return m ? m[0] : '';
}
async function fetchFredLatestValue(seriesCode){
  const url = `https://fred.stlouisfed.org/graph/fredgraph.csv?id=${encodeURIComponent(seriesCode)}`;
  const r = await fetch(url);
  if(!r.ok) throw new Error(`HTTP ${r.status}`);
  const csv = await r.text();
  const lines = csv.split(/\r?\n/).slice(1).filter(Boolean);
  for(let i = lines.length - 1; i >= 0; i--){
    const [date, value] = lines[i].split(',');
    if(value && value !== '.') return { date, value: Number(value) };
  }
  throw new Error('No valid value');
}

function generateToolDraft(model, date, onlineSummary){
  const dims = (model.dimensions || []).slice();
  const top = dims.sort((a,b)=>(b.score||0)-(a.score||0)).slice(0,3);
  const bottom = dims.sort((a,b)=>(a.score||0)-(b.score||0)).slice(0,3);
  const alerts = (model.alerts || []).filter((a)=>a.triggered);
  const lines = [
    '宏观监控每日报告',
    `报告日期: ${date}`,
    `模型更新日: ${asText(model.asOf || date)}`,
    `综合得分: ${round(model.totalScore || 0, 1)} (${asText(model.status || '--')})`,
    '',
    '执行摘要',
    `- 当前模型处于“${asText(model.status || '--')}”状态，总分 ${round(model.totalScore || 0, 1)}。`,
    `- 当前触发预警 ${alerts.length} 条。`
  ];
  if(onlineSummary){
    lines.push(`- 在线数据校验：检查 ${onlineSummary.checked} 项，更新 ${onlineSummary.updated} 项，失败 ${onlineSummary.failed} 项。`);
  }
  lines.push('', '主要支撑维度');
  top.forEach((x)=>lines.push(`- ${asText(x.name)}: ${round(x.score,1)}`));
  lines.push('', '主要拖累维度');
  bottom.forEach((x)=>lines.push(`- ${asText(x.name)}: ${round(x.score,1)}`));
  return lines.join('\n');
}

async function runOnlineDataCheckForTool(model){
  const indicators = model?.tables?.indicators || [];
  const inputs = model?.tables?.inputs || [];
  const inputCodeKey = pickInputCodeKey(inputs);
  const sourceKey = indicators.length ? keyByIncludes(indicators[0], ['sourceurl','source','数据源']) : null;
  const seriesKey = indicators.length ? keyByIncludes(indicators[0], ['series/code','series','code','建议系列']) : null;
  const indCodeKey = indicators.length ? keyByIncludes(indicators[0], ['indicatorcode','code']) : null;
  const results = [];
  let checked = 0, updated = 0, failed = 0;
  for(const row of indicators){
    const source = asText(row[sourceKey]).toLowerCase();
    const code = extractSingleSeriesCode(row[seriesKey] || row[indCodeKey]);
    const indicator = asText(row[indCodeKey] || code);
    const shouldCheck = source.includes('fred') || /^([A-Z]{2,}|[A-Z0-9_]+)$/.test(code);
    if(!shouldCheck || !code) continue;
    checked += 1;
    try{
      const latest = await fetchFredLatestValue(code);
      updated += 1;
      results.push({ 指标: indicator, 来源: 'FRED', 序列: code, 状态: 'OK', 最新日期: latest.date, 最新值: latest.value, 错误: '' });
    }catch(err){
      failed += 1;
      results.push({ 指标: indicator, 来源: 'FRED', 序列: code, 状态: 'FAILED', 最新日期: '', 最新值: '', 错误: asText(err?.message || 'Failed to fetch') });
    }
  }
  return { checked, updated, failed, results };
}

function renderOnlineCheckTableForTool(rows){
  const mapped = (rows || []).map((row)=>({
    指标: asText(row.indicator || row['指标']),
    来源: asText(row.source || row['来源']),
    序列: asText(row.series || row['序列']),
    状态: asText(row.status || row['状态']),
    最新日期: asText(row.latestDate || row['最新日期']),
    最新值: asText(row.latestValue || row['最新值']),
    错误: asText(row.error || row['错误'])
  }));
  renderObjectTable('online-check-table', mapped);
}

function renderIndicatorVerificationTableForTool(rows){
  const zh = getMonitorLang() === 'zh';
  const mapped = (rows || []).map((r)=>({
    [zh ? '指标代码' : 'IndicatorCode']: asText(r.IndicatorCode || r.indicatorCode || ''),
    [zh ? '指标名称' : 'IndicatorName']: asText(r.IndicatorName || r.indicatorName || ''),
    [zh ? '维度ID' : 'DimensionID']: asText(r.DimensionID || r.dimensionId || ''),
    [zh ? '最新值' : 'LatestValue']: asText(r.LatestValue || r.latestValue || ''),
    [zh ? '值日期' : 'ValueDate']: asText(r.ValueDate || r.valueDate || ''),
    [zh ? '来源日期' : 'SourceDate']: asText(r.SourceDate || r.sourceDate || ''),
    [zh ? '是否在线校验' : 'VerifiedOnline']: asText(r.VerifiedOnline ?? r.verifiedOnline ?? ''),
    [zh ? '校验状态' : 'VerificationStatus']: asText(r.VerificationStatus || r.verificationStatus || ''),
    [zh ? '校验错误' : 'VerificationError']: asText(r.VerificationError || r.verificationError || ''),
    [zh ? '生成时间' : 'GeneratedAt']: asText(r.GeneratedAt || r.generatedAt || '')
  }));
  renderObjectTable('indicator-verification-table', mapped);
}

function renderToolReportLinks(reports){
  const root = q('report-links');
  if(!root) return;
  const list = Array.isArray(reports) ? reports : [];
  if(!list.length){
    root.innerHTML = `<p class="table-empty">${mt('no_reports')}</p>`;
    return;
  }
  root.innerHTML = '';
  list.forEach((report)=>{
    const date = asText(report.date || '--');
    const score = asText(report?.meta?.score ?? '--');
    const status = asText(report?.meta?.status ?? '--');
    const item = document.createElement('article');
    item.className = 'report-item';
    item.innerHTML = `
      <div class="report-item-main">
        <div class="report-date">${date}</div>
        <div class="badge-row">
          <span class="badge">${mt('tool_status_score')}: ${score}</span>
          <span class="badge">${mt('tool_status_signal')}: ${status}</span>
        </div>
      </div>
      <a class="report-open" href="https://nexo.hk/daily-report.html?date=${encodeURIComponent(date)}" target="_blank" rel="noopener noreferrer">${mt('tool_open')}</a>
    `;
    root.appendChild(item);
  });
}

function getReportDateFromUrl(){
  const p = new URLSearchParams(location.search);
  const d = p.get('date');
  return /^\d{4}-\d{2}-\d{2}$/.test(asText(d)) ? d : todayISO();
}

async function requireAuth(){
  const me = await api.get('/monitor-api/auth/me');
  if(!me?.ok){
    if(location.pathname.endsWith('/index.html') || location.pathname === '/') return null;
    location.href='./index.html';
    return null;
  }
  if(q('user-email')) q('user-email').textContent = me.user.email;
  return me.user;
}

function setupLogout(){
  const btn=q('logout-btn');
  if(!btn) return;
  btn.addEventListener('click', async()=>{
    try { sessionStorage.removeItem('monitor_human_verified'); } catch(_err) {}
    await api.post('/monitor-api/auth/logout',{});
    location.href='./index.html';
  });
}

async function initLogin(){
  const me = await api.get('/monitor-api/auth/me');
  if(me?.ok){ location.href='./dashboard.html'; return; }

  const statusEl = q('login-status');
  const googleBtn = q('google-login');
  const questionEl = q('human-question');
  const answerEl = q('human-answer');
  const verifyBtn = q('human-verify');
  const hasHumanCheck = !!(questionEl && answerEl && verifyBtn && googleBtn);
  const normalizeNum = (raw) => {
    const s = String(raw || '').trim();
    if(!s) return null;
    const n = Number(s);
    return Number.isFinite(n) ? n : null;
  };

  const updateHumanQuestion = () => {
    if(!hasHumanCheck) return;
    const a = Math.floor(Math.random() * 8) + 1;
    const b = Math.floor(Math.random() * 8) + 1;
    humanAnswer = a + b;
    questionEl.textContent = `${mt('human_question')}: ${a} + ${b} = ?`;
    answerEl.value = '';
  };

  const passHumanCheck = () => {
    if(!googleBtn) return;
    googleBtn.disabled = false;
    if(statusEl) statusEl.textContent = mt('human_pass');
  };

  if(hasHumanCheck){
    googleBtn.disabled = true;
    const doVerify = ()=>{
      const v = normalizeNum(answerEl.value);
      if(Number.isFinite(v) && v === humanAnswer){
        passHumanCheck();
        return;
      }
      if(statusEl) statusEl.textContent = mt('human_fail');
      updateHumanQuestion();
    };
    updateHumanQuestion();
    verifyBtn.addEventListener('click', doVerify);
    answerEl.addEventListener('keydown', (e)=>{
      if(e.key === 'Enter'){
        e.preventDefault();
        doVerify();
      }
    });
    window.addEventListener('pageshow', ()=>{
      googleBtn.disabled = true;
      if(statusEl) statusEl.textContent = '';
      updateHumanQuestion();
    });
  }

  const btn=q('google-login');
  if(btn){
    btn.addEventListener('click', ()=>{
      if(btn.disabled){
        if(statusEl) statusEl.textContent = mt('human_needed');
        return;
      }
      location.href='/monitor-api/auth/google/start';
    });
  }
  const p=new URLSearchParams(location.search);
  if(p.get('auth')==='failed' && statusEl) statusEl.textContent = mt('login_failed');
  if(p.get('auth')==='forbidden' && statusEl) statusEl.textContent = mt('login_forbidden');
  const probe = await api.get('/monitor-api/health');
  if(probe?.oauthConfigured === false && statusEl) statusEl.textContent = mt('login_oauth_missing');
}

async function initOps(){
  if(!(await requireAuth())) return;
  setupLogout();
  const renderOps = async () => {
  const data = await api.get(`/monitor-api/ops/overview?days=30&minutes=${OPS_WINDOW_MINUTES}`);
  if(!data) return;
  q('kpi-visits').textContent = data.totals.pageVisits;
  q('kpi-input').textContent = data.totals.inputTokens;
  q('kpi-output').textContent = data.totals.outputTokens;
  q('kpi-total').textContent = data.totals.totalTokens;
  if(q('ops-window-label')) q('ops-window-label').textContent = mt('ops_since_launch');
  q('top-pages').innerHTML = tableFromRows(data.visitsByPath || []);

  const vctx = q('visits-chart');
  if(vctx && window.Chart){
    const visitsSeries = data.visitsMinute || [];
    const visitsLabels = visitsSeries.map(x => (x.minute || '').slice(11, 16) || (x.minute || ''));
    const visitsConfig = {type:'line',data:{labels:visitsLabels,datasets:[{label:mt('chart_visits'),data:visitsSeries.map(x=>x.visits),borderColor:'#0d7e6b',fill:false}]},options:{responsive:true,maintainAspectRatio:false}};
    if(visitsChart){
      visitsChart.data = visitsConfig.data;
      visitsChart.update();
    } else {
      visitsChart = new Chart(vctx, visitsConfig);
    }
  }
  const tctx = q('tokens-chart');
  if(tctx && window.Chart){
    const tokenSeries = data.tokensMinute || [];
    const tokenLabels = tokenSeries.map(x => (x.minute || '').slice(11, 16) || (x.minute || ''));
    const tokensConfig = {type:'bar',data:{labels:tokenLabels,datasets:[{label:mt('chart_input'),data:tokenSeries.map(x=>x.input_tokens),backgroundColor:'#6bb8a8'},{label:mt('chart_output'),data:tokenSeries.map(x=>x.output_tokens),backgroundColor:'#1f8b73'}]},options:{responsive:true,maintainAspectRatio:false}};
    if(tokensChart){
      tokensChart.data = tokensConfig.data;
      tokensChart.update();
    } else {
      tokensChart = new Chart(tctx, tokensConfig);
    }
  }
  };

  await renderOps();
  setInterval(renderOps, OPS_REFRESH_MS);
}

async function initSubscribers(){
  if(!(await requireAuth())) return;
  setupLogout();
  const render = async () => {
    const data = await api.get('/monitor-api/biz/subscribers');
    if(!data) return;
    q('sub-count').textContent = `${mt('active_subscribers')}: ${data.count}`;
    const rows = data.subscribers || [];
    const header = `
      <tr>
        <th>${mt('sub_time')}</th>
        <th>${mt('sub_email')}</th>
        <th>${mt('sub_welcome')}</th>
        <th>${mt('sub_daily')}</th>
        <th>${mt('sub_delete')}</th>
      </tr>`;
    const body = rows.map((r)=>{
      const createdAt = String(r.created_at || '').replace('T',' ').replace('Z','');
      const welcome = r.welcome_email_sent ? mt('yes') : mt('no');
      const daily = r.daily_report_sent_today ? mt('yes') : mt('no');
      const email = String(r.email || '');
      return `<tr>
        <td>${createdAt}</td>
        <td>${email}</td>
        <td>${welcome}</td>
        <td>${daily}</td>
        <td><button class="btn ghost sub-delete-btn" data-email="${email}">${mt('delete_btn')}</button></td>
      </tr>`;
    }).join('');
    q('sub-table').innerHTML = `<div class="table"><table><thead>${header}</thead><tbody>${body}</tbody></table></div>`;
    q('sub-table').querySelectorAll('.sub-delete-btn').forEach((btn)=>{
      btn.addEventListener('click', async()=>{
        const email = btn.getAttribute('data-email') || '';
        if(!email) return;
        const ok = window.confirm(`${mt('delete_confirm')} ${email} ?`);
        if(!ok) return;
        btn.disabled = true;
        const res = await api.del(`/monitor-api/biz/subscribers/${encodeURIComponent(email)}`);
        btn.disabled = false;
        if(!res?.ok){
          window.alert(mt('delete_failed'));
          return;
        }
        await render();
      });
    });
  };
  await render();
}

async function initForms(){
  if(!(await requireAuth())) return;
  setupLogout();
  const data = await api.get('/monitor-api/data/forms');
  if(!data) return;
  q('forms-list').innerHTML = (data.forms||[]).map(f=>`<a href="./form.html?name=${encodeURIComponent(f.name)}"><span>${f.label}</span><span>${f.count}</span></a>`).join('');
}

function _pmGetNumber(id, fallback = 0){
  const v = Number(q(id)?.value);
  return Number.isFinite(v) ? v : fallback;
}

function _pmGetBool(id){
  return q(id)?.checked ? 1 : 0;
}

function _pmApplyLimits(lim){
  const setValue = (id, value) => { if(q(id)) q(id).value = value ?? ''; };
  setValue('pm-max-daily-notional', lim?.max_daily_notional ?? 500);
  setValue('pm-max-order-notional', lim?.max_order_notional ?? 50);
  setValue('pm-max-market-exposure', lim?.max_market_exposure ?? 120);
  setValue('pm-max-slippage-bps', lim?.max_slippage_bps ?? 20);
  setValue('pm-max-open-orders', lim?.max_open_orders ?? 3);
  setValue('pm-min-expected-edge-bps', lim?.min_expected_edge_bps ?? 40);
  setValue('pm-min-model-confidence', lim?.min_model_confidence ?? 0.65);
  setValue('pm-min-orderbook-depth', lim?.min_orderbook_depth ?? 300);
  setValue('pm-order-cooldown-sec', lim?.order_cooldown_sec ?? 30);
  setValue('pm-max-daily-loss', lim?.max_daily_realized_loss ?? 80);
  setValue('pm-max-consecutive-failed', lim?.max_consecutive_failed_orders ?? 5);
  setValue('pm-max-reject-ratio', lim?.max_reject_ratio_pct_10m ?? 40);
  setValue('pm-default-order-type', lim?.default_order_type || 'GTC');
  setValue('pm-cancel-stale-after-sec', lim?.cancel_stale_after_sec ?? 120);
  if(q('pm-auto-trading')) q('pm-auto-trading').checked = !!Number(lim?.auto_trading ?? 0);
  if(q('pm-halt-api-degraded')) q('pm-halt-api-degraded').checked = !!Number(lim?.halt_on_api_degraded ?? 1);
  if(q('pm-post-only')) q('pm-post-only').checked = !!Number(lim?.post_only ?? 1);
  if(q('pm-allow-taker')) q('pm-allow-taker').checked = !!Number(lim?.allow_taker ?? 0);
  if(q('pm-paper-mode')) q('pm-paper-mode').checked = !!Number(lim?.paper_mode ?? 1);
}

function _pmCollectLimits(){
  return {
    max_daily_notional: _pmGetNumber('pm-max-daily-notional', 500),
    max_order_notional: _pmGetNumber('pm-max-order-notional', 50),
    max_market_exposure: _pmGetNumber('pm-max-market-exposure', 120),
    max_slippage_bps: _pmGetNumber('pm-max-slippage-bps', 20),
    max_open_orders: _pmGetNumber('pm-max-open-orders', 3),
    auto_trading: _pmGetBool('pm-auto-trading'),
    min_expected_edge_bps: _pmGetNumber('pm-min-expected-edge-bps', 40),
    min_model_confidence: _pmGetNumber('pm-min-model-confidence', 0.65),
    min_orderbook_depth: _pmGetNumber('pm-min-orderbook-depth', 300),
    order_cooldown_sec: _pmGetNumber('pm-order-cooldown-sec', 30),
    max_daily_realized_loss: _pmGetNumber('pm-max-daily-loss', 80),
    max_consecutive_failed_orders: _pmGetNumber('pm-max-consecutive-failed', 5),
    max_reject_ratio_pct_10m: _pmGetNumber('pm-max-reject-ratio', 40),
    halt_on_api_degraded: _pmGetBool('pm-halt-api-degraded'),
    default_order_type: String(q('pm-default-order-type')?.value || 'GTC').toUpperCase(),
    post_only: _pmGetBool('pm-post-only'),
    allow_taker: _pmGetBool('pm-allow-taker'),
    cancel_stale_after_sec: _pmGetNumber('pm-cancel-stale-after-sec', 120),
    paper_mode: _pmGetBool('pm-paper-mode'),
  };
}

async function initPolymarketRisk(){
  if(!(await requireAuth())) return;
  setupLogout();
  const accountSelect = q('pm-account');
  const statusEl = q('pm-risk-status');
  if(!accountSelect) return;
  const setStatus = (key) => { if(statusEl) statusEl.textContent = mt(key); };

  const accountRes = await api.get('/monitor-api/polymarket/accounts');
  const accounts = Array.isArray(accountRes?.items) ? accountRes.items : [];
  accountSelect.innerHTML = accounts.map((a)=>`<option value="${asText(a.id)}">${asText(a.id)} · ${asText(a.funder_address)}</option>`).join('');
  if(!accounts.length){
    setStatus('pm_no_account');
    return;
  }

  const loadCurrent = async () => {
    const accountId = asText(accountSelect.value);
    if(!accountId) return;
    const res = await api.get(`/monitor-api/polymarket/risk-limits?account_id=${encodeURIComponent(accountId)}`);
    if(!res?.ok) return;
    _pmApplyLimits(res.limits || res.template || {});
    setStatus('pm_loaded');
  };

  accountSelect.addEventListener('change', loadCurrent);
  q('pm-load')?.addEventListener('click', loadCurrent);
  q('pm-template')?.addEventListener('click', async ()=>{
    const accountId = asText(accountSelect.value);
    if(!accountId) return;
    const res = await api.get(`/monitor-api/polymarket/risk-limits?account_id=${encodeURIComponent(accountId)}`);
    _pmApplyLimits(res?.template || {});
  });
  q('pm-save')?.addEventListener('click', async ()=>{
    const accountId = asText(accountSelect.value);
    if(!accountId) return;
    const out = await api.put('/monitor-api/polymarket/risk-limits', {
      account_id: accountId,
      limits: _pmCollectLimits(),
    });
    setStatus(out?.ok ? 'pm_saved' : 'pm_save_failed');
  });

  await loadCurrent();
}

async function initFormDetail(){
  if(!(await requireAuth())) return;
  setupLogout();
  const name = new URLSearchParams(location.search).get('name') || 'dimensions';
  q('form-title').textContent = `${mt('form_title')}: ${name}`;
  const data = await api.get(`/monitor-api/data/forms/${encodeURIComponent(name)}`);
  q('form-table').innerHTML = tableFromRows(data?.rows || []);
}

async function initDataTool(){
  if(!(await requireAuth())) return;
  setupLogout();
  const editor = q('report-editor');
  const saveStatus = q('save-status');
  const btnGen = q('generate-report');
  const btnFinal = q('finalize-report');
  const btnSave = q('save-report');
  const btnDl = q('download-report');
  const runCheck = q('run-online-check');
  if(!editor) return;

  const date = getReportDateFromUrl();
  const model = await dataGet('/api/model/current') || await dataGet('/api/model/current?view=core') || {};
  const existing = await dataGet(`/api/reports/${encodeURIComponent(date)}`);
  const reports = await dataGet('/api/reports?limit=30');
  const latestCheck = await dataGet('/api/checks/latest');
  editor.value = asText(existing?.text) || generateToolDraft(model, date, null);
  renderOnlineCheckTableForTool(model.onlineCheck || latestCheck?.rows || []);
  renderIndicatorVerificationTableForTool(existing?.reportPayload?.indicatorDetails || model.indicatorDetails || []);
  renderToolReportLinks(reports?.reports || []);

  btnGen?.addEventListener('click', async()=>{
    const m = await dataGet('/api/model/current') || model;
    editor.value = generateToolDraft(m, date, null);
    if(saveStatus) saveStatus.textContent = mt('tool_regened');
  });

  btnFinal?.addEventListener('click', async()=>{
    const m = await dataGet('/api/model/current') || model;
    let summary = null;
    if(runCheck?.checked){
      if(saveStatus) saveStatus.textContent = mt('tool_checking');
      summary = await runOnlineDataCheckForTool(m);
      renderOnlineCheckTableForTool(summary.results);
      await dataPost('/api/checks', { checkedAt: new Date().toISOString(), summary, rows: summary.results });
    }
    editor.value = generateToolDraft(m, date, summary);
    renderIndicatorVerificationTableForTool(m.indicatorDetails || []);
    const payload = { date, text: editor.value, meta: { score: round(m.totalScore || 0,1), status: asText(m.status || '') }, path: `reports/${date}.html` };
    const res = await dataPost('/api/reports', payload);
    if(res?.ok){
      const latest = await dataGet('/api/reports?limit=30');
      renderToolReportLinks(latest?.reports || []);
    }
    if(saveStatus) saveStatus.textContent = res?.ok ? mt('tool_final_saved') : mt('tool_save_failed');
  });

  btnSave?.addEventListener('click', async()=>{
    const m = await dataGet('/api/model/current') || model;
    renderIndicatorVerificationTableForTool(m.indicatorDetails || []);
    const payload = { date, text: editor.value, meta: { score: round(m.totalScore || 0,1), status: asText(m.status || '') }, path: `reports/${date}.html` };
    const res = await dataPost('/api/reports', payload);
    if(res?.ok){
      const latest = await dataGet('/api/reports?limit=30');
      renderToolReportLinks(latest?.reports || []);
    }
    if(saveStatus) saveStatus.textContent = res?.ok ? mt('tool_saved') : mt('tool_save_failed');
  });

  btnDl?.addEventListener('click', ()=>{
    const blob = new Blob([editor.value], { type: 'text/plain;charset=utf-8' });
    const a = document.createElement('a');
    a.href = URL.createObjectURL(blob);
    a.download = `macro-daily-report-${date}.txt`;
    document.body.appendChild(a);
    a.click();
    a.remove();
    URL.revokeObjectURL(a.href);
  });
}

(async function(){
  trackMonitorPageView();
  ensureSiteFooter();
  applyMonitorI18n();
  setupMonitorLangToggle();
  const page = document.body.dataset.page;
  if(page==='login') return initLogin();
  if(page==='ops') return initOps();
  if(page==='subscribers') return initSubscribers();
  if(page==='polymarket-risk') return initPolymarketRisk();
  if(page==='data-tool') return initDataTool();
  if(page==='forms') return initForms();
  if(page==='form-detail') return initFormDetail();
})();
