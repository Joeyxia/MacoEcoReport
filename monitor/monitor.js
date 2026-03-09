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
  }
};

const OPS_REFRESH_MS = 60 * 1000;
const OPS_WINDOW_MINUTES = 1440;
let visitsChart = null;
let tokensChart = null;

function q(id){ return document.getElementById(id); }

function trackMonitorPageView(){
  const payload = { path: `${location.pathname}${location.search || ''}`, referrer: document.referrer || '' };
  fetch('/monitor-api/track/page', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload) }).catch(()=>{});
}

function tableFromRows(rows){
  if(!rows || !rows.length) return '<p class="muted">No data</p>';
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
    root.innerHTML = '<p class="table-empty">暂无数据。</p>';
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

function renderToolReportLinks(reports){
  const root = q('report-links');
  if(!root) return;
  const list = Array.isArray(reports) ? reports : [];
  if(!list.length){
    root.innerHTML = '<p class="table-empty">暂无已保存报告。</p>';
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
          <span class="badge">综合评分: ${score}</span>
          <span class="badge">信号: ${status}</span>
        </div>
      </div>
      <a class="report-open" href="https://nexo.hk/daily-report.html?date=${encodeURIComponent(date)}" target="_blank" rel="noopener noreferrer">打开</a>
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
    await api.post('/monitor-api/auth/logout',{});
    location.href='./index.html';
  });
}

async function initLogin(){
  const me = await api.get('/monitor-api/auth/me');
  if(me?.ok){ location.href='./dashboard.html'; return; }
  const btn=q('google-login');
  if(btn){ btn.addEventListener('click', ()=>{ location.href='/monitor-api/auth/google/start'; }); }
  const p=new URLSearchParams(location.search);
  if(p.get('auth')==='failed') q('login-status').textContent='Google login failed, please retry.';
  if(p.get('auth')==='forbidden') q('login-status').textContent='Your Google account is not authorized.';
  const probe = await api.get('/monitor-api/health');
  if(probe?.oauthConfigured === false) q('login-status').textContent = 'Google OAuth is not configured on server yet.';
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
  if(q('ops-window-label')) q('ops-window-label').textContent = `${data.minutes || OPS_WINDOW_MINUTES}m`;
  q('top-pages').innerHTML = tableFromRows(data.visitsByPath || []);

  const vctx = q('visits-chart');
  if(vctx && window.Chart){
    const visitsSeries = data.visitsMinute || [];
    const visitsLabels = visitsSeries.map(x => (x.minute || '').slice(11, 16) || (x.minute || ''));
    const visitsConfig = {type:'line',data:{labels:visitsLabels,datasets:[{label:'Visits',data:visitsSeries.map(x=>x.visits),borderColor:'#0d7e6b',fill:false}]},options:{responsive:true,maintainAspectRatio:false}};
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
    const tokensConfig = {type:'bar',data:{labels:tokenLabels,datasets:[{label:'Input',data:tokenSeries.map(x=>x.input_tokens),backgroundColor:'#6bb8a8'},{label:'Output',data:tokenSeries.map(x=>x.output_tokens),backgroundColor:'#1f8b73'}]},options:{responsive:true,maintainAspectRatio:false}};
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
    q('sub-count').textContent = `Active subscribers: ${data.count}`;
    const rows = data.subscribers || [];
    const header = `
      <tr>
        <th>订阅时间</th>
        <th>订阅邮箱地址</th>
        <th>订阅成功邮件是否发送</th>
        <th>当日日报是否发送</th>
        <th>删除邮箱地址</th>
      </tr>`;
    const body = rows.map((r)=>{
      const createdAt = String(r.created_at || '').replace('T',' ').replace('Z','');
      const welcome = r.welcome_email_sent ? '是' : '否';
      const daily = r.daily_report_sent_today ? '是' : '否';
      const email = String(r.email || '');
      return `<tr>
        <td>${createdAt}</td>
        <td>${email}</td>
        <td>${welcome}</td>
        <td>${daily}</td>
        <td><button class="btn ghost sub-delete-btn" data-email="${email}">删除</button></td>
      </tr>`;
    }).join('');
    q('sub-table').innerHTML = `<div class="table"><table><thead>${header}</thead><tbody>${body}</tbody></table></div>`;
    q('sub-table').querySelectorAll('.sub-delete-btn').forEach((btn)=>{
      btn.addEventListener('click', async()=>{
        const email = btn.getAttribute('data-email') || '';
        if(!email) return;
        const ok = window.confirm(`确认删除订阅邮箱 ${email} 吗？`);
        if(!ok) return;
        btn.disabled = true;
        const res = await api.del(`/monitor-api/biz/subscribers/${encodeURIComponent(email)}`);
        btn.disabled = false;
        if(!res?.ok){
          window.alert('删除失败，请重试。');
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

async function initFormDetail(){
  if(!(await requireAuth())) return;
  setupLogout();
  const name = new URLSearchParams(location.search).get('name') || 'dimensions';
  q('form-title').textContent = `Form: ${name}`;
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
  const model = await api.get('/api/model/current?view=core') || await api.get('/api/model/current') || {};
  const existing = await api.get(`/api/reports/${encodeURIComponent(date)}`);
  const reports = await api.get('/api/reports?limit=30');
  editor.value = asText(existing?.text) || generateToolDraft(model, date, null);
  renderOnlineCheckTableForTool(model.onlineCheck || []);
  renderToolReportLinks(reports?.reports || []);

  btnGen?.addEventListener('click', async()=>{
    const m = await api.get('/api/model/current?view=core') || model;
    editor.value = generateToolDraft(m, date, null);
    if(saveStatus) saveStatus.textContent = '草稿已重新生成。';
  });

  btnFinal?.addEventListener('click', async()=>{
    const m = await api.get('/api/model/current?view=core') || model;
    let summary = null;
    if(runCheck?.checked){
      if(saveStatus) saveStatus.textContent = '正在执行在线数据校验...';
      summary = await runOnlineDataCheckForTool(m);
      renderOnlineCheckTableForTool(summary.results);
      await api.post('/api/checks', { checkedAt: new Date().toISOString(), summary, rows: summary.results });
    }
    editor.value = generateToolDraft(m, date, summary);
    const payload = { date, text: editor.value, meta: { score: round(m.totalScore || 0,1), status: asText(m.status || '') }, path: `reports/${date}.html` };
    const res = await api.post('/api/reports', payload);
    if(res?.ok){
      const latest = await api.get('/api/reports?limit=30');
      renderToolReportLinks(latest?.reports || []);
    }
    if(saveStatus) saveStatus.textContent = res?.ok ? '最终报告已生成并保存。' : '保存失败，请重试。';
  });

  btnSave?.addEventListener('click', async()=>{
    const m = await api.get('/api/model/current?view=core') || model;
    const payload = { date, text: editor.value, meta: { score: round(m.totalScore || 0,1), status: asText(m.status || '') }, path: `reports/${date}.html` };
    const res = await api.post('/api/reports', payload);
    if(res?.ok){
      const latest = await api.get('/api/reports?limit=30');
      renderToolReportLinks(latest?.reports || []);
    }
    if(saveStatus) saveStatus.textContent = res?.ok ? '已保存。' : '保存失败，请重试。';
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
  const page = document.body.dataset.page;
  if(page==='login') return initLogin();
  if(page==='ops') return initOps();
  if(page==='subscribers') return initSubscribers();
  if(page==='data-tool') return initDataTool();
  if(page==='forms') return initForms();
  if(page==='form-detail') return initFormDetail();
})();
