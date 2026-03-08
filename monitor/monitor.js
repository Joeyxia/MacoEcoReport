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

(async function(){
  trackMonitorPageView();
  const page = document.body.dataset.page;
  if(page==='login') return initLogin();
  if(page==='ops') return initOps();
  if(page==='subscribers') return initSubscribers();
  if(page==='forms') return initForms();
  if(page==='form-detail') return initFormDetail();
})();
