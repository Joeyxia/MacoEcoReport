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
  }
};

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
  const data = await api.get('/monitor-api/ops/overview?days=30');
  if(!data) return;
  q('kpi-visits').textContent = data.totals.pageVisits;
  q('kpi-input').textContent = data.totals.inputTokens;
  q('kpi-output').textContent = data.totals.outputTokens;
  q('kpi-total').textContent = data.totals.totalTokens;
  q('top-pages').innerHTML = tableFromRows(data.visitsByPath || []);

  const vctx = q('visits-chart');
  if(vctx && window.Chart){
    new Chart(vctx,{type:'line',data:{labels:(data.visitsDaily||[]).map(x=>x.day),datasets:[{label:'Visits',data:(data.visitsDaily||[]).map(x=>x.visits),borderColor:'#0d7e6b',fill:false}]},options:{responsive:true,maintainAspectRatio:false}});
  }
  const tctx = q('tokens-chart');
  if(tctx && window.Chart){
    new Chart(tctx,{type:'bar',data:{labels:(data.tokensDaily||[]).map(x=>x.day),datasets:[{label:'Input',data:(data.tokensDaily||[]).map(x=>x.input_tokens),backgroundColor:'#6bb8a8'},{label:'Output',data:(data.tokensDaily||[]).map(x=>x.output_tokens),backgroundColor:'#1f8b73'}]},options:{responsive:true,maintainAspectRatio:false}});
  }
}

async function initSubscribers(){
  if(!(await requireAuth())) return;
  setupLogout();
  const data = await api.get('/monitor-api/biz/subscribers');
  if(!data) return;
  q('sub-count').textContent = `Active subscribers: ${data.count}`;
  q('sub-table').innerHTML = tableFromRows(data.subscribers || []);
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
