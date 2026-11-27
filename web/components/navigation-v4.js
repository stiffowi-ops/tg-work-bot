function renderNavigationV4(){
 const profilePages=['home','profile','notifications','team'];
 const isProfile=profilePages.includes(page);
 const current=isProfile?'profile':(['search','library','cases','videos','faq','links','saved','leaders','calendar','menu'].includes(page)?'library':page);
 document.getElementById('sidebar').innerHTML='';
 document.getElementById('top').innerHTML=`<div class="v4-top-inner"><div class="v4-brand">${brand()}</div><div class="v4-top-actions">${page==='notifications'?`<button class="v4-back" data-go="profile">${I('arrow')} Профиль</button>`:''}<button class="v4-bell" data-go="notifications" aria-label="Уведомления${unread()?': '+unread():''}">${I('bell')}${unread()?`<span class="v4-unread">${unread()}</span>`:''}</button></div></div>`;
 document.getElementById('mobile').innerHTML=`<div class="v4-nav-inner">${[['profile','user','Профиль'],['projects','project','Проекты'],['library','book','Инфосистема'],['tests','test','Обучение']].map(([p,i,t])=>`<button class="${current===p?'active':''}" data-go="${p}" ${current===p?'aria-current="page"':''}><span class="v4-nav-icon">${I(i)}</span><span>${t}</span></button>`).join('')}</div>`;
 const personal=page==='home'||page==='profile';document.getElementById('content').classList.toggle('employee-view',personal);document.body.classList.toggle('routing-view',personal);
}
function infoSystem(){return `<div class="v4-library-head"><h1>Инфосистема</h1><p class="muted">Рабочие материалы и знания команды</p></div><div class="v4-library-tools">${btn('Поиск по материалам','go:search','secondary')}${btn('Сохранённые материалы','go:saved','secondary')}</div>${section('Разделы')}${tools()}${section('Материалы')}${collections(sorted(docs),true,'info')}`}
