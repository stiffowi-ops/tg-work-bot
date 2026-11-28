// The authenticated profile supplies award counts, dates and the selected appearance.
const awardKinds = {
  leader: {title: 'Лидер месяца', icon: 'trophy'},
  expert: {title: 'Эксперт команды', icon: 'users'},
  veteran: {title: 'Ветеран команды', icon: 'calendar'}
};
function awardDeclension(n, one, few, many) {
  const last = n % 10, lastTwo = n % 100;
  if (lastTwo >= 11 && lastTwo <= 14) return many;
  return last === 1 ? one : last >= 2 && last <= 4 ? few : many;
}
function awardEntries(profile) {
  return Array.isArray(profile.achievements) ? profile.achievements.filter(a =>
    a && Object.hasOwn(awardKinds, a.kind) && Number.isInteger(a.count) && a.count > 0) : [];
}
function awardArt(kind) {
  return '<img class="award-medal" alt="" src="' + {"leader":"/assets/award-leader.svg","expert":"/assets/award-expert.svg","veteran":"/assets/award-veteran.svg"}[kind] + '">';
}
function awardAvatar(profile, portrait) {
  const awards = awardEntries(profile);
  const leader = awards.find(a => a.kind === 'leader');
  const years = awards.find(a => a.kind === 'veteran')?.years;
  const framed = !!leader?.current;
  return `<span class="award-avatar ${framed?'is-framed':''}">
    ${portrait}
    ${framed?'<img class="award-frame" alt="" src="/assets/award-frame.svg">':''}
    ${years > 0?`<span class="award-year" aria-label="Стаж: ${esc(years)} ${awardDeclension(years,'год','года','лет')}"><b>${esc(years)}</b></span>`:''}
    ${leader?`<span class="award-ribbon">Лидер × ${leader.count}</span>`:''}
  </span>`;
}
function awardChips(profile) {
  return awardEntries(profile).filter(a=>a.kind==='expert'||a.kind==='veteran').map(a=>
    `<span class="award-chip ${a.kind}">${a.kind==='veteran'?'Ветеран':'Эксперт команды'}${a.kind==='veteran'&&a.years?' · '+esc(a.years)+' '+awardDeclension(a.years,'год','года','лет'):''}</span>`).join('');
}
function awardRow(a) {
  const amount = a.kind==='leader'?'× '+a.count:a.kind==='veteran'&&a.years?a.years+' '+awardDeclension(a.years,'год','года','лет'):'';
  return `<button type="button" class="award-row ${a.kind}" data-award="${a.kind}">
    ${awardArt(a.kind)}<span class="award-row-copy"><strong>${awardKinds[a.kind].title}</strong><small>${esc(a.subtitle)}</small></span>
    ${amount?`<span class="award-count">${esc(amount)}</span>`:''}<span class="award-arrow" aria-hidden="true"></span></button>`;
}
function achievementShelf(profile) {
  const awards=awardEntries(profile), n=awards.reduce((total,a)=>total+a.count,0);
  return `<details class="award-shelf collection" data-key="cabinet:achievements" ${(collectionOpen.get('cabinet:achievements')??true)?'open':''}>
    <summary><span class="award-summary-icon">${I('trophy')}</span><span class="award-summary-copy"><strong>Мои достижения</strong><small>${n?n+' '+awardDeclension(n,'награда','награды','наград')+' за всё время':'Награды за результаты и стаж'}</small></span><span class="award-arrow" aria-hidden="true"></span></summary>
    <div class="award-list">${awards.length?awards.map(awardRow).join(''):'<p class="award-empty">Здесь появятся ваши первые достижения.</p>'}</div>
  </details>`;
}
function showAchievement(profile, kind) {
  const a=awardEntries(profile).find(entry=>entry.kind===kind);
  if (!a) return;
  const dialog=document.createElement('dialog');
  dialog.className='award-dialog';
  dialog.setAttribute('aria-labelledby','award-detail-title');
  const history=Array.isArray(a.history)?a.history:[];
  dialog.innerHTML=`<div class="award-detail-art ${kind}"><span class="award-detail-label">Достижение</span><button type="button" class="award-close" aria-label="Закрыть достижение">${I('close')}</button>${awardArt(kind)}</div>
  <div class="award-detail-body"><h2 id="award-detail-title">${awardKinds[kind].title}</h2>
  <p class="award-detail-sub">${esc(a.subtitle)}</p><p class="award-detail-description">${esc(a.description)}</p>
  ${a.appearance?`<div class="award-unlocked"><span>${I('user')}</span><div><small>Открыто оформление профиля</small><strong>${esc(a.appearance)}</strong></div></div>`:''}
  ${history.length?`<section class="award-history"><h3>История побед</h3><ul>${history.map(t=>`<li>${esc(t)}</li>`).join('')}</ul></section>`:''}</div>`;
  document.body.append(dialog);
  dialog.querySelector('.award-close').onclick=()=>dialog.close();
  dialog.addEventListener('click',e=>{if(e.target===dialog){const r=dialog.getBoundingClientRect();if(e.clientX<r.left||e.clientX>r.right||e.clientY<r.top||e.clientY>r.bottom)dialog.close();}});
  dialog.addEventListener('close',()=>dialog.remove(),{once:true});
  dialog.showModal();
}
document.addEventListener('click',event=>{
  const trigger=event.target.closest('[data-award]');
  if(trigger && accessReady) showAchievement(state,trigger.dataset.award);
});

function employeeStart() {
  const project = active();
  const firstName = state.name.trim().split(/\s+/)[0] || 'Сотрудник';
  const awardsAvailable = Array.isArray(state.achievements);
  const portrait = awardEntries(state).length ? awardAvatar(state, avatar(true)) : avatar(true);
  const editIcon = '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="m14 5 5 5M4 20l5-1 12-12-5-5L4 14z"/></svg>';

  return `<section class="employee-start">
    <header class="employee-welcome"><h1>Добро пожаловать,<br><span>${esc(firstName)}!</span></h1></header>
    <div class="award-overview ${awardsAvailable ? 'has-awards' : ''}">
      <section class="employee-card ${awardEntries(state).length ? 'award-profile' : ''}" aria-label="Мой профиль">
        <div class="employee-card-head">
          <button class="employee-photo" data-action="profilephoto" aria-label="Изменить фото профиля">${portrait}</button>
          <div class="grow employee-identity">
            <h2>${esc(state.name)}</h2><p class="employee-role">${esc(state.role || 'Сотрудник команды')}</p>
            <button class="employee-status" data-action="profilestatus" aria-label="Изменить рабочий статус">${status()}</button>
            ${awardsAvailable ? `<div class="award-chips">${awardChips(state)}</div>` : ''}
          </div>
          <button class="employee-edit" data-action="profileedit" aria-label="Редактировать профиль">${editIcon}</button>
        </div>
        ${state.bio?.trim() ? `<p class="employee-bio">${esc(state.bio)}</p>` : ''}
      </section>
      ${awardsAvailable ? achievementShelf(state) : ''}
    </div>
    <div class="employee-shortcuts">
      <button class="employee-shortcut employee-shortcut-project ${project ? projectArtwork(project) : 'calendar'}" data-action="${project ? 'projectdocs:' + esc(project.id) : 'choose'}">
        <span class="employee-shortcut-copy"><small>Мой проект</small><strong>${esc(project?.name || 'Выбрать проект')}</strong><span>Рабочие материалы</span></span>
      </button>
      <button class="employee-shortcut employee-shortcut-team" data-go="team">
        <span class="employee-shortcut-copy"><small>Рабочее пространство</small><strong>Наша команда</strong><span>Коллеги и статусы</span></span>
      </button>
    </div>
    <div id="cabinet-work" class="cabinet-work">${cabinetWork()}</div>
  </section>`;
}
let identityBusy=false;
async function identityAdminOpen(){
 if(!accessReady||!cmsAdmin||identityBusy)return;
 identityBusy=true;
 try{
 const people=await employeeRequest('/api/admin/identity');
 if(!accessReady||page!=='admin')return;
 modal('Достижения сотрудников',people.length?'<form id="identity-admin-form" class="award-admin-form"><label>Сотрудник<select name="id">'+people.map(p=>'<option value="'+esc(p.id)+'">'+esc(p.name)+' · '+esc(p.id)+'</option>').join('')+'</select></label><p id="identity-summary"></p><label>Месяц начала работы<input name="startMonth" type="month"></label><button type="submit" class="secondary" value="start">Сохранить стаж</button><button type="submit" class="primary" value="leader">Лидер месяца</button><button type="submit" class="secondary" value="revoke-leader">Снять лидера</button><button type="submit" class="primary" value="expert">Эксперт</button><button type="submit" class="secondary" value="revoke-expert">Снять эксперта</button><p class="muted">Лидер — один на текущий месяц. Эксперт — не чаще раза в день.</p><p id="identity-message" role="status"></p></form>':'<p>Пока нет зарегистрированных сотрудников.</p>');
 const form=document.getElementById('identity-admin-form');if(!form)return;
 const show=()=>{const p=people.find(p=>p.id===form.elements.id.value);form.elements.startMonth.value=p?.identity?.startMonth||'';form.querySelector('#identity-summary').textContent=awardEntries(p||{}).map(a=>awardKinds[a.kind].title+' × '+a.count).join(' · ')||'Нет наград'};
 form.elements.id.addEventListener('change',show);show();
 form.addEventListener('submit',async e=>{e.preventDefault();if(identityBusy||!accessReady||!cmsAdmin)return;const action=e.submitter?.value;if(!action)return;if(action!=='start'&&!confirm('Подтвердить изменение награды?'))return;
 const data={id:form.elements.id.value,startMonth:form.elements.startMonth.value,action};identityBusy=true;form.querySelectorAll('button,input,select').forEach(x=>x.disabled=true);
 try{await employeeRequest('/api/admin/identity','PUT',data);const latest=await employeeRequest('/api/admin/identity');people.splice(0,people.length,...latest);state=await employeeRequest('/api/me');if(form.isConnected){show();form.querySelector('#identity-message').textContent='Сохранено.'}}
 catch(err){if(err.status===401||err.status===403)reportAccessError(err);else if(form.isConnected)form.querySelector('#identity-message').textContent=err.message}
 finally{identityBusy=false;form.querySelectorAll('button,input,select').forEach(x=>x.disabled=false)}});
 }catch(err){reportAccessError(err)}finally{identityBusy=false}
}
document.addEventListener('click',e=>{if(e.target.closest('[data-identity-admin]'))void identityAdminOpen()});
