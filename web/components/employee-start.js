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
const identityTones={leader:['#88501e','#c58b3e','#ffe7a4','#9d682e','#f3c77a'],expert:['#395864','#779fa9','#deedf0','#527883','#a5d5df'],veteran:['#6d4a2d','#b78555','#f0d7b1','#87613e','#dab990'],tenure:['#6d4a2d','#b78555','#f0d7b1','#87613e','#dab990']};
const gradeFor=m=>m>=36?5:m>=24?4:m>=12?3:m>=6?2:m>=3?1:0;
function tenureText(m){return m>=12?Math.floor(m/12)+' г.':m+' мес.'}
let identityFrameSerial=0;
function frameSVG(kind,grade){
 const c=identityTones[kind],id='w'+(++identityFrameSerial),n=[0,4,5,7,9,11][grade],end=[0,153,165,180,192,204][grade],start=112;
 let leaf='';for(let j=0;j<n;j++){
   const a=(start+(end-start)*(n===1?0:j/(n-1)))*Math.PI/180,r=76+grade*.6,x=100+r*Math.cos(a),y=91+r*Math.sin(a),rot=(a*180/Math.PI-180)*.65;
   const l=12+grade*1.35+(j%2)*2.8,w=3.5+grade*.38;
   leaf+=`<g transform="translate(${x.toFixed(2)} ${y.toFixed(2)}) rotate(${rot.toFixed(2)})"><path d="M0 5 Q${-w*2} ${-l*.35} -${w} -${l} Q${w} ${-l*.55} 0 5Z" fill="url(#${id}m)" stroke="${c[3]}" stroke-width=".45"/><path d="M0 5 Q${w*1.8} ${-l*.30} ${w*1.55} -${l*.72} Q${-w*.2} ${-l*.58} 0 5Z" fill="url(#${id}h)" stroke="${c[3]}" stroke-width=".35"/><path d="M0 3 L-${w*.83} -${l*.83}" stroke="${c[2]}" opacity=".55" stroke-width=".45"/></g>`;
 }
 const branch=n?`<path d="M76 164 A77 77 0 0 1 ${ (100+77*Math.cos(end*Math.PI/180)).toFixed(2)} ${(91+77*Math.sin(end*Math.PI/180)).toFixed(2)}" fill="none" stroke="url(#${id}m)" stroke-width="${1+grade*.16}"/>`+leaf:'';
 return `<svg class="award-frame identity-frame" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 200 200" aria-hidden="true"><defs><linearGradient id="${id}m" x1="0" y1="0" x2="1" y2="1"><stop stop-color="${c[0]}"/><stop offset=".25" stop-color="${c[2]}"/><stop offset=".48" stop-color="${c[1]}"/><stop offset=".69" stop-color="${c[4]}"/><stop offset="1" stop-color="${c[0]}"/></linearGradient><linearGradient id="${id}h" x1="1" y1="0" x2="0" y2="1"><stop stop-color="${c[2]}"/><stop offset=".55" stop-color="${c[4]}"/><stop offset="1" stop-color="${c[3]}"/></linearGradient></defs><circle cx="100" cy="91" r="71" fill="none" stroke="url(#${id}m)" stroke-width="${2.2+grade*.38}"/>${grade===5?`<circle cx="100" cy="91" r="76" fill="none" stroke="${c[4]}" stroke-width=".9" opacity=".75"/>`:''}${branch}<g transform="translate(200 0) scale(-1 1)">${branch}</g></svg>`;
}
function identityPortrait(kind,months,portrait,count=1){
 if(kind==='none')return `<span class="identity-plain">${portrait}</span>`;
 const grade=gradeFor(months),t=identityTones[kind],label={leader:'Лидер × '+count,expert:'Эксперт',veteran:'Ветеран',tenure:'Стаж'}[kind];
 return `<span class="award-avatar identity-avatar is-framed" style="--metal-dark:${t[0]};--metal-mid:${t[1]};--metal-light:${t[2]};--metal-end:${t[4]}">${portrait}${frameSVG(kind,grade)}${grade?`<span class="award-year"><b>${grade}</b></span>`:''}<span class="award-ribbon">${label}</span></span>`;
}
function awardAvatar(profile,portrait){
 const kind=Object.hasOwn(identityTones,profile.appearance)?profile.appearance:'none';
 const months=Number.isInteger(profile.tenureMonths)?Math.max(0,profile.tenureMonths):0;
 const count=awardEntries(profile).find(a=>a.kind==='leader')?.count||1;
 return identityPortrait(kind,months,portrait,count);
}
function profileHasFrame(p){return Object.hasOwn(identityTones,p.appearance)}
function awardRow(a) {
  const amount = a.kind==='leader'?'× '+a.count:a.kind==='veteran'&&a.years?a.years+' '+awardDeclension(a.years,'год','года','лет'):'';
  return `<button type="button" class="award-row ${a.kind}" data-award="${a.kind}">
    ${awardArt(a.kind)}<span class="award-row-copy"><strong>${awardKinds[a.kind].title}</strong><small>${esc(a.subtitle)}</small></span>
    ${amount?`<span class="award-count">${esc(amount)}</span>`:''}<span class="award-arrow" aria-hidden="true"></span></button>`;
}
function achievementShelf(profile, colleague=false) {
  const awards=awardEntries(profile), n=awards.reduce((total,a)=>total+a.count,0);
  return `<details class="award-shelf collection" data-key="${colleague?'team:achievements:'+esc(profile.id):'cabinet:achievements'}" ${colleague?'data-award-owner="'+esc(profile.id)+'"':''} ${(collectionOpen.get('cabinet:achievements')??true)?'open':''}>
    <summary><span class="award-summary-icon">${I('trophy')}</span><span class="award-summary-copy"><strong>${colleague?'Достижения сотрудника':'Мои достижения'}</strong><small>${n?n+' '+awardDeclension(n,'награда','награды','наград')+' за всё время':'Награды за результаты и стаж'}</small></span><span class="award-arrow" aria-hidden="true"></span></summary>
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
  if(trigger && accessReady){const owner=trigger.closest('[data-award-owner]')?.dataset.awardOwner;const p=owner?teamMembers().find(x=>x.id===owner):state;if(p)showAchievement(p,trigger.dataset.award);}
});

function employeeStart() {
  const project = active();
  const firstName = state.name.trim().split(/\s+/)[0] || 'Сотрудник';
  const awardsAvailable = Array.isArray(state.achievements);
  const portrait = awardAvatar(state,avatar(true));
  const editIcon = '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="m14 5 5 5M4 20l5-1 12-12-5-5L4 14z"/></svg>';

  return `<section class="employee-start">
    <header class="employee-welcome"><h1>Добро пожаловать,<br><span>${esc(firstName)}!</span></h1></header>
    <div class="award-overview ${awardsAvailable ? 'has-awards' : ''}">
      <section class="employee-card ${profileHasFrame(state) ? 'award-profile' : ''}" aria-label="Мой профиль">
        <div class="employee-card-head">
          <button class="employee-photo" data-action="profilephoto" aria-label="Изменить фото профиля">${portrait}</button>
          <div class="grow employee-identity">
            <h2>${esc(state.name)}</h2><p class="employee-role">${esc(state.role || 'Сотрудник команды')}</p>
            <button class="employee-status" data-action="profilestatus" aria-label="Изменить рабочий статус">${status()}</button>
            <button class="identity-settings-link" type="button" data-appearance-open>Оформление профиля</button>
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

let appearanceBusy=false;
function openAppearance(){
 if(!accessReady||appearanceBusy)return;
 const allowed=Array.isArray(state.availableAppearances)?state.availableAppearances:['none'];
 const options=[['leader','Лидер месяца'],['expert','Эксперт команды'],['veteran','Ветеран команды'],['tenure','Стаж'],['none','Без оформления']];
 const months=Number.isInteger(state.tenureMonths)?Math.max(0,state.tenureMonths):0;
 const count=awardEntries(state).find(a=>a.kind==='leader')?.count||1;
 let selected=allowed.includes(state.appearance)?state.appearance:'none';
 modal('Оформление профиля',`<div class="identity-settings"><p class="muted">Выберите одно из доступных оформлений. Все награды сохраняются.</p><div class="identity-choices">${options.map(([kind,title])=>`<button type="button" class="identity-choice ${selected===kind?'chosen':''}" data-appearance="${kind}" aria-pressed="${selected===kind}" ${allowed.includes(kind)?'':'disabled'}>${identityPortrait(kind,months,avatar(true),count)}<strong>${title}</strong><small>${selected===kind?'Выбрано':allowed.includes(kind)?'Доступно':'Пока закрыто'}</small></button>`).join('')}</div><button type="button" class="primary" data-appearance-save>Готово</button><p class="muted" id="appearance-message" role="status"></p></div>`);
 const settings=document.querySelector('.identity-settings');
 settings.addEventListener('click',async e=>{
  const choice=e.target.closest('[data-appearance]');if(choice&&!appearanceBusy&&allowed.includes(choice.dataset.appearance)){
   selected=choice.dataset.appearance;
   for(const node of settings.querySelectorAll('[data-appearance]')){const on=node.dataset.appearance===selected;node.classList.toggle('chosen',on);node.setAttribute('aria-pressed',String(on));node.querySelector('small').textContent=on?'Выбрано':node.disabled?'Пока закрыто':'Доступно'}
   return;
  }
  if(!e.target.closest('[data-appearance-save]')||appearanceBusy||!accessReady)return;
  appearanceBusy=true;mutationBusy=true;const save=settings.querySelector('[data-appearance-save]');save.disabled=true;
  try{const updated=await employeeRequest('/api/me/appearance','PUT',{appearance:selected});if(!accessReady)return;state=updated;close();render();notify('Оформление сохранено')}
  catch(err){if(err.status===401)reportAccessError(err);else if(settings.isConnected)settings.querySelector('#appearance-message').textContent=err.message}
  finally{appearanceBusy=false;mutationBusy=false;save.disabled=false}
 });
}
document.addEventListener('click',e=>{if(e.target.closest('[data-appearance-open]'))openAppearance()});
