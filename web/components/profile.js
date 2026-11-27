let profileDraft = null;
function profile(){
 return `<div class="profilehero">${avatar(true)}<div class="grow"><div class="eyebrow">Мой кабинет</div><h1>${esc(state.name)}</h1><p>Сотрудник команды</p>${status()}</div></div><div class="dashboard profile-layout"><div>${section('Мой профиль')}<div class="panel profile-summary"><div class="profile-detail"><small>О себе</small><p>${esc(state.bio.trim()||'Описание пока не добавлено.')}</p></div><div class="profile-detail"><small>Рабочий статус</small><div>${status()}</div></div><div class="profile-actions">${btn('Редактировать профиль','profileedit','primary')}${btn('Изменить статус','profilestatus')}</div></div>${section('Мой проект')}${projectpanel()}</div><div>${section('Личное пространство')}${metrics()}${section('Сохранённые документы','Все','go:saved')}${collections(sorted(docs.filter(d=>state.saved.includes(d.id))),false,'profile-saved')}${section('Мои тесты')}${testcard()}${section('Уведомления','Все','go:notifications')}${noticecard()}</div></div>`;
}
function profileOptions(value){
 return [['work','На работе'],['vacation','В отпуске'],['away','Недоступен']].map(([v,t])=>`<option value="${v}" ${value===v?'selected':''}>${t}</option>`).join('');
}
function profilePreview(photo,name){
 return `<span class="avatar big">${photo?`<img alt="Фото профиля" src="${esc(photo)}">`:esc(name.trim().split(/\s+/).slice(0,2).map(x=>x[0]||'').join(''))}</span>`;
}
function openProfileEdit(statusOnly=false){
 profileDraft={photo:state.photo,busy:false};
 modal(statusOnly?'Рабочий статус':'Редактировать профиль',`<form id="profileform" class="formgrid" data-mode="${statusOnly?'status':'full'}">${statusOnly?'':`<div class="row profile-photo-row"><div id="profile-photo-preview">${profilePreview(state.photo,state.name)}</div><div class="grow"><h3>Фотография профиля</h3><small>JPG, PNG, WebP · до 8 МБ</small><div class="profile-actions">${btn('Загрузить','upload')}${btn('Удалить','removephoto')}</div><small>При первом входе используется доступная фотография Telegram.</small><input id="photofile" type="file" accept="image/jpeg,image/png,image/webp" class="hidden"></div></div><label class="field">Имя и фамилия<input name="name" value="${esc(state.name)}" required maxlength="80" autocomplete="name"></label><label class="field">Должность<input name="role" maxlength="100" value="${esc(state.role||'')}"></label><label class="field">О себе <small>Будет видно коллегам в разделе «Наша команда».</small><textarea name="bio" maxlength="600">${esc(state.bio)}</textarea><span id="bio-count">${state.bio.length} / 600</span></label>`}<label class="field">Рабочий статус<select name="status">${profileOptions(state.status)}</select></label><label class="field ${state.status==='vacation'?'':'hidden'}" id="untilfield">Отпуск до (необязательно)<input type="date" name="until" value="${esc(state.until)}"></label><p class="muted">Изменения сохраняются в вашем профиле. Описание и рабочий статус видны коллегам.</p><div class="profile-actions"><button class="primary" type="submit">${I('check')} Сохранить</button>${btn('Отмена','close')}</div></form>`);
 document.querySelector('#profileform [name="'+(statusOnly?'status':'name')+'"]')?.focus();
}
function handleProfileAction(a){
 if(a==='profilephoto'){openProfileEdit();document.getElementById('photofile')?.click();return true}
 if(a==='profileedit'||a==='profilestatus'){openProfileEdit(a==='profilestatus');return true}
 const form=document.getElementById('profileform');
 if(a==='upload'){form?.querySelector('#photofile')?.click();return true}
 if(a==='removephoto'){if(form&&profileDraft&&!profileDraft.busy){profileDraft.photo='';updatePhotoPreview(form)}return true}
 return false;
}
function updatePhotoPreview(form){
 const target=form.querySelector('#profile-photo-preview');
 if(target&&profileDraft)target.innerHTML=profilePreview(profileDraft.photo,form.elements.name?.value||state.name);
}
document.addEventListener('input',e=>{
 const form=e.target.closest?.('#profileform');if(!form)return;
 if(e.target.name==='status')form.querySelector('#untilfield').classList.toggle('hidden',e.target.value!=='vacation');
 if(e.target.name==='name')updatePhotoPreview(form);
 if(e.target.name==='bio')form.querySelector('#bio-count').textContent=e.target.value.length+' / 600';
});
document.addEventListener('submit',async e=>{
 const form=e.target;if(form.id!=='profileform')return;e.preventDefault();
 if(!accessReady||mutationBusy||!profileDraft||profileDraft.busy)return;
 const f=new FormData(form),mode=form.dataset.mode,next={...state};
 if(mode==='full'){next.name=String(f.get('name')||'').trim();if(!next.name){notify('Введите имя');form.elements.name.focus();return}next.role=String(f.get('role')||'').trim();next.bio=String(f.get('bio')||'');next.photo=profileDraft.photo}
 next.status=String(f.get('status'));if(!['work','vacation','away'].includes(next.status))return;
 next.until=next.status==='vacation'?String(f.get('until')||''):'';
 const submit=form.querySelector('[type=submit]');submit.disabled=true;
 const saved=await saveState(next);submit.disabled=false;if(!saved)return;
 profileDraft=null;close();render();notify('Профиль сохранён');
});
document.addEventListener('change',e=>{
 const input=e.target;if(input.id!=='photofile')return;
 const form=input.closest('#profileform'),draft=profileDraft,file=input.files[0];input.value='';
 if(!form||!draft||!file||draft.busy)return;
 if(!['image/jpeg','image/png','image/webp'].includes(file.type)||file.size>8*1024*1024){notify('Выберите JPG, PNG или WebP до 8 МБ');return}
 const img=new Image(),url=URL.createObjectURL(file),submit=form.querySelector('[type=submit]');
 draft.busy=true;submit.disabled=true;
 const finish=()=>{URL.revokeObjectURL(url);draft.busy=false;submit.disabled=false};
 img.onload=()=>{try{if(!form.isConnected||profileDraft!==draft)return;const canvas=document.createElement('canvas'),s=Math.min(1,640/Math.max(img.naturalWidth,img.naturalHeight));canvas.width=Math.max(1,Math.round(img.naturalWidth*s));canvas.height=Math.max(1,Math.round(img.naturalHeight*s));canvas.getContext('2d').drawImage(img,0,0,canvas.width,canvas.height);draft.photo=canvas.toDataURL('image/jpeg',.92);updatePhotoPreview(form);notify('Фото готово. Нажмите «Сохранить».')}catch{notify('Не удалось обработать фото')}finally{finish()}};
 img.onerror=()=>{finish();if(form.isConnected)notify('Не удалось открыть фото')};img.src=url;
});
