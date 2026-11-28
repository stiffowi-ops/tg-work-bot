
let cmsBusy=false,cmsFormDirty=false,cmsDeleteID='';
let cmsMediaRenewedAt=0,cmsMediaRenewing=false;
let cmsAdmin=false,cmsProject=null,cmsDraft=null,cmsLoading=false,cmsError='',cmsTicket=0,cmsKind='document',cmsEditID='';
const cmsKinds={text:'Текст',document:'Документ',video:'Видео',image:'Изображение',link:'Ссылка',section:'Подраздел',faq:'FAQ'};
const cmsCovers={presentation:'Презентация',script:'Скрипт работы',instruction:'Инструкция',analytics:'Аналитика',case:'Кейс',general:'Общие материалы'};
const cmsMedia=b=>'/api/content/'+encodeURIComponent(cmsProject.ID||cmsProject.id)+'/file/'+encodeURIComponent(b.id);
async function cmsRequest(path,method='GET',body){const response=await fetch(path,{method,headers:{Authorization:'tma '+(telegram()?.initData||''),...(body instanceof FormData?{}:{'Content-Type':'application/json'})},body:body===undefined?undefined:body instanceof FormData?body:JSON.stringify(body),cache:'no-store',credentials:'same-origin'});const data=await response.json();if(!response.ok)throw Object.assign(new Error(data.error||'Ошибка сервера'),{status:response.status});return data}
async function cmsInit(){const seq=++cabinetIndexSeq;try{const data=await cmsRequest('/api/content');if(seq!==cabinetIndexSeq||!accessReady)return;cabinetCatalog=Array.isArray(data.materials)?data.materials:[];cabinetError='';cmsAdmin=data.admin===true;for(const p of data.projects){const old=projectById(p.id);if(old){old.name=p.title;old.desc=p.description}}if(accessReady){renderNavigationV4();cabinetRefreshUI()}}catch(e){if(seq!==cabinetIndexSeq||!accessReady)return;cabinetCatalog=null;cabinetError=e.message;if(e.status===401||e.status===403)reportAccessError(e);else cabinetRefreshUI()}}
async function cmsLoad(id,edit=false){const ticket=++cmsTicket;cmsLoading=true;cmsError='';cmsProject=null;cmsDraft=null;cmsEditID='';cmsDeleteID='';cmsFormDirty=false;render();try{const data=await cmsRequest('/api/content/'+encodeURIComponent(id)+(edit?'?edit=1':''));if(ticket!==cmsTicket||!accessReady||page!==(edit?'admin':'project-detail'))return;cmsProject=data;cmsMediaRenewedAt=Date.now();if(edit)cmsDraft=structuredClone(data)}catch(e){if(ticket===cmsTicket){cmsError=e.message;if(e.status===401||e.status===403)reportAccessError(e)}}finally{if(ticket===cmsTicket){cmsLoading=false;if(accessReady&&page===(edit?'admin':'project-detail')){render();if(!edit)cabinetJumpTo()}}}}
function cmsPlaceholder(){return '<div class="panel">'+esc(cmsLoading?'Загружаем проект…':cmsError||'Выберите проект')+'</div>'}
function renderCMSBlock(b){const m={project:cmsProject.id,id:b.id};return '<div class="cms-material-item cms-item-'+esc(b.kind)+'" data-material-ref="'+esc(cabinetRef(m))+'">'+renderCMSBlockBody(b)+cabinetBookmark(m)+'</div>'}
function renderCMSBlockBody(b){const title=esc(b.title),txt=esc(b.text||''),media=esc(cmsMedia(b));switch(b.kind){
case 'text':case 'section':return '<section class="feed-panel"><h2>'+title+'</h2><p class="feed-text">'+txt+'</p></section>';
case 'document':return '<a class="material-cover" href="'+media+'"><span class="cover-art cover-'+cmsCoverKey(b.cover)+'"></span><span class="cover-label">'+title+' '+I('arrow')+'</span></a>';
case 'video':return '<section class="feed-panel"><h2>'+title+'</h2><div class="routing-player"><video playsinline preload="none" controls data-video-src="'+media+'"></video><button class="player-launch" type="button" aria-label="Воспроизвести видео"><img src="/assets/play-selected.webp" alt=""></button></div><p class="player-status" role="status" hidden></p>'+(txt?'<p class="feed-text">'+txt+'</p>':'')+'</section>';
case 'image':return '<section class="feed-panel"><h2>'+title+'</h2><img class="feed-image" src="'+media+'" alt="'+title+'" loading="lazy">'+(txt?'<p>'+txt+'</p>':'')+'</section>';
case 'link':return '<a class="feed-link" href="'+esc(b.url)+'" target="_blank" rel="noopener noreferrer">'+I('link')+'<strong>'+title+'</strong>'+I('arrow')+'</a>';
case 'faq':return '<details class="feed-panel feed-faq"><summary>'+title+'</summary><p class="feed-text">'+txt+'</p></details>';
default:return ''}}
function cmsFeed(blocks){let out='',group=[],kind='';const flush=()=>{if(group.length){out+='<section class="feed-panel"><h2>'+(kind==='document'?'Материалы проекта':'Полезные ссылки')+'</h2><div class="'+(kind==='document'?'material-grid':'link-grid')+'">'+group.map(renderCMSBlock).join('')+'</div></section>';group=[];kind=''}};for(const b of blocks){if(b.kind==='document'||b.kind==='link'){if(kind!==b.kind)flush();kind=b.kind;group.push(b)}else{flush();out+=renderCMSBlock(b)}}flush();return out}
function cmsDetail(){const p=projectById(openedProject);if(!p)return projectPage();const current=cmsProject?.id===p.id?cmsProject:null;return '<section class="projects-page project-detail-page"><button class="team-back" data-go="projects">‹ Все проекты</button><header class="projects-heading feed-heading"><div><h1>'+esc(current?.title||p.name)+'</h1><p>'+esc(current?.description||p.desc)+'</p></div><button class="project-pin '+(state.project===p.id?'on':'')+'" data-action="pin:'+esc(p.id)+'" aria-label="Закрепить проект" aria-pressed="'+(state.project===p.id)+'">'+I('saved')+'</button></header>'+(current?cmsFeed(current.blocks||[])||'<div class="panel">Материалы ещё не добавлены.</div>':cmsPlaceholder())+'</section>'}
const cmsKindArt={
 text:['script','book'],document:['presentation','file'],video:['video','video'],
 image:['case','camera'],link:['general','link'],section:['analytics','folder'],faq:['instruction','help']
};
const cmsCoverKey=cover=>Object.hasOwn(cmsCovers,cover)?cover:'general';

function cmsArt(cover,icon=''){
 return `<span class="cms-preview cover-art cover-${cover}" aria-hidden="true">${icon?I(icon):''}</span>`;
}
function cmsProjectPicker(){
 const selected=cmsDraft?.id||openedProject;
 return `<fieldset class="cms-picker" id="cms-project"><legend>Проект</legend><div class="cms-choices cms-projects">${projects.map(p=>
  `<button type="button" class="cms-option" data-action="cmsproject:${esc(p.id)}" aria-pressed="${selected===p.id}">${cmsArt(projectArtwork(p))}<span>${esc(p.name)}</span></button>`
 ).join('')}</div></fieldset>`;
}
function cmsKindPicker(kind){
 return `<fieldset class="cms-picker"><legend>Тип материала</legend><div class="cms-choices">${Object.entries(cmsKinds).map(([k,title])=>
  `<button type="button" class="cms-option" data-action="cmskind:${k}" aria-pressed="${kind===k}">${cmsArt(...cmsKindArt[k])}<span>${title}</span></button>`
 ).join('')}</div></fieldset>`;
}
function cmsCoverPicker(selected){
 return `<fieldset class="cms-picker"><legend>Обложка</legend><div class="cms-choices">${Object.entries(cmsCovers).map(([k,title])=>
  `<label class="cms-option cms-cover-choice"><input type="radio" name="cover" value="${k}" ${k===selected?'checked':''}>${cmsArt(k)}<span>${title}</span></label>`
 ).join('')}</div></fieldset>`;
}
function cmsAdminPage(){
 if(!cmsAdmin)return '<p>Доступ закрыт.</p>';
 const d=cmsDraft;
 return `<section class="projects-page"><header class="projects-heading"><h1>Администрирование</h1></header>
 <section class="feed-panel"><h2>Рабочая группа</h2><p class="muted">Проверка токена, ID группы, прав бота и вашего членства.</p><button class="secondary" data-action="cmsdiagnostic">Проверить подключение к Telegram</button><div id="cms-diagnostic" role="status"></div></section>
 ${cmsProjectPicker()}
 ${d?`<div class="cms-grid"><section class="feed-panel"><div class="cms-section-heading"><h2>Редактирование проекта</h2><a href="#cms-order-heading">К материалам ↓</a></div>
 <label>Название<input id="cms-title" value="${esc(d.title)}" maxlength="120"></label><label>Описание<textarea id="cms-desc" rows="3">${esc(d.description)}</textarea></label>
 <div id="cms-editor">${cmsEditor()}</div></section><section class="feed-panel"><h2 id="cms-order-heading" tabindex="-1">Материалы проекта</h2>
 <div id="cms-order">${cmsOrder()}</div><button class="primary" data-action="cmssave">Сохранить изменения</button></section></div>`:cmsPlaceholder()}</section>`;
}
function cmsEditor(){
 const b=cmsDraft?.blocks.find(x=>x.id===cmsEditID),kind=b?.kind||cmsKind;
 const accept={document:'.pdf,.docx,.pptx,.xlsx,.txt',video:'.mp4,.webm',image:'.png,.jpg,.jpeg,.webp'};
 return `<form id="cms-form"><h3>${b?'Редактировать блок':'Добавить контент'}</h3>${b?'':cmsKindPicker(kind)}
 <label>Название<input name="title" required maxlength="140" value="${esc(b?.title||'')}"></label>
 <label>${kind==='faq'?'Ответ':'Описание / текст'}<textarea name="text" rows="4">${esc(b?.text||'')}</textarea></label>
 ${kind==='link'?`<label>Ссылка HTTPS<input name="url" type="url" required value="${esc(b?.url||'')}"></label>`:''}
 ${accept[kind]&&!b?`<label class="cms-upload">Выбрать файл<input name="file" type="file" required accept="${accept[kind]}"></label>`:''}
 ${kind==='video'?'<p class="muted">Для совместимости с телефонами: MP4 (H.264, AAC при наличии звука). WebM зависит от поддержки устройства.</p>':''}
 ${kind==='document'?cmsCoverPicker(b?cmsCoverKey(b.cover):'presentation'):''}
 <label class="cms-check"><input name="published" type="checkbox" ${!b||b.published?'checked':''}> Опубликовать</label>
 <button class="primary" type="submit">${b?'Применить изменения':'Добавить материал'}</button>${b?'<button type="button" class="secondary" data-action="cmscancel">Отмена</button>':''}
 <p id="cms-message" role="status"></p></form>`;
}
function cmsBlockArt(b){
 if(b.kind==='image')return `<img class="cms-preview" src="${esc(cmsMedia(b))}" alt="" loading="lazy" draggable="false">`;
 if(b.kind==='document')return cmsArt(cmsCoverKey(b.cover));
 return cmsArt(...cmsKindArt[b.kind]);
}
function cmsOrder(){
 if(!cmsDraft.blocks.length)return '<p class="muted">В проекте пока нет материалов.</p>';
 return cmsDraft.blocks.map((b,i)=>{
  const id=esc(b.id),title=esc(b.title);
  return `<div class="cms-order-row" draggable="true" data-block="${id}">
   <button type="button" class="cms-material-edit" data-action="cmsedit:${id}" aria-label="Редактировать «${title}»">${cmsBlockArt(b)}<span class="grow"><strong>${title}</strong><small>${cmsKinds[b.kind]}${b.published?'':' · Черновик'}</small></span></button>
   ${cmsDeleteID===b.id?`<div class="cms-remove-prompt" role="group" aria-label="Подтверждение удаления"><strong>Удалить «${title}»?</strong><span>${['document','video','image'].includes(b.kind)?'Прикреплённый файл тоже будет удалён.':''}</span>
   <div><button type="button" class="cms-remove" data-action="cmsconfirmdelete:${id}">Да, удалить</button><button type="button" data-action="cmskeep:${id}">Отмена</button></div><p class="cms-remove-error" role="alert"></p></div>`:
   `<div class="cms-row-actions"><button type="button" data-action="cmsup:${i}" aria-label="Выше" ${i===0?'disabled':''}>↑</button><button type="button" data-action="cmsdown:${i}" aria-label="Ниже" ${i===cmsDraft.blocks.length-1?'disabled':''}>↓</button><button type="button" class="cms-remove" data-action="cmsdelete:${id}" aria-label="Удалить «${title}»">Удалить</button></div>`}
  </div>`;
 }).join('');
}
function cmsRefreshOrder(){
 document.getElementById('cms-order').innerHTML=cmsOrder();
}
function cmsSync(){if(!cmsDraft)return;cmsDraft.title=document.getElementById('cms-title')?.value??cmsDraft.title;cmsDraft.description=document.getElementById('cms-desc')?.value??cmsDraft.description}
function cmsReorder(a,b){if(cmsBusy||!cmsDraft||a<0||b<0||a>=cmsDraft.blocks.length||b>=cmsDraft.blocks.length)return;const [x]=cmsDraft.blocks.splice(a,1);cmsDraft.blocks.splice(b,0,x);cmsRefreshOrder()}
function cmsHasUnsaved(){if(!cmsDraft)return false;cmsSync();return cmsFormDirty||JSON.stringify(cmsDraft)!==JSON.stringify(cmsProject)}
function cmsCanLeave(){if(cmsBusy){notify('Дождитесь завершения сохранения.');return false}return !cmsHasUnsaved()||confirm('Есть несохранённые изменения. Покинуть редактор?')}
function cmsLock(busy){cmsBusy=busy;const panel=document.querySelector('#content .cms-grid');if(panel){panel.inert=busy;panel.setAttribute('aria-busy',String(busy))}const select=document.getElementById('cms-project');if(select)select.disabled=busy}
function cmsUpdateSaved(result){cmsDeleteID='';cmsDraft=structuredClone(result);cmsProject=result;const p=projectById(result.id);if(p){p.name=result.title;p.desc=result.description}}
async function cmsSave(){if(cmsBusy||!cmsDraft)return;if(cmsFormDirty){notify('Сначала примените изменения блока или добавьте материал.');return}cmsSync();const snapshot=structuredClone(cmsDraft),ticket=cmsTicket;cmsLock(true);try{const result=await cmsRequest('/api/content/'+snapshot.id,'PUT',snapshot);if(ticket!==cmsTicket||!accessReady||page!=='admin')return;cmsUpdateSaved(result);cmsEditID='';render();notify('Изменения сохранены')}catch(e){if(ticket===cmsTicket)notify(e.message)}finally{cmsLock(false)}}
async function cmsDeleteBlock(id){
 if(cmsBusy||id!==cmsDeleteID||!cmsDraft.blocks.some(b=>b.id===id))return;
 const ticket=cmsTicket,project=cmsProject.id;
 const current=()=>ticket===cmsTicket&&accessReady&&page==='admin';
 cmsLock(true);
 try{
  const result=await cmsRequest('/api/content/'+encodeURIComponent(project)+'/blocks/'+encodeURIComponent(id),'DELETE',{revision:cmsProject.revision});
  if(!current())return;
  // Keep pending edits and the current form. Only this block was committed.
  cmsProject=result;
  cmsDraft.revision=result.revision;
  cmsDraft.blocks=cmsDraft.blocks.filter(b=>b.id!==id);
  if(cmsEditID===id){
   cmsEditID='';cmsFormDirty=false;
   document.getElementById('cms-editor').innerHTML=cmsEditor();
  }
  cmsDeleteID='';
  cmsRefreshOrder();
  notify('Материал удалён');
 }catch(err){
  if(current())document.querySelector('.cms-remove-error').textContent=err.message;
 }finally{
  cmsLock(false);
  if(current())(document.querySelector('.cms-remove-prompt button')||document.getElementById('cms-order-heading')).focus({preventScroll:true});
 }
}
function cmsAction(a){
 if(!a.startsWith('cms'))return false;
 if(!cmsAdmin||page!=='admin')return true;
 if(cmsBusy){notify('Дождитесь завершения сохранения.');return true}
 if(a==='cmsdiagnostic'){void cmsCheckChat();return true}
 const [cmd,v]=a.split(':');
 if(cmd==='cmsproject'){
  if(projectById(v)&&v!==cmsDraft?.id&&cmsCanLeave()){openedProject=v;void cmsLoad(v,true)}
  return true;
 }
 if(!cmsDraft)return true;
 cmsSync();
 if(cmd==='cmskind'&&cmsKind===v&&!cmsEditID)return true;
 if(['cmskind','cmsedit','cmscancel'].includes(cmd)){
  if(cmsFormDirty&&!confirm('Отменить несохранённые изменения блока?'))return true;
  if(cmd==='cmskind')cmsKind=v;
  cmsEditID=cmd==='cmsedit'?v:'';
  cmsFormDirty=false;
  document.getElementById('cms-editor').innerHTML=cmsEditor();
  if(cmd==='cmsedit')document.querySelector('#cms-form [name=title]').focus();
  if(cmd==='cmskind')document.querySelector('#cms-form [data-action="cmskind:'+v+'"]').focus({preventScroll:true});
 }
 if(cmd==='cmsup'||cmd==='cmsdown')cmsReorder(+v,+v+(cmd==='cmsup'?-1:1));
 if(cmd==='cmsdelete'||cmd==='cmskeep'){
  cmsDeleteID=cmd==='cmsdelete'?v:'';
  cmsRefreshOrder();
  document.querySelector('[data-action="'+(cmsDeleteID?'cmskeep:':'cmsdelete:')+v+'"]')?.focus({preventScroll:true});
 }
 if(cmd==='cmsconfirmdelete')void cmsDeleteBlock(v);
 if(cmd==='cmssave')void cmsSave();
 return true;
}
document.addEventListener('input',e=>{if(e.target.closest('#cms-form'))cmsFormDirty=true});
document.addEventListener('change',e=>{if(e.target.closest('#cms-form'))cmsFormDirty=true});
window.addEventListener('beforeunload',e=>{if(page==='admin'&&(cmsBusy||cmsHasUnsaved())){e.preventDefault();e.returnValue=''}});
let dragBlock='';
document.addEventListener('dragstart',e=>{const el=e.target.closest('.cms-order-row');if(el){dragBlock=el.dataset.block;e.dataTransfer.setData('text/plain',dragBlock)}});
document.addEventListener('dragover',e=>{if(e.target.closest('.cms-order-row'))e.preventDefault()});
document.addEventListener('drop',e=>{const el=e.target.closest('.cms-order-row');if(el&&cmsDraft&&!cmsBusy){e.preventDefault();cmsReorder(cmsDraft.blocks.findIndex(b=>b.id===dragBlock),cmsDraft.blocks.findIndex(b=>b.id===el.dataset.block))}});
function cmsUpload(path,form,onProgress){return new Promise((resolve,reject)=>{const x=new XMLHttpRequest();x.open('POST',path);x.setRequestHeader('Authorization','tma '+(telegram()?.initData||''));x.responseType='json';x.timeout=600000;x.upload.onprogress=e=>{if(e.lengthComputable)onProgress(Math.round(e.loaded/e.total*100))};x.onload=()=>{if(x.status>=200&&x.status<300&&x.response)resolve(x.response);else reject(new Error(x.response?.error||'Не удалось загрузить материал (HTTP '+x.status+').'))};x.onerror=()=>reject(new Error('Соединение прервано. Проверьте проект перед повторной загрузкой.'));x.ontimeout=()=>reject(new Error('Время загрузки истекло. Проверьте проект перед повтором.'));x.send(form)})}
document.addEventListener('submit',async e=>{if(e.target.id!=='cms-form')return;e.preventDefault();if(!cmsAdmin||!cmsDraft||cmsBusy)return;cmsSync();const f=new FormData(e.target),b=cmsDraft.blocks.find(x=>x.id===cmsEditID),snapshot=structuredClone(cmsDraft),ticket=cmsTicket,kind=cmsKind,msg=document.getElementById('cms-message');const current=()=>ticket===cmsTicket&&accessReady&&page==='admin';if(b){for(const k of ['title','text','url','cover'])if(f.has(k))b[k]=f.get(k);b.published=f.has('published');cmsEditID='';cmsFormDirty=false;render();notify('Сохраните изменения проекта');return}const file=f.get('file');if(file instanceof File&&file.size>128*1024*1024){notify('Файл больше 128 МБ.');return}cmsLock(true);try{if(JSON.stringify(snapshot)!==JSON.stringify(cmsProject)){const saved=await cmsRequest('/api/content/'+snapshot.id,'PUT',snapshot);if(!current())return;cmsUpdateSaved(saved)}f.set('kind',kind);f.set('published',String(f.has('published')));if(msg)msg.textContent='Загрузка…';const result=await cmsUpload('/api/content/'+snapshot.id+'/blocks',f,n=>{if(msg&&msg.isConnected)msg.textContent=n===100?'Файл отправлен, сохраняем…':'Загрузка: '+n+'%'});if(!current())return;cmsUpdateSaved(result);cmsFormDirty=false;render();notify('Материал добавлен')}catch(err){if(current()){if(msg)msg.textContent=err.message;notify(err.message)}}finally{cmsLock(false)}});
async function cmsRenewMedia(){if(cmsMediaRenewing||!accessReady||!['project-detail','admin'].includes(page)||document.hidden||Date.now()-cmsMediaRenewedAt<35*60000)return;cmsMediaRenewing=true;const ticket=cmsTicket;try{await cmsRequest('/api/content/media-session','POST');if(ticket===cmsTicket)cmsMediaRenewedAt=Date.now()}catch(e){if(ticket===cmsTicket&&(e.status===401||e.status===403))reportAccessError(e)}finally{cmsMediaRenewing=false}}
document.addEventListener('visibilitychange',()=>{if(!document.hidden)void cmsRenewMedia()});
setInterval(()=>{void cmsRenewMedia()},60000);
function playerStatus(v,message){const el=v.closest('.feed-panel')?.querySelector('.player-status');if(el){el.textContent=message;el.hidden=!message}}
function playerError(v,err){const code=v.error?.code;return code===2?'Не удалось получить видео. Проверьте соединение и откройте проект заново.':code===3?'Не удалось декодировать видео. Рекомендуется MP4 (H.264).':code===4||err?.name==='NotSupportedError'?'Формат видео не поддерживается устройством. Рекомендуется MP4 (H.264).':err?.name==='NotAllowedError'?'Браузер заблокировал запуск. Нажмите Play ещё раз.':'Не удалось запустить видео. Повторите попытку.'}
document.addEventListener('click',async e=>{const b=e.target.closest('.player-launch');if(!b||b.disabled)return;const v=b.parentElement.querySelector('video');b.disabled=true;playerStatus(v,'Загрузка видео…');try{if(v.error){v.removeAttribute('src');v.load()}if(!v.getAttribute('src'))v.src=v.dataset.videoSrc;await v.play();b.hidden=true;playerStatus(v,'')}catch(err){b.hidden=false;playerStatus(v,playerError(v,err))}finally{b.disabled=false}});
document.addEventListener('error',e=>{const v=e.target;if(v.matches?.('.routing-player video')&&v.getAttribute('src')){const b=v.parentElement.querySelector('.player-launch');if(b)b.hidden=false;playerStatus(v,playerError(v))}},true);
document.addEventListener('playing',e=>{if(e.target.matches?.('.routing-player video')){playerStatus(e.target,'');const b=e.target.parentElement.querySelector('.player-launch');if(b)b.hidden=true}},true);
document.addEventListener('waiting',e=>{if(e.target.matches?.('.routing-player video'))playerStatus(e.target,'Загрузка видео…')},true);
document.addEventListener('play',e=>{if(e.target.matches?.('.routing-player video'))document.querySelectorAll('video').forEach(v=>{if(v!==e.target)v.pause()})},true);

async function cmsCheckChat(){const el=document.getElementById('cms-diagnostic');if(!el)return;el.textContent='Проверяем Telegram…';const button=document.querySelector('[data-action="cmsdiagnostic"]');if(button)button.disabled=true;try{const steps=await cmsRequest('/api/admin/chat-diagnostic');if(!el.isConnected)return;el.innerHTML=steps.map(s=>'<div class="chat-diagnostic-row"><span aria-hidden="true">'+(s.ok?'✓':'!')+'</span><div><strong>'+esc(s.check)+'</strong><p>'+esc(s.detail)+'</p></div></div>').join('')}catch(e){if(el.isConnected)el.textContent=e.message}finally{if(button)button.disabled=false}}
