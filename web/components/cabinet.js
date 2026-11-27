
let cabinetCatalog=null,cabinetError='',cabinetSaving=false,cabinetJump=null,cabinetIndexSeq=0;
const cabinetGroups=[['document','Документы','folder'],['video','Видео','video'],['link','Полезные ссылки','link'],['image','Изображения','file'],['faq','FAQ','help'],['text','Информация','book'],['section','Подразделы','book']];
const cabinetRef=m=>m.project+':'+m.id;
const cabinetIsSaved=m=>(state.savedMaterials||[]).includes(cabinetRef(m));
function cabinetBookmark(m){const ref=cabinetRef(m),saved=cabinetIsSaved(m);return '<button type="button" class="cabinet-bookmark '+(saved?'on':'')+'" data-action="cabinet-save:'+esc(ref)+'" data-material-save="'+esc(ref)+'" aria-label="'+(saved?'Убрать из сохранённых':'Сохранить материал')+'" aria-pressed="'+saved+'">'+I('saved')+'</button>'}
function cabinetRow(m){return '<div class="cabinet-material-row"><button type="button" class="cabinet-material-title" data-action="cabinet-open:'+esc(cabinetRef(m))+'">'+esc(m.title)+'</button>'+cabinetBookmark(m)+'</div>'}
function cabinetFolder(key, title, icon, items, renderRow = cabinetRow) {
 const folderKey = 'cabinet:' + key;
 return `<details class="collection cabinet-folder" data-key="${esc(folderKey)}" ${collectionOpen.get(folderKey) ? 'open' : ''}>
   <summary><span class="cabinet-folder-icon">${I(icon)}</span><span class="grow">${esc(title)}</span>
     <small>${items.length}</small>${I('arrow')}</summary>
   <div class="cabinet-folder-body">${items.map(renderRow).join('')}</div>
 </details>`;
}

function savedLibraryDocs() {
 return docs.filter(doc => (state.saved || []).includes(doc.id));
}

function savedMaterialCount() {
 if (cabinetCatalog === null) return null;
 return cabinetCatalog.filter(cabinetIsSaved).length + savedLibraryDocs().length;
}

function cabinetCatalogNotice() {
 return `<p class="cabinet-note">${esc(cabinetError || 'Загружаем материалы…')}</p>` +
   (cabinetError ? btn('Повторить', 'cabinet-refresh') : '');
}

function cabinetSavedList() {
 const saved = (cabinetCatalog || []).filter(cabinetIsSaved);
 const folders = projects.map(project => {
   const items = saved.filter(material => material.project === project.id);
   return items.length ? cabinetFolder('saved:' + project.id, project.name, 'folder', items) : '';
 }).join('');
 const libraryDocs = savedLibraryDocs();
 const libraryFolder = libraryDocs.length
   ? cabinetFolder('saved:library', 'Инфосистема', 'book', libraryDocs, rowdoc)
   : '';
 const notice = cabinetCatalog === null ? cabinetCatalogNotice() : '';
 return folders + libraryFolder + notice ||
   '<p class="cabinet-note">Сохраняйте нужные материалы с помощью закладки. Они появятся здесь.</p>';
}

function cabinetProjectFolders(project) {
 if (!project) return '';
 let body;
 if (cabinetCatalog === null) {
   body = cabinetCatalogNotice();
 } else {
   const items = cabinetCatalog.filter(material => material.project === project.id);
   body = cabinetGroups.map(([kind, title, icon]) => {
     const matches = items.filter(material => material.kind === kind);
     return matches.length ? cabinetFolder(project.id + ':' + kind, title, icon, matches) : '';
   }).join('') || '<p class="cabinet-note">В проекте пока нет опубликованных материалов.</p>';
 }
 return `<section class="cabinet-section">
   <div class="cabinet-section-head"><h2>Материалы моего проекта</h2>
     <button class="cabinet-text-link" data-action="projectdocs:${esc(project.id)}">Все ${I('arrow')}</button>
   </div><div class="cabinet-panel">${body}</div>
 </section>`;
}

function cabinetWork() {
 const project = active();
 const count = savedMaterialCount();
 return `<div class="cabinet-panel cabinet-shortcuts">
   <button class="cabinet-quick" data-action="${project ? 'projectdocs:' + esc(project.id) : 'choose'}">
     <span class="cabinet-folder-icon">${I('project')}</span>
     <span class="grow"><span class="shortcut-label">Мой проект</span><strong>${esc(project?.name || 'Выбрать проект')}</strong></span>${I('arrow')}
   </button>
   <button class="cabinet-quick" data-go="cabinet-saved">
     <span class="cabinet-folder-icon">${I('saved')}</span><span class="grow"><strong>Сохранённые материалы</strong></span>
     ${count === null ? '' : `<span class="cabinet-count">${count}</span>`}${I('arrow')}
   </button>
 </div>
 ${cabinetProjectFolders(project)}
 <section class="cabinet-section">
   <div class="cabinet-section-head"><h2>Личное</h2></div>
   <div class="cabinet-panel">
     <button class="cabinet-quick" data-go="team"><span class="cabinet-folder-icon">${I('users')}</span>
       <span class="grow"><strong>Наша команда</strong><small>Коллеги и рабочие статусы</small></span>${I('arrow')}</button>
     <button class="cabinet-quick" data-go="tests"><span class="cabinet-folder-icon">${I('test')}</span>
       <span class="grow"><strong>Моё обучение</strong><small>Демо-раздел</small></span>${I('arrow')}</button>
     <button class="cabinet-quick" data-go="notifications"><span class="cabinet-folder-icon">${I('bell')}</span>
       <span class="grow"><strong>Уведомления</strong><small>Демо-раздел</small></span>${I('arrow')}</button>
   </div>
 </section>`;
}

let cabinetSavedOrigin = 'profile';
function cabinetSavedPage() {
 const backLabel = cabinetSavedOrigin === 'library' ? 'Инфосистема' : 'Мой профиль';
 return `<section class="cabinet-section cabinet-saved-page">
   <button class="team-back" data-go="${cabinetSavedOrigin}">‹ ${backLabel}</button><h1>Сохранённые материалы</h1>
   <div id="cabinet-saved-content" class="cabinet-panel">${cabinetSavedList()}</div>
 </section>`;
}
function cabinetRefreshUI(){if(!accessReady)return;rememberCollections();const work=document.getElementById('cabinet-work'),saved=document.getElementById('cabinet-saved-content');if(work)work.innerHTML=cabinetWork();if(saved)saved.innerHTML=cabinetSavedList();for(const b of document.querySelectorAll('[data-material-save]')){const on=(state.savedMaterials||[]).includes(b.dataset.materialSave);b.classList.toggle('on',on);b.setAttribute('aria-pressed',String(on));b.setAttribute('aria-label',on?'Убрать из сохранённых':'Сохранить материал');b.disabled=cabinetSaving}}
async function cabinetSave(ref){if(cabinetSaving||mutationBusy||!accessReady)return;const split=ref.indexOf(':');if(split<0)return;const project=ref.slice(0,split),id=ref.slice(split+1),saved=!(state.savedMaterials||[]).includes(ref);cabinetSaving=true;document.querySelectorAll('[data-material-save]').forEach(b=>b.disabled=true);try{const result=await cmsRequest('/api/me/materials','PUT',{project,id,saved});if(!accessReady)return;state.savedMaterials=result.savedMaterials||[];notify(saved?'Материал сохранён':'Материал удалён из сохранённых')}catch(e){if(e.status===401||e.status===403)reportAccessError(e);else notify(e.message)}finally{cabinetSaving=false;cabinetRefreshUI()}}
function cabinetJumpTo(){const jump=cabinetJump;if(!jump||page!=='project-detail'||cmsProject?.id!==jump.project)return;cabinetJump=null;requestAnimationFrame(()=>{if(page!=='project-detail'||cmsProject?.id!==jump.project)return;const el=[...document.querySelectorAll('[data-material-ref]')].find(x=>x.dataset.materialRef===jump.project+':'+jump.id);if(!el){notify('Материал больше недоступен.');return}el.scrollIntoView({block:'start',behavior:'smooth'});el.classList.add('cabinet-highlight');setTimeout(()=>el.classList.remove('cabinet-highlight'),1600)})}
function cabinetAction(a){if(a==='cabinet-refresh'){void cmsInit();return true}if(a.startsWith('cabinet-save:')){void cabinetSave(a.slice(13));return true}if(a.startsWith('cabinet-open:')){const ref=a.slice(13),m=cabinetCatalog?.find(x=>cabinetRef(x)===ref);if(m){cabinetJump={project:m.project,id:m.id};openProject(m.project)}return true}return false}
