
let cabinetCatalog=null,cabinetError='',cabinetSaving=false,cabinetJump=null,cabinetIndexSeq=0;
const cabinetGroups=[['document','Документы','folder'],['video','Видео','video'],['link','Полезные ссылки','link'],['image','Изображения','file'],['faq','FAQ','help'],['text','Информация','book'],['section','Подразделы','book']];
const cabinetRef=m=>m.project+':'+m.id;
const cabinetIsSaved=m=>(state.savedMaterials||[]).includes(cabinetRef(m));
function cabinetBookmark(m){const ref=cabinetRef(m),saved=cabinetIsSaved(m);return '<button type="button" class="cabinet-bookmark '+(saved?'on':'')+'" data-action="cabinet-save:'+esc(ref)+'" data-material-save="'+esc(ref)+'" aria-label="'+(saved?'Убрать из сохранённых':'Сохранить материал')+'" aria-pressed="'+saved+'">'+I('saved')+'</button>'}
function cabinetRow(m){return '<div class="cabinet-material-row"><button type="button" class="cabinet-material-title" data-action="cabinet-open:'+esc(cabinetRef(m))+'">'+esc(m.title)+'</button>'+cabinetBookmark(m)+'</div>'}
function cabinetFolder(key,title,icon,items){const k='cabinet:'+key;return '<details class="collection cabinet-folder" data-key="'+esc(k)+'" '+(collectionOpen.get(k)?'open':'')+'><summary><span class="cabinet-folder-icon">'+I(icon)+'</span><span class="grow">'+esc(title)+'</span><small>'+items.length+'</small>'+I('arrow')+'</summary><div class="cabinet-folder-body">'+items.map(cabinetRow).join('')+'</div></details>'}
function cabinetSavedList(){if(cabinetCatalog===null)return '<p class="cabinet-note">'+esc(cabinetError||'Загружаем материалы…')+'</p>'+(cabinetError?btn('Повторить','cabinet-refresh'):'');const items=cabinetCatalog.filter(cabinetIsSaved);return items.length?projects.map(p=>{const matches=items.filter(m=>m.project===p.id);return matches.length?cabinetFolder('saved:'+p.id,p.name,'folder',matches):''}).join(''):'<p class="cabinet-note">Сохраняйте нужные материалы с помощью закладки. Они появятся здесь.</p>'}
let cabinetTab='saved',cabinetPositions={saved:0,recent:0,project:0},cabinetRecentQueue=Promise.resolve();
function cabinetRememberPosition(){const rail=document.querySelector('.cabinet-rail');if(rail)cabinetPositions[cabinetTab]=rail.scrollLeft}
function cabinetRestorePosition(){const rail=document.querySelector('.cabinet-rail');if(rail)rail.scrollLeft=cabinetPositions[cabinetTab]||0}
function cabinetArtwork(m){
 if(m.kind==='document')return 'cover-'+cmsCoverKey(m.cover);
 if(m.kind==='link'){const k=cmsLinkCoverKey(m.cover);return 'cover-'+k+(Object.hasOwn(cmsLinkCovers,k)?' link-art-'+k:'')}
 if(m.kind==='video')return 'cover-video';
 return 'cover-'+({text:'script',section:'analytics',faq:'instruction',image:'case'}[m.kind]||'general');
}
function cabinetTileFace(title,subtitle,art,icon=''){
 return '<span class="cabinet-tile-art cover-art '+art+'" aria-hidden="true">'+(icon?I(icon):'')+'</span><span class="cabinet-tile-copy"><strong title="'+esc(title)+'">'+esc(title)+'</strong><small>'+esc(subtitle)+'</small></span>'
}
function cabinetTile(m){
 return '<div class="cabinet-tile"><button class="cabinet-tile-open" aria-label="'+esc(m.title)+'" data-action="cabinet-open:'+esc(cabinetRef(m))+'">'+cabinetTileFace(m.title,cmsKinds[m.kind]||'Материал',cabinetArtwork(m))+'</button>'+cabinetBookmark(m)+'</div>'
}
function cabinetCategory(k,title,icon,count){
 const art='cover-'+({document:'presentation',video:'video',link:'website link-art-website',image:'case',faq:'instruction',text:'script',section:'analytics'}[k]);
 const label=count+' '+(count%10===1&&count%100!==11?'материал':count%10>=2&&count%10<=4&&(count%100<12||count%100>14)?'материала':'материалов');
 return '<div class="cabinet-tile"><button class="cabinet-tile-open cabinet-category" aria-label="'+esc(title)+'" data-action="cabinet-category:'+k+'">'+cabinetTileFace(title,label,art,icon)+'</button></div>'
}
function cabinetWork(){
 const p=active(),catalog=cabinetCatalog||[],items=p?catalog.filter(m=>m.project===p.id):[];
 const refs=cabinetTab==='recent'?state.recentMaterials||[]:[...(state.savedMaterials||[])].reverse();
 const byRef=new Map(catalog.map(m=>[cabinetRef(m),m]));
 const matches=refs.map(ref=>byRef.get(ref)).filter(Boolean);
 let body='';
 if(cabinetCatalog===null)body='<p class="cabinet-note">'+esc(cabinetError||'Загружаем материалы…')+'</p>'+(cabinetError?btn('Повторить','cabinet-refresh'):'');
 else if(cabinetTab==='project'){
  body=p?'<div class="cabinet-rail" tabindex="0" role="region" aria-label="Карусель материалов">'+cabinetGroups.map(([k,t,i])=>{const n=items.filter(m=>m.kind===k).length;return n?cabinetCategory(k,t,i,n):''}).join('')+'</div>':'<p class="cabinet-note">Закрепите проект, чтобы его материалы были под рукой.</p>';
  if(p&&!items.length)body='<p class="cabinet-note">В проекте пока нет опубликованных материалов.</p>';
 }else body=matches.length?'<div class="cabinet-rail" tabindex="0" role="region" aria-label="Карусель материалов">'+matches.map(cabinetTile).join('')+'</div>':'<p class="cabinet-note">'+(cabinetTab==='recent'?'Здесь появятся материалы, которые вы открывали.':'Сохраните нужный материал с помощью закладки.')+'</p>';
 return '<section class="cabinet-section"><div class="cabinet-section-head"><h2>Мои материалы</h2></div><div class="cabinet-tabs" role="group" aria-label="Подборка материалов">'+[['saved','Сохранённые'],['recent','Недавние'],['project','Мой проект']].map(([k,t])=>'<button data-action="cabinet-tab:'+k+'" aria-pressed="'+(cabinetTab===k)+'">'+t+'</button>').join('')+'</div>'+body+'</section><button class="cabinet-training" data-go="tests"><span class="cabinet-folder-icon">'+I('test')+'</span><span class="grow"><strong>Моё обучение</strong><small>Тесты и задания</small></span>'+I('arrow')+'</button>';
}
function cabinetRecord(ref){
 const split=ref.indexOf(':');if(split<0||!accessReady)return;
 const uid=state.id,project=ref.slice(0,split),id=ref.slice(split+1);
 cabinetRecentQueue=cabinetRecentQueue.catch(()=>{}).then(async()=>{
  if(!accessReady||state.id!==uid)return;
  try{const r=await cmsRequest('/api/me/recent','POST',{project,id});if(accessReady&&state.id===uid){state.recentMaterials=r.recentMaterials||[];if(['profile','home'].includes(page))cabinetRefreshUI()}}catch(e){if(e.status===401||e.status===403)reportAccessError(e)}
 });
}
document.addEventListener('click',e=>{
 if(page!=='project-detail'||!accessReady)return;
 const item=e.target.closest('[data-material-ref]');if(!item||e.target.closest('[data-material-save]'))return;
 if(e.target.closest('a,.player-launch,summary'))cabinetRecord(item.dataset.materialRef);
});
document.addEventListener('play',e=>{if(page==='project-detail'&&e.target.matches?.('video')){const item=e.target.closest('[data-material-ref]');if(item)cabinetRecord(item.dataset.materialRef)}},true);
function cabinetSavedPage(){return '<section class="cabinet-section cabinet-saved-page"><button class="team-back" data-go="profile">‹ Мой профиль</button><h1>Сохранённые материалы</h1><p class="muted">Опубликованные материалы из вашей личной подборки</p><div id="cabinet-saved-content" class="cabinet-panel">'+cabinetSavedList()+'</div></section>'}
function cabinetRefreshUI(){if(!accessReady)return;rememberCollections();const work=document.getElementById('cabinet-work'),saved=document.getElementById('cabinet-saved-content');if(work){cabinetRememberPosition();work.innerHTML=cabinetWork();cabinetRestorePosition()}if(saved)saved.innerHTML=cabinetSavedList();for(const b of document.querySelectorAll('[data-material-save]')){const on=(state.savedMaterials||[]).includes(b.dataset.materialSave);b.classList.toggle('on',on);b.setAttribute('aria-pressed',String(on));b.setAttribute('aria-label',on?'Убрать из сохранённых':'Сохранить материал');b.disabled=cabinetSaving}}
async function cabinetSave(ref){if(cabinetSaving||mutationBusy||!accessReady)return;const split=ref.indexOf(':');if(split<0)return;const project=ref.slice(0,split),id=ref.slice(split+1),saved=!(state.savedMaterials||[]).includes(ref);cabinetSaving=true;document.querySelectorAll('[data-material-save]').forEach(b=>b.disabled=true);try{const result=await cmsRequest('/api/me/materials','PUT',{project,id,saved});if(!accessReady)return;state.savedMaterials=result.savedMaterials||[];notify(saved?'Материал сохранён':'Материал удалён из сохранённых')}catch(e){if(e.status===401||e.status===403)reportAccessError(e);else notify(e.message)}finally{cabinetSaving=false;cabinetRefreshUI()}}
function cabinetJumpTo(){const jump=cabinetJump;if(!jump||page!=='project-detail'||cmsProject?.id!==jump.project)return;cabinetJump=null;requestAnimationFrame(()=>{if(page!=='project-detail'||cmsProject?.id!==jump.project)return;const el=[...document.querySelectorAll('[data-material-ref]')].find(x=>x.dataset.materialRef===jump.project+':'+jump.id);if(!el){notify('Материал больше недоступен.');return}el.scrollIntoView({block:'start',behavior:'smooth'});el.classList.add('cabinet-highlight');setTimeout(()=>el.classList.remove('cabinet-highlight'),1600)})}
function cabinetAction(a){if(a.startsWith('cabinet-tab:')){const v=a.slice(12);if(['saved','recent','project'].includes(v)){cabinetRememberPosition();cabinetTab=v;const work=document.getElementById('cabinet-work');if(work){work.innerHTML=cabinetWork();cabinetRestorePosition()}}return true}if(a.startsWith('cabinet-category:')){const k=a.slice(17),p=active(),g=cabinetGroups.find(x=>x[0]===k);if(p&&g){const items=(cabinetCatalog||[]).filter(m=>m.project===p.id&&m.kind===k);modal(g[1],'<div class="cabinet-panel">'+items.map(cabinetRow).join('')+'</div>')}return true}if(a==='cabinet-refresh'){void cmsInit();return true}if(a.startsWith('cabinet-save:')){void cabinetSave(a.slice(13));return true}if(a.startsWith('cabinet-open:')){const ref=a.slice(13),m=cabinetCatalog?.find(x=>cabinetRef(x)===ref);if(m){cabinetRecord(ref);cabinetJump={project:m.project,id:m.id};openProject(m.project)}return true}return false}

// Native touch swipe; desktop mouse dragging does not activate a card.
let cabinetDrag=null,cabinetSuppressClick=false;
document.addEventListener('pointerdown',e=>{
 const rail=e.target.closest('.cabinet-rail');
 if(!rail||e.pointerType!=='mouse'||e.button!==0||e.target.closest('[data-material-save]')||rail.scrollWidth<=rail.clientWidth)return;
 cabinetSuppressClick=false;
 cabinetDrag={rail,id:e.pointerId,x:e.clientX,left:rail.scrollLeft,moved:false};
});
document.addEventListener('pointermove',e=>{
 const d=cabinetDrag;if(!d||e.pointerId!==d.id)return;
 if(!d.rail.isConnected||!(e.buttons&1)){cabinetDrag=null;return}
 const delta=e.clientX-d.x;
 if(!d.moved&&Math.abs(delta)<6)return;
 if(!d.moved){d.moved=true;d.rail.classList.add('is-dragging');d.rail.setPointerCapture(e.pointerId)}
 d.rail.scrollLeft=d.left-delta;e.preventDefault();
});
function cabinetEndDrag(e){
 const d=cabinetDrag;if(!d||e.pointerId!==d.id)return;
 cabinetDrag=null;d.rail.classList.remove('is-dragging');
 if(d.rail.hasPointerCapture(e.pointerId))d.rail.releasePointerCapture(e.pointerId);
 if(d.moved){cabinetSuppressClick=true;setTimeout(()=>{cabinetSuppressClick=false},0)}
}
document.addEventListener('pointerup',cabinetEndDrag);
document.addEventListener('pointercancel',cabinetEndDrag);
document.addEventListener('click',e=>{if(cabinetSuppressClick){e.preventDefault();e.stopImmediatePropagation();cabinetSuppressClick=false}},true);
document.addEventListener('keydown',e=>{
 const rail=e.target.closest('.cabinet-rail');if(!rail||e.target!==rail||!['ArrowLeft','ArrowRight'].includes(e.key))return;
 e.preventDefault();rail.scrollBy({left:(e.key==='ArrowRight'?1:-1)*rail.clientWidth*.65,behavior:matchMedia('(prefers-reduced-motion: reduce)').matches?'auto':'smooth'});
});
