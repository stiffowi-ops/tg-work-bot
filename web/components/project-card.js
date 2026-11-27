
const projectArtwork=p=>p.id==='industry'?'industry':'calendar';
function renderProjectCard(p){
 const pinned=state.project===p.id;
 return `<article class="project-tile ${projectArtwork(p)} ${pinned?'is-pinned':''}"><button type="button" class="project-tile-main" data-action="projectdocs:${esc(p.id)}"><span class="project-tile-copy">${pinned?'<span class="project-pinned-label">Мой проект</span>':''}<h2>${esc(p.name)}</h2><span class="project-description">${esc(p.desc)}</span><span class="project-open">Открыть проект ${I('arrow')}</span></span></button><button type="button" class="project-pin ${pinned?'on':''}" data-action="pin:${esc(p.id)}" aria-label="${pinned?'Открепить':'Закрепить'}: ${esc(p.name)}" aria-pressed="${pinned}">${I('saved')}</button></article>`
}
