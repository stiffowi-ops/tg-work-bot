
let openedProject='',projectMaterialFilter='all';
const projectById=id=>projects.find(p=>p.id===id);
const projectMaterials=id=>docs.filter(d=>d.p===id);
const orderedProjects=()=>[...projects].sort((a,b)=>Number(b.id===state.project)-Number(a.id===state.project));
function projectPage(){return `<section class="projects-page"><header class="projects-heading"><h1>Проекты</h1><p>Выберите направление своей работы</p></header><div class="projects-grid">${orderedProjects().map(renderProjectCard).join('')}</div></section>`}
function openProject(id){if(!projectById(id))return;openedProject=id;projectMaterialFilter='all';go('project-detail')}
function projectAction(a){
 if(a.startsWith('projectdocs:')){openProject(a.slice(12));return true}
 if(a.startsWith('projectkind:')){projectMaterialFilter=a.slice(12);render();return true}
 if(a.startsWith('projectteam:')){const id=a.slice(12);if(projectById(id)){teamProjectFilter=id;teamFilter='all';teamQuery='';go('team')}return true}
 return false
}
