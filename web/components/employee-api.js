let accessReady=false, teamRecords=[], teamLoading=false, teamError='', mutationBusy=false;
const telegram=()=>window.Telegram?.WebApp;
async function employeeRequest(path,method='GET',body){
 const init=telegram()?.initData;if(!init)throw Object.assign(new Error('Откройте приложение через Telegram.'),{status:401});
 const headers={Authorization:'tma '+init};
 if(body!==undefined)headers['Content-Type']='application/json';
 const response=await fetch(path,{method,headers,body:body===undefined?undefined:JSON.stringify(body),cache:'no-store'});
 let data;try{data=await response.json()}catch{throw new Error('Сервер временно недоступен.')}
 if(!response.ok)throw Object.assign(new Error(data.error||'Не удалось выполнить запрос.'),{status:response.status});
 return data;
}
function accessScreen(message, retry=false){
 accessReady=false;cabinetCatalog=null;cabinetTab='saved';cabinetPositions={saved:0,recent:0,project:0};cmsAdmin=false;cmsProject=null;cmsDraft=null;cmsTicket++;state={name:'',role:'',bio:'',photo:'',status:'away',project:'',saved:[],read:[],progress:0};teamRecords=[];
 document.body.classList.remove('team-view','projects-view','project-industry');document.body.classList.add('routing-view');document.getElementById('sidebar').innerHTML='';document.getElementById('top').innerHTML='<div class="v4-top-inner"><div class="v4-brand">'+brand()+'</div></div>';document.getElementById('mobile').classList.add('hidden');
 document.getElementById('overlay').innerHTML='';profileDraft=null;
 document.getElementById('content').innerHTML='<div class="panel access-panel"><h1>Рабочее пространство</h1><p>'+esc(message)+'</p>'+(retry?'<button class="secondary" id="access-retry">Повторить</button>':'')+'</div>';
 document.getElementById('access-retry')?.addEventListener('click',bootEmployee);
}
function reportAccessError(error){
 if(error.status===401||error.status===403){accessScreen(error.message);return}
 notify(error.message||'Не удалось сохранить данные.');
}
async function bootEmployee(){
 telegram()?.ready();telegram()?.expand();
 accessScreen('Проверяем доступ к рабочему чату…');
 try{state=await employeeRequest('/api/me');accessReady=true;document.getElementById('mobile').classList.remove('hidden');page='profile';render();void cmsInit()}
 catch(error){accessScreen(error.message,error.status!==401&&error.status!==403)}
}
async function saveState(next){
 if(!accessReady||mutationBusy)return false;
 mutationBusy=true;try{state=await employeeRequest('/api/me','PUT',next);return true}catch(error){reportAccessError(error);return false}finally{mutationBusy=false}
}
async function refreshTeam(){
 if(!accessReady||teamLoading)return;
 teamLoading=true;teamError='';teamRecords=[];renderTeam();
 try{teamRecords=await employeeRequest('/api/team')}catch(error){teamError=error.message;reportAccessError(error)}finally{teamLoading=false;if(accessReady&&page==='team')renderTeam()}
}
