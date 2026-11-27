function employeeStart() {
 const role = state.role || 'Сотрудник команды';
 return `<section class="employee-start">
   <header class="employee-card" aria-label="Мой профиль">
     <div class="profile-banner-top">
       <span>Мой профиль</span>
     </div>
     <div class="employee-card-head">
       <button class="employee-photo" data-action="profilephoto" aria-label="Изменить фото профиля">${avatar(true)}</button>
       <div class="employee-identity">
         <h1>${esc(state.name)}</h1>
         <p class="employee-role">${esc(role)}</p>
         <button class="employee-status" data-action="profilestatus" aria-label="Изменить рабочий статус">${status()}</button>
       </div>
       <button class="employee-edit" data-action="profileedit" aria-label="Редактировать профиль">
         <svg viewBox="0 0 24 24" aria-hidden="true"><path d="m14 5 5 5M4 20l5-1 12-12-5-5L4 14z"/></svg>
       </button>
     </div>
   </header>
   <div id="cabinet-work" class="cabinet-work">${cabinetWork()}</div>
 </section>`;
}
