package main
import("net/http";"net/url";"strconv";"errors")
type chatDiagnosticStep struct{
 Check string `json:"check"`
 OK bool `json:"ok"`
 Detail string `json:"detail"`
}
func diagnosticError(err error)string{
 var e *telegramAPIError
 if errors.As(err,&e){
  if e.MigrateTo<0{return "Группа преобразована в супергруппу. Новый WORK_CHAT_ID: "+strconv.FormatInt(e.MigrateTo,10)}
  if e.Code==429||e.HTTPStatus==429{return "Telegram ограничил частоту запросов. Повторите позже."}
  if e.Code==401||e.HTTPStatus==401{return "Telegram отклонил BOT_TOKEN."}
  if e.Code==403||e.HTTPStatus==403{return "Боту запрещён доступ к чату. Проверьте членство и права."}
  if e.Code==400||e.HTTPStatus==400{return "Telegram не принял запрос. Проверьте ID группы, членство и права бота."}
 }
 return "Telegram не ответил корректно. Проверьте соединение и настройки."
}
func(c *cms) chatDiagnostic(w http.ResponseWriter,r *http.Request){
 u,ok:=c.admin(w,r);if !ok{return}
 steps:=[]chatDiagnosticStep{}
 add:=func(check string,ok bool,detail string){steps=append(steps,chatDiagnosticStep{check,ok,detail})}
 var bot tgUser
 err:=c.auth.call(r.Context(),"getMe",url.Values{},&bot)
 if err!=nil||bot.ID<=0||!bot.Bot{add("Токен и аккаунт бота",false,diagnosticError(err));writeJSON(w,200,steps);return}
 add("Токен и аккаунт бота",true,"@"+bot.Username)
 if c.auth.chat>=0{add("WORK_CHAT_ID",false,"Укажите отрицательный числовой ID группы.");writeJSON(w,200,steps);return}
 params:=url.Values{"chat_id":{strconv.FormatInt(c.auth.chat,10)}}
 var chat struct{ID int64;Title,Type string}
 if err=c.auth.call(r.Context(),"getChat",params,&chat);err!=nil{add("Рабочая группа",false,diagnosticError(err));writeJSON(w,200,steps);return}
 groupOK:=chat.ID==c.auth.chat&&(chat.Type=="group"||chat.Type=="supergroup")
 add("Рабочая группа",groupOK,chat.Title+" ("+strconv.FormatInt(chat.ID,10)+", "+chat.Type+")")
 if !groupOK{writeJSON(w,200,steps);return}
 var botMembership struct{Status string}
 err=c.auth.call(r.Context(),"getChatMember",url.Values{"chat_id":params["chat_id"],"user_id":{strconv.FormatInt(bot.ID,10)}},&botMembership)
 if err!=nil{add("Права бота",false,diagnosticError(err))}else{add("Права бота",botMembership.Status=="administrator"||botMembership.Status=="creator",botMembership.Status+" — бот должен быть администратором")}
 var count int
 if err=c.auth.call(r.Context(),"getChatMemberCount",params,&count);err!=nil{add("Участники группы",false,diagnosticError(err))}else{add("Участники группы",count>0,strconv.Itoa(count)+" участников, включая ботов. Bot API не выдаёт полный список имён.")}
 member,err:=c.auth.member(r.Context(),u.ID)
 if err!=nil{add("Ваше членство в группе",false,diagnosticError(err))}else if member{add("Ваше членство в группе",true,"Telegram подтвердил членство.")}else{add("Ваше членство в группе",false,"Telegram не подтвердил членство. Доступ администратора сохранён.")}
 writeJSON(w,200,steps)
}
