package main

import (
 "context"
 "encoding/json"
 "errors"
 "io"
 "net/http"
 "net/url"
 "regexp"
 "sort"
 "strconv"
 "strings"
 "time"
)

// Identity data is part of the existing atomic profile store. No new volume.
type employeeIdentity struct {
 StartMonth string `json:"startMonth,omitempty"`
 LeaderMonths []string `json:"leaderMonths,omitempty"`
 ExpertDates []string `json:"expertDates,omitempty"`
 VeteranNotified bool `json:"veteranNotified,omitempty"`
}
type identityBadge struct {
 Kind string `json:"kind"`
 Count int `json:"count"`
 Active bool `json:"active"`
 History []string `json:"history,omitempty"`
}
type identityProfile struct {
 employee
 Achievements []identityBadge `json:"achievements"`
}
var identityTZ=time.FixedZone("MSK",3*60*60)
func identityMonth(now time.Time) string {return now.In(identityTZ).Format("2006-01")}
func isVeteran(start string, now time.Time) bool {
 t,err:=time.ParseInLocation("2006-01",start,identityTZ)
 return err==nil && !now.Before(t.AddDate(1,0,0))
}
func identityBadges(p employee, now time.Time) []identityBadge {
 out:=[]identityBadge{}
 if len(p.Identity.LeaderMonths)>0 {
  active:=false;for _,m:=range p.Identity.LeaderMonths{if m==identityMonth(now){active=true}}
  out=append(out,identityBadge{"leader",len(p.Identity.LeaderMonths),active,p.Identity.LeaderMonths})
 }
 if len(p.Identity.ExpertDates)>0{out=append(out,identityBadge{"expert",len(p.Identity.ExpertDates),true,p.Identity.ExpertDates})}
 if isVeteran(p.Identity.StartMonth,now){out=append(out,identityBadge{"veteran",1,true,[]string{p.Identity.StartMonth}})}
 return out
}
func employeeView(p employee) identityProfile{return identityProfile{p,identityBadges(p,time.Now())}}
type identityAdminRow struct {
 ID string `json:"id"`
 Name string `json:"name"`
 Identity employeeIdentity `json:"identity"`
 Achievements []identityBadge `json:"achievements"`
}
type identityChange struct {
 ID string `json:"id"`
 Action string `json:"action"`
 StartMonth string `json:"startMonth"`
}
var errIdentityMissing=errors.New("Сотрудник не найден.")
func identityAppend(values []string,v string) ([]string,bool){
 for _,x:=range values{if x==v{return values,false}}
 return append(append([]string(nil),values...),v),true
}
func identityRemove(values []string,v string) []string{
 out:=make([]string,0,len(values));for _,x:=range values{if x!=v{out=append(out,x)}};return out
}
// All changes, including replacing the current leader, are committed together.
func(s *employeeStore) changeIdentity(c identityChange, now time.Time)(employee,string,error){
 s.mu.Lock();defer s.mu.Unlock()
 p,ok:=s.people[c.ID];if !ok{return p,"",errIdentityMissing}
 next:=make(map[string]employee,len(s.people));for id,v:=range s.people{next[id]=v}
 month:=identityMonth(now);notice:=""
 switch c.Action{
 case "start":
  if c.StartMonth!=""{if _,err:=time.Parse("2006-01",c.StartMonth);err!=nil||c.StartMonth>month{return p,"",errors.New("Укажите корректный месяц начала работы.")}}
  p.Identity.StartMonth=c.StartMonth
  if !isVeteran(c.StartMonth,now){p.Identity.VeteranNotified=false}
 case "leader":
  var added bool;p.Identity.LeaderMonths,added=identityAppend(p.Identity.LeaderMonths,month)
  if added{notice="leader"}
  for id,other:=range next{if id!=p.ID{other.Identity.LeaderMonths=identityRemove(other.Identity.LeaderMonths,month);next[id]=other}}
 case "revoke-leader":p.Identity.LeaderMonths=identityRemove(p.Identity.LeaderMonths,month)
 case "expert":
  var added bool;p.Identity.ExpertDates,added=identityAppend(p.Identity.ExpertDates,now.In(identityTZ).Format("2006-01-02"))
  if added{notice="expert"}
 case "revoke-expert":
  if n:=len(p.Identity.ExpertDates);n>0{p.Identity.ExpertDates=append([]string(nil),p.Identity.ExpertDates[:n-1]...)}
 default:return p,"",errors.New("Неизвестное действие.")
 }
 next[p.ID]=p
 if err:=s.persistLocked(next);err!=nil{return p,"",err}
 return p,notice,nil
}
func(a *employeeAPI) identityAdmin(w http.ResponseWriter,r *http.Request){
 u,ok:=a.authorize(w,r);if !ok{return}
 if !isAdmin(u.ID){apiError(w,403,"Доступ только администратору.");return}
 if r.Method=="GET"{
  people:=a.store.all();sort.Slice(people,func(i,j int)bool{return people[i].Name<people[j].Name})
  out:=make([]identityAdminRow,0,len(people));for _,p:=range people{out=append(out,identityAdminRow{p.ID,p.Name,p.Identity,identityBadges(p,time.Now())})}
  writeJSON(w,200,out);return
 }
 var c identityChange;dec:=json.NewDecoder(http.MaxBytesReader(w,r.Body,8192));dec.DisallowUnknownFields()
 if dec.Decode(&c)!=nil||dec.Decode(new(any))!=io.EOF{apiError(w,400,"Некорректные данные.");return}
 id,err:=strconv.ParseInt(c.ID,10,64);if err!=nil||id<=0||strconv.FormatInt(id,10)!=c.ID{apiError(w,400,"Некорректный сотрудник.");return}
 if !isAdmin(id){member,err:=a.member(r.Context(),id);if err!=nil{apiError(w,503,"Не удалось проверить членство сотрудника.");return};if !member{apiError(w,403,"Сотрудник не состоит в рабочей группе.");return}}
 if _,ok:=a.store.get(c.ID);!ok{apiError(w,404,errIdentityMissing.Error());return}
 // Validate request separately; storage errors must not expose internal paths.
 switch c.Action{case "start","leader","revoke-leader","expert","revoke-expert":default:apiError(w,400,"Неизвестное действие.");return}
 now:=time.Now()
 if c.Action=="start"&&c.StartMonth!=""{if _,err:=time.Parse("2006-01",c.StartMonth);err!=nil||c.StartMonth>identityMonth(now){apiError(w,400,"Укажите корректный месяц начала работы.");return}}
 p,notice,err:=a.store.changeIdentity(c,now);if err!=nil{apiError(w,500,"Не удалось сохранить достижения.");return}
 delivery:="none"
 if notice!=""{delivery="sent";if a.sendIdentity(r.Context(),p.ID,notice)!=nil{delivery="failed"}}
 writeJSON(w,200,map[string]any{"employee":identityAdminRow{p.ID,p.Name,p.Identity,identityBadges(p,now)},"notification":delivery})
}
var identityBotName=regexp.MustCompile("^[A-Za-z0-9_]{5,32}$")
func(a *employeeAPI) sendIdentity(ctx context.Context,id,kind string)error{
 titles:=map[string]string{"leader":"Лидер месяца","expert":"Эксперт команды","veteran":"Ветеран команды"}
 title,ok:=titles[kind];if !ok{return errors.New("unknown achievement")}
 var bot struct{Username string `json:"username"`}
 if err:=a.call(ctx,"getMe",url.Values{},&bot);err!=nil{return err}
 if !identityBotName.MatchString(bot.Username){return errors.New("bot username unavailable")}
 link:="https://t.me/"+bot.Username+"?startapp=achievement_"+kind
 keyboard,_:=json.Marshal(map[string]any{"inline_keyboard":[][]map[string]string{{{"text":"Посмотреть достижение","url":link}}}})
 var result json.RawMessage
 return a.call(ctx,"sendMessage",url.Values{"chat_id":{id},"text":{"Поздравляем! Новое достижение: «"+title+"»."},"reply_markup":{string(keyboard)}},&result)
}
func(s *employeeStore) markVeteran(id,start string)error{
 s.mu.Lock();defer s.mu.Unlock()
 p,ok:=s.people[id];if !ok||p.Identity.StartMonth!=start||p.Identity.VeteranNotified{return nil}
 p.Identity.VeteranNotified=true;_,err:=s.putLocked(p);return err
}
// One sequential worker per process; deployment already requires a single replica.
func(a *employeeAPI) veteranWorker(ctx context.Context){
 if a.store==nil{return}
 tick:=time.NewTicker(time.Hour);defer tick.Stop()
 for{
  for _,p:=range a.store.all(){
   if ctx.Err()!=nil{return}
   if p.Identity.VeteranNotified||!isVeteran(p.Identity.StartMonth,time.Now()){continue}
   id,err:=strconv.ParseInt(p.ID,10,64);if err!=nil{continue}
   if !isAdmin(id){ok,err:=a.member(ctx,id);if err!=nil||!ok{continue}}
   if a.sendIdentity(ctx,p.ID,"veteran")==nil{_ = a.store.markVeteran(p.ID,p.Identity.StartMonth)}
  }
  select{case <-ctx.Done():return;case <-tick.C:}
 }
}
func identityStartParam(s string)bool{return strings.HasPrefix(s,"achievement_")}
