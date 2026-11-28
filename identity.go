package main
import(
 "encoding/json"
 "errors"
 "io"
 "net/http"
 "sort"
 "strconv"
 "time"
)
type employeeIdentity struct {
 StartMonth string `json:"startMonth,omitempty"`
 LeaderMonths []string `json:"leaderMonths,omitempty"`
 ExpertDates []string `json:"expertDates,omitempty"`
}
type identityAward struct {
 Kind string `json:"kind"`
 Count int `json:"count"`
 Current bool `json:"current"`
 Years int `json:"years,omitempty"`
 Subtitle string `json:"subtitle"`
 Description string `json:"description"`
 History []string `json:"history,omitempty"`
 Appearance string `json:"appearance,omitempty"`
}
type identityProfile struct{employee;Achievements []identityAward `json:"achievements"`}
var identityZone=time.FixedZone("MSK",3*60*60)
func identityMonth(now time.Time)string{return now.In(identityZone).Format("2006-01")}
func identityYears(start string,now time.Time)int{
 t,e:=time.ParseInLocation("2006-01",start,identityZone);if e!=nil{return 0}
 n:=now.In(identityZone);y:=n.Year()-t.Year();if n.Month()<t.Month(){y--};if y<0{return 0};return y
}
func identityAwards(p employee,now time.Time)[]identityAward{
 x:=p.Identity;out:=[]identityAward{}
 if len(x.LeaderMonths)>0{
  current:=false;for _,m:=range x.LeaderMonths{if m==identityMonth(now){current=true}}
  sub:="За результаты работы";if current{sub="Лидер текущего месяца"}
  out=append(out,identityAward{Kind:"leader",Count:len(x.LeaderMonths),Current:current,Subtitle:sub,Description:"Награда за выдающийся результат месяца.",History:x.LeaderMonths,Appearance:"Золотая рамка лидера на текущий месяц"})
 }
 if len(x.ExpertDates)>0{out=append(out,identityAward{Kind:"expert",Count:len(x.ExpertDates),Subtitle:"За профессиональные достижения",Description:"Награда за экспертизу и вклад в развитие команды.",History:x.ExpertDates})}
 if years:=identityYears(x.StartMonth,now);years>0{out=append(out,identityAward{Kind:"veteran",Count:1,Years:years,Subtitle:"За стаж в команде",Description:"Присваивается автоматически после года работы.",History:[]string{x.StartMonth}})}
 return out
}
func identityView(p employee,now time.Time)identityProfile{return identityProfile{p,identityAwards(p,now)}}
type identityAdminRow struct{ID string `json:"id"`;Name string `json:"name"`;Identity employeeIdentity `json:"identity"`;Achievements []identityAward `json:"achievements"`}
func identityRow(p employee,now time.Time)identityAdminRow{return identityAdminRow{p.ID,p.Name,p.Identity,identityAwards(p,now)}}
type identityEdit struct{ID string `json:"id"`;Action string `json:"action"`;StartMonth string `json:"startMonth"`}
func identityWithout(xs []string,v string)[]string{out:=[]string{};for _,x:=range xs{if x!=v{out=append(out,x)}};return out}
func identityWith(xs []string,v string)[]string{out:=identityWithout(xs,v);out=append(out,v);sort.Strings(out);return out}
func(s *employeeStore)editIdentity(c identityEdit,now time.Time)(employee,error){
 s.mu.Lock();defer s.mu.Unlock()
 p,ok:=s.people[c.ID];if !ok{return p,errors.New("profile not found")}
 next:=make(map[string]employee,len(s.people));for k,v:=range s.people{next[k]=v}
 month:=identityMonth(now)
 switch c.Action{
 case "start":p.Identity.StartMonth=c.StartMonth
 case "leader":
  p.Identity.LeaderMonths=identityWith(p.Identity.LeaderMonths,month)
  for id,other:=range next{if id!=p.ID{other.Identity.LeaderMonths=identityWithout(other.Identity.LeaderMonths,month);next[id]=other}}
 case "revoke-leader":p.Identity.LeaderMonths=identityWithout(p.Identity.LeaderMonths,month)
 case "expert":p.Identity.ExpertDates=identityWith(p.Identity.ExpertDates,now.In(identityZone).Format("2006-01-02"))
 case "revoke-expert":if n:=len(p.Identity.ExpertDates);n>0{p.Identity.ExpertDates=append([]string(nil),p.Identity.ExpertDates[:n-1]...)}
 default:return p,errors.New("invalid action")
 }
 next[p.ID]=p;return p,s.persistEmployeesLocked(next)
}
func(a *employeeAPI)identityAdmin(w http.ResponseWriter,r *http.Request){
 u,ok:=a.authorize(w,r);if !ok{return};if !isAdmin(u.ID){apiError(w,403,"Доступ только администратору.");return}
 now:=time.Now()
 if r.Method=="GET"{people:=a.store.all();sort.Slice(people,func(i,j int)bool{return people[i].Name<people[j].Name});out:=make([]identityAdminRow,0,len(people));for _,p:=range people{out=append(out,identityRow(p,now))};writeJSON(w,200,out);return}
 var c identityEdit;d:=json.NewDecoder(http.MaxBytesReader(w,r.Body,8192));d.DisallowUnknownFields()
 if d.Decode(&c)!=nil||d.Decode(new(any))!=io.EOF{apiError(w,400,"Некорректные данные.");return}
 id,err:=strconv.ParseInt(c.ID,10,64);if err!=nil||id<=0||strconv.FormatInt(id,10)!=c.ID{apiError(w,400,"Некорректный сотрудник.");return}
 if _,ok:=a.store.get(c.ID);!ok{apiError(w,404,"Сотрудник не найден.");return}
 switch c.Action{
 case "start":if c.StartMonth!=""{if _,err:=time.Parse("2006-01",c.StartMonth);err!=nil||c.StartMonth>identityMonth(now){apiError(w,400,"Некорректная дата начала работы.");return}}
 case "leader","revoke-leader","expert","revoke-expert":
 default:apiError(w,400,"Неизвестное действие.");return
 }
 if !isAdmin(id){member,err:=a.member(r.Context(),id);if err!=nil{apiError(w,503,"Не удалось проверить членство.");return};if !member{apiError(w,403,"Сотрудник не состоит в рабочей группе.");return}}
 p,err:=a.store.editIdentity(c,now);if err!=nil{apiError(w,500,"Не удалось сохранить достижения.");return}
 writeJSON(w,200,identityRow(p,now))
}
