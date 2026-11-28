package main

import (
 "bytes"
 "context"
 "crypto/hmac"
 "crypto/sha256"
 "encoding/base64"
 "encoding/hex"
 "encoding/json"
 "errors"
 "image"
 "image/jpeg"
 "io"
 "log"
 "net/http"
 "net/url"
 "os"
 "path/filepath"
 "sort"
 "strconv"
 "strings"
 "sync"
 "time"
 "unicode/utf8"
)

type tgUser struct {
 ID int64 `json:"id"`
 First string `json:"first_name"`
 Last string `json:"last_name"`
 Username string `json:"username"`
 Bot bool `json:"is_bot"`
}
type employee struct {
 ID string `json:"id"`
 Username string `json:"username,omitempty"`
 Name string `json:"name"`
 Role string `json:"role"`
 Bio string `json:"bio"`
 Status string `json:"status"`
 Until string `json:"until"`
 Photo string `json:"photo"`
 Project string `json:"project"`
 Saved []int `json:"saved"`
 SavedMaterials []string `json:"savedMaterials"`
 RecentMaterials []string `json:"recentMaterials,omitempty"`
 Read []int `json:"read"`
 Progress int `json:"progress"`
}
var errUnauthorized=errors.New("invalid Telegram session")
func hmac256(key, value []byte) []byte {
 h:=hmac.New(sha256.New,key); h.Write(value); return h.Sum(nil)
}
func verifyInitData(raw,token string,now time.Time)(tgUser,error){
 var u tgUser
 if token==""||len(raw)==0||len(raw)>16384{return u,errUnauthorized}
 q,err:=url.ParseQuery(raw);if err!=nil{return u,errUnauthorized}
 pairs:=make([]string,0,len(q));for k,v:=range q{
  if len(v)!=1{return u,errUnauthorized}
  if k!="hash"{pairs=append(pairs,k+"="+v[0])}
 }
 sort.Strings(pairs)
 hash,err:=hex.DecodeString(q.Get("hash"));if err!=nil||len(hash)!=32{return u,errUnauthorized}
 key:=hmac256([]byte("WebAppData"),[]byte(token))
 if !hmac.Equal(hash,hmac256(key,[]byte(strings.Join(pairs,"\n")))){return u,errUnauthorized}
 stamp,err:=strconv.ParseInt(q.Get("auth_date"),10,64)
 if err!=nil||stamp>now.Unix()+60||stamp<now.Add(-12*time.Hour).Unix(){return u,errUnauthorized}
 if json.Unmarshal([]byte(q.Get("user")),&u)!=nil||u.ID<=0||u.Bot{return u,errUnauthorized}
 return u,nil
}
type employeeStore struct{
 mu sync.Mutex
 path string
 people map[string]employee
}
func openStore(path string)(*employeeStore,error){
 s:=&employeeStore{path:path,people:map[string]employee{}}
 raw,err:=os.ReadFile(path)
 if errors.Is(err,os.ErrNotExist){return s,nil}
 if err!=nil{return nil,err}
 if err=json.Unmarshal(raw,&s.people);err!=nil||s.people==nil{return nil,errors.New("invalid profile store")}
 return s,nil
}
func(s *employeeStore)get(id string)(employee,bool){s.mu.Lock();defer s.mu.Unlock();p,ok:=s.people[id];return p,ok}
func(s *employeeStore)all()[]employee{s.mu.Lock();defer s.mu.Unlock();out:=make([]employee,0,len(s.people));for _,p:=range s.people{out=append(out,p)};return out}
func(s *employeeStore)put(p employee,createOnly bool)(employee,error){
 s.mu.Lock();defer s.mu.Unlock()
 if old,ok:=s.people[p.ID];ok{if createOnly{return old,nil};p.SavedMaterials=old.SavedMaterials;p.RecentMaterials=old.RecentMaterials}
 return s.putLocked(p)
}
func(s *employeeStore)putLocked(p employee)(employee,error){
 next:=make(map[string]employee,len(s.people)+1);for k,v:=range s.people{next[k]=v};next[p.ID]=p
 raw,err:=json.Marshal(next);if err!=nil{return p,err}
 dir:=filepath.Dir(s.path);if err=os.MkdirAll(dir,0700);err!=nil{return p,err}
 f,err:=os.CreateTemp(dir,".profiles-*");if err!=nil{return p,err}
 defer os.Remove(f.Name())
 if _,err=f.Write(raw);err!=nil{f.Close();return p,err}
 if err=f.Sync();err!=nil{f.Close();return p,err}
 if err=f.Close();err!=nil{return p,err}
 if err=os.Rename(f.Name(),s.path);err!=nil{return p,err}
 s.people=next;return p,nil
}
func(s *employeeStore)setSavedMaterial(id,ref string,saved bool)([]string,error){
 s.mu.Lock();defer s.mu.Unlock()
 p,ok:=s.people[id];if !ok{return nil,errors.New("profile not found")}
 next:=make([]string,0,len(p.SavedMaterials)+1)
 for _,v:=range p.SavedMaterials{if v!=ref{next=append(next,v)}}
 if saved{if len(next)>=500{return nil,errors.New("saved material limit")};next=append(next,ref)}
 p.SavedMaterials=next
 _,err:=s.putLocked(p);return next,err
}
func(s *employeeStore)recordRecent(id,ref string)([]string,error){
 s.mu.Lock();defer s.mu.Unlock()
 p,ok:=s.people[id];if !ok{return nil,errors.New("profile not found")}
 next:=[]string{ref}
 for _,v:=range p.RecentMaterials{if v!=ref&&len(next)<15{next=append(next,v)}}
 if len(p.RecentMaterials)>0&&p.RecentMaterials[0]==ref{return p.RecentMaterials,nil}
 p.RecentMaterials=next;_,err:=s.putLocked(p);return next,err
}
type employeeAPI struct{
 token string
 chat int64
 store *employeeStore
 client *http.Client
 setupError string
}
func newEmployeeAPI(storage storagePaths)*employeeAPI{
 a:=&employeeAPI{}
 a.token=strings.TrimSpace(os.Getenv("BOT_TOKEN"))
 if a.token==""{a.setupError="Не настроен BOT_TOKEN. Проверьте переменные контейнера.";log.Print("access configuration: BOT_TOKEN missing");return a}
 chat,err:=strconv.ParseInt(strings.TrimSpace(os.Getenv("WORK_CHAT_ID")),10,64)
 if err!=nil||chat>=0{log.Print("access configuration: WORK_CHAT_ID missing or invalid; group access disabled")}else{a.chat=chat}
 dir:=storage.root
 if err=os.MkdirAll(dir,0700);err!=nil{a.setupError="Хранилище профилей недоступно. Проверьте DATA_DIR и права записи.";log.Printf("access configuration: cannot create DATA_DIR: %v",err);return a}
 store,err:=openStore(storage.employees)
 if err!=nil{a.setupError="Не удалось прочитать хранилище профилей. Проверьте DATA_DIR и employees.json.";log.Printf("access configuration: profile store open failed: %v",err);return a}
 probe,err:=os.CreateTemp(dir,".access-check-*")
 if err!=nil{a.setupError="Нет прав записи в хранилище профилей. Проверьте DATA_DIR.";log.Printf("access configuration: DATA_DIR not writable: %v",err);return a}
 probePath:=probe.Name();closeErr:=probe.Close();removeErr:=os.Remove(probePath)
 if closeErr!=nil||removeErr!=nil{a.setupError="Ошибка проверки хранилища профилей.";log.Print("access configuration: storage probe cleanup failed");return a}
 a.store=store
 a.client=&http.Client{Timeout:5*time.Second,CheckRedirect:func(*http.Request,[]*http.Request)error{return http.ErrUseLastResponse}}
 adminCount:=0
 for _,s:=range strings.Split(os.Getenv("ADMIN_USER_IDS"),","){s=strings.TrimSpace(s);if s==""{continue};id,err:=strconv.ParseInt(s,10,64);if err!=nil||id<=0||s!=strconv.FormatInt(id,10){log.Print("access configuration: invalid entry in ADMIN_USER_IDS");continue};adminCount++}
 log.Printf("access configuration: storage ready; DATA_DIR=%s; group configured=%t; admin IDs configured=%d",dir,a.chat<0,adminCount)
 return a
}
func(a *employeeAPI)call(ctx context.Context,method string,params url.Values,out any)error{
 req,err:=http.NewRequestWithContext(ctx,"POST","https://api.telegram.org/bot"+a.token+"/"+method,strings.NewReader(params.Encode()))
 if err!=nil{return errors.New("Telegram request failed")}
 req.Header.Set("Content-Type","application/x-www-form-urlencoded")
 res,err:=a.client.Do(req);if err!=nil{return errors.New("Telegram unavailable")};defer res.Body.Close()
 var envelope struct{OK bool;Result json.RawMessage;ErrorCode int `json:"error_code"`;Parameters struct{MigrateTo int64 `json:"migrate_to_chat_id"`;RetryAfter int `json:"retry_after"`} `json:"parameters"`}
 if err:=json.NewDecoder(io.LimitReader(res.Body,2<<20)).Decode(&envelope);err!=nil{return &telegramAPIError{HTTPStatus:res.StatusCode}}
 if res.StatusCode!=200||!envelope.OK{return &telegramAPIError{HTTPStatus:res.StatusCode,Code:envelope.ErrorCode,MigrateTo:envelope.Parameters.MigrateTo,RetryAfter:envelope.Parameters.RetryAfter}}
 return json.Unmarshal(envelope.Result,out)
}
type telegramAPIError struct{HTTPStatus,Code,RetryAfter int;MigrateTo int64}
func(e *telegramAPIError)Error()string{return "Telegram API request failed (HTTP "+strconv.Itoa(e.HTTPStatus)+", code "+strconv.Itoa(e.Code)+")"}
func(a *employeeAPI)member(ctx context.Context,id int64)(bool,error){
 if a.chat>=0{return false,errors.New("WORK_CHAT_ID missing or invalid")}
 var m struct{Status string;IsMember bool `json:"is_member"`}
 if err:=a.call(ctx,"getChatMember",url.Values{"chat_id":{strconv.FormatInt(a.chat,10)},"user_id":{strconv.FormatInt(id,10)}},&m);err!=nil{return false,err}
 switch m.Status{case "creator","administrator","member":return true,nil;case "restricted":return m.IsMember,nil;case "left","kicked":return false,nil;default:return false,errors.New("Telegram returned unknown membership status")}
}
func photoData(raw []byte)(string,error){
 cfg,kind,err:=image.DecodeConfig(bytes.NewReader(raw));if err!=nil||kind!="jpeg"||cfg.Width<1||cfg.Height<1||cfg.Width>1200||cfg.Height>1200{return "",errors.New("invalid photo")}
 im,err:=jpeg.Decode(bytes.NewReader(raw));if err!=nil{return "",err}
 var buf bytes.Buffer;if err=jpeg.Encode(&buf,im,&jpeg.Options{Quality:90});err!=nil{return "",err}
 if buf.Len()>600000{return "",errors.New("photo too large")}
 return "data:image/jpeg;base64,"+base64.StdEncoding.EncodeToString(buf.Bytes()),nil
}
func(a *employeeAPI)telegramPhoto(ctx context.Context,id int64)string{
 var p struct{Photos [][]struct{FileID string `json:"file_id"`;Width int;Height int}}
 if a.call(ctx,"getUserProfilePhotos",url.Values{"user_id":{strconv.FormatInt(id,10)},"limit":{"1"}},&p)!=nil||len(p.Photos)==0{return ""}
 var selected string;best:=0
 for _,f:=range p.Photos[0]{if f.Width<=1200&&f.Height<=1200&&f.Width*f.Height>best{selected=f.FileID;best=f.Width*f.Height}}
 if selected==""{return ""}
 var file struct{Path string `json:"file_path"`}
 if a.call(ctx,"getFile",url.Values{"file_id":{selected}},&file)!=nil||file.Path==""||strings.ContainsAny(file.Path,"?#\\")||strings.Contains(file.Path,"..")||strings.HasPrefix(file.Path,"/"){return ""}
 req,err:=http.NewRequestWithContext(ctx,"GET","https://api.telegram.org/file/bot"+a.token+"/"+file.Path,nil);if err!=nil{return ""}
 res,err:=a.client.Do(req);if err!=nil{return ""};defer res.Body.Close()
 if res.StatusCode!=200{return ""}
 raw,err:=io.ReadAll(io.LimitReader(res.Body,1000001));if err!=nil||len(raw)>1000000{return ""}
 photo,_:=photoData(raw);return photo
}
func writeJSON(w http.ResponseWriter,status int,v any){w.Header().Set("Content-Type","application/json; charset=utf-8");w.Header().Set("Cache-Control","no-store");w.WriteHeader(status);_ = json.NewEncoder(w).Encode(v)}
func apiError(w http.ResponseWriter,status int,msg string){writeJSON(w,status,map[string]string{"error":msg})}
func(a *employeeAPI)authorize(w http.ResponseWriter,r *http.Request)(tgUser,bool){
 if a.store==nil{msg:=a.setupError;if msg==""{msg="Доступ пока не настроен. Обратитесь к администратору."};apiError(w,503,msg);return tgUser{},false}
 raw:=r.Header.Get("Authorization");if !strings.HasPrefix(raw,"tma "){apiError(w,401,"Откройте приложение через Telegram.");return tgUser{},false}
 u,err:=verifyInitData(strings.TrimPrefix(raw,"tma "),a.token,time.Now());if err!=nil{apiError(w,401,"Сессия Telegram недействительна. Откройте приложение заново.");return tgUser{},false}
 if isAdmin(u.ID){return u,true}
 if a.chat>=0{apiError(w,503,"Рабочая группа не настроена: проверьте WORK_CHAT_ID.");return u,false}
 member,err:=a.member(r.Context(),u.ID);if err!=nil{log.Printf("access check: getChatMember failed: %v",err);var te *telegramAPIError;if errors.As(err,&te)&&te.MigrateTo<0{log.Printf("access check: group migrated; update WORK_CHAT_ID to %d",te.MigrateTo)};apiError(w,503,"Не удалось проверить доступ к рабочему чату.");return u,false}
 if !member{apiError(w,403,"Приложение доступно только участникам рабочего чата.");return u,false}
 return u,true
}
func initialName(u tgUser)string{
 name:=strings.TrimSpace(u.First+" "+u.Last);if name==""{name="Сотрудник"}
 runes:=[]rune(name);if len(runes)>80{name=string(runes[:80])};return name
}
func (a *employeeAPI)me(w http.ResponseWriter,r *http.Request){
 u,ok:=a.authorize(w,r);if !ok{return};id:=strconv.FormatInt(u.ID,10)
 p,exists:=a.store.get(id)
 if !exists{
  p=employee{ID:id,Username:u.Username,Name:initialName(u),Role:"Сотрудник команды",Status:"work",Photo:a.telegramPhoto(r.Context(),u.ID),Saved:[]int{},Read:[]int{},SavedMaterials:[]string{}}
  var err error;p,err=a.store.put(p,true);if err!=nil{apiError(w,500,"Не удалось создать профиль.");return}
 }
 if p.SavedMaterials==nil{p.SavedMaterials=[]string{}}
 writeJSON(w,200,p)
}
func normalizeEmployee(p *employee)error{
 p.Name=strings.TrimSpace(p.Name);p.Role=strings.TrimSpace(p.Role);p.Bio=strings.TrimSpace(p.Bio)
 if p.Name==""||utf8.RuneCountInString(p.Name)>80||utf8.RuneCountInString(p.Role)>100||utf8.RuneCountInString(p.Bio)>600{return errors.New("Проверьте длину полей профиля.")}
 if p.Status!="work"&&p.Status!="vacation"&&p.Status!="away"{return errors.New("Некорректный статус.")}
 if p.Status!="vacation"{p.Until=""}
 if p.Until!=""{if _,err:=time.Parse("2006-01-02",p.Until);err!=nil{return errors.New("Некорректная дата отпуска.")}}
 if p.Project!=""&&p.Project!="calendar"&&p.Project!="industry"{return errors.New("Неизвестный проект.")}
 if p.Photo!=""{
  if !strings.HasPrefix(p.Photo,"data:image/jpeg;base64,")||len(p.Photo)>800100{return errors.New("Некорректное фото.")}
  raw,err:=base64.StdEncoding.DecodeString(strings.TrimPrefix(p.Photo,"data:image/jpeg;base64,"));if err!=nil{return errors.New("Некорректное фото.")}
  p.Photo,err=photoData(raw);if err!=nil{return errors.New("Некорректное фото.")}
 }
 if p.Progress<0||p.Progress>100||p.Progress%10!=0{return errors.New("Некорректный прогресс.")}
 p.Saved=validIDs(p.Saved,8);p.Read=validIDs(p.Read,3);return nil
}
func validIDs(ids []int,max int)[]int{out:=[]int{};seen:=map[int]bool{};for _,id:=range ids{if id>=1&&id<=max&&!seen[id]{out=append(out,id);seen[id]=true}};return out}
func(a *employeeAPI)save(w http.ResponseWriter,r *http.Request){
 u,ok:=a.authorize(w,r);if !ok{return}
 var p employee;dec:=json.NewDecoder(http.MaxBytesReader(w,r.Body,1<<20))
 if dec.Decode(&p)!=nil||dec.Decode(new(any))!=io.EOF{apiError(w,400,"Некорректные данные.");return}
 p.ID=strconv.FormatInt(u.ID,10);p.Username=u.Username
 if _,exists:=a.store.get(p.ID);!exists{apiError(w,409,"Сначала откройте профиль.");return}
 if err:=normalizeEmployee(&p);err!=nil{apiError(w,400,err.Error());return}
 saved,err:=a.store.put(p,false);if err!=nil{apiError(w,500,"Не удалось сохранить профиль.");return}
 writeJSON(w,200,saved)
}
type colleague struct{
 ID string `json:"id"`
 Username string `json:"username,omitempty"`
 Name string `json:"name"`
 Role string `json:"role"`
 Bio string `json:"bio"`
 Status string `json:"status"`
 Until string `json:"until"`
 Photo string `json:"photo"`
 Project string `json:"project"`
}
func(a *employeeAPI)team(w http.ResponseWriter,r *http.Request){
 u,ok:=a.authorize(w,r);if !ok{return}
 people:=a.store.all();sort.Slice(people,func(i,j int)bool{return people[i].Name<people[j].Name})
 result:=make([]colleague,0,len(people))
 // A bounded worker pool avoids an unbounded burst of Telegram requests.
 type check struct{p employee;allowed bool;err error}
 jobs:=make(chan employee);checks:=make(chan check,len(people));var wg sync.WaitGroup
 ctx,cancel:=context.WithTimeout(r.Context(),10*time.Second);defer cancel()
 for i:=0;i<4;i++{wg.Add(1);go func(){defer wg.Done();for p:=range jobs{
  id,err:=strconv.ParseInt(p.ID,10,64);if err!=nil{continue}
  allowed:=id==u.ID||isAdmin(id);if !allowed{allowed,err=a.member(ctx,id)}
  checks<-check{p,allowed,err}
 }}()}
 go func(){defer close(jobs);for _,p:=range people{select{case jobs<-p:case <-ctx.Done():return}}}()
 go func(){wg.Wait();close(checks)}()
 failed:=0
 for c:=range checks{if c.err!=nil{failed++;continue};if !c.allowed{continue};p:=c.p;result=append(result,colleague{p.ID,p.Username,p.Name,p.Role,p.Bio,p.Status,p.Until,p.Photo,p.Project})}
 if ctx.Err()!=nil||failed>0{apiError(w,503,"Не удалось подтвердить полный состав зарегистрированных сотрудников. Повторите проверку.");return}
 sort.Slice(result,func(i,j int)bool{return result[i].Name<result[j].Name})
 writeJSON(w,200,result)
}
func(a *employeeAPI)register(mux *http.ServeMux){
 mux.HandleFunc("GET /api/me",a.me)
 mux.HandleFunc("PUT /api/me",a.save)
 mux.HandleFunc("GET /api/team",a.team)
 // Unknown API paths do not expose the static handler.
 mux.HandleFunc("/api/",func(w http.ResponseWriter,r *http.Request){apiError(w,404,"Не найдено.")})
}
