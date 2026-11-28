package main

import(
 "encoding/hex"
 "encoding/json"
 "net/http"
 "net/http/httptest"
 "net/url"
 "path/filepath"
 "sort"
 "strings"
 "testing"
 "time"
 "strconv"
)
func signedInit(id int64,when time.Time,token string)string{
 u,_:=json.Marshal(tgUser{ID:id,First:"Тест"})
 q:=url.Values{"auth_date":{strconv.FormatInt(when.Unix(),10)},"user":{string(u)}}
 var pairs []string;for k,v:=range q{pairs=append(pairs,k+"="+v[0])};sort.Strings(pairs)
 q.Set("hash",hex.EncodeToString(hmac256(hmac256([]byte("WebAppData"),[]byte(token)),[]byte(strings.Join(pairs,"\n")))))
 return q.Encode()
}
func TestVerifyTelegram(t *testing.T){
 now:=time.Now();raw:=signedInit(42,now,"test-token")
 u,err:=verifyInitData(raw,"test-token",now);if err!=nil||u.ID!=42{t.Fatal("valid signed user rejected")}
 q,_:=url.ParseQuery(raw);q.Set("user","{\"id\":99}")
 for _,bad:=range []string{"",q.Encode(),raw+"&user=x",signedInit(42,now.Add(-13*time.Hour),"test-token"),signedInit(42,now.Add(time.Hour),"test-token")}{
  if _,err:=verifyInitData(bad,"test-token",now);err==nil{t.Fatal("accepted invalid data")}
 }
}
type telegramMock func(*http.Request)(*http.Response,error)
func(m telegramMock)RoundTrip(r *http.Request)(*http.Response,error){return m(r)}
func TestMemberGateAndPersistence(t *testing.T){
 t.Setenv("ADMIN_USER_IDS","")
 store,err:=openStore(filepath.Join(t.TempDir(),"employees.json"));if err!=nil{t.Fatal(err)}
 member:=true
 a:=&employeeAPI{token:"test-token",chat:-100123,store:store,client:&http.Client{Transport:telegramMock(func(r *http.Request)(*http.Response,error){
  body:="{\"ok\":true,\"result\":{\"photos\":[]}}"
  if strings.HasSuffix(r.URL.Path,"/getChatMember"){if member{body="{\"ok\":true,\"result\":{\"status\":\"member\"}}"}else{body="{\"ok\":true,\"result\":{\"status\":\"left\"}}"}}
  return &http.Response{StatusCode:200,Body:ioNop(body),Header:make(http.Header)},nil
 })}}
 mux:=http.NewServeMux();a.register(mux)
 call:=func(method,path,body,auth string)*httptest.ResponseRecorder{r:=httptest.NewRequest(method,path,strings.NewReader(body));if auth!=""{r.Header.Set("Authorization","tma "+auth)};w:=httptest.NewRecorder();mux.ServeHTTP(w,r);return w}
 raw:=signedInit(42,time.Now(),a.token)
 if w:=call("GET","/api/me","","");w.Code!=401{t.Fatal(w.Code)}
 w:=call("GET","/api/me","",raw);if w.Code!=200{t.Fatal(w.Code,w.Body.String())}
 var p employee;if json.Unmarshal(w.Body.Bytes(),&p)!=nil||p.ID!="42"{t.Fatal("wrong profile")}
 p.ID="99";p.Name="Edited";p.Bio="Description";p.Project="calendar";b,_:=json.Marshal(p)
 if w=call("PUT","/api/me",string(b),raw);w.Code!=200{t.Fatal(w.Code,w.Body.String())}
 if _,exists:=store.get("99");exists{t.Fatal("client changed identity")}
 reopened,err:=openStore(store.path);if err!=nil{t.Fatal(err)};saved,ok:=reopened.get("42");if !ok||saved.Name!="Edited"{t.Fatal("not persisted")}
 if w=call("GET","/api/team","",raw);w.Code!=200||strings.Contains(w.Body.String(),"progress"){t.Fatal(w.Code,w.Body.String())}
 member=false
 for _,path:=range []string{"/api/me","/api/team"}{if w=call("GET",path,"",raw);w.Code!=403{t.Fatal("left user allowed",path,w.Code)}}
 if w=call("PUT","/api/me",string(b),raw);w.Code!=403{t.Fatal("left user can write")}
}
type bodyReader struct{*strings.Reader}
func(bodyReader)Close()error{return nil}
func ioNop(s string)bodyReader{return bodyReader{strings.NewReader(s)}}

func TestConfiguredAdminAccessWithoutGroup(t *testing.T){
 t.Setenv("BOT_TOKEN","test-token")
 t.Setenv("WORK_CHAT_ID","not-configured")
 t.Setenv("ADMIN_USER_IDS","42, 77")
 store,err:=openStore(filepath.Join(t.TempDir(),"employees.json"));if err!=nil{t.Fatal(err)}
 a:=newEmployeeAPI(store)
 if a.store==nil||a.chat!=0{t.Fatal("invalid group must not disable configured admin access")}
 calls:=0
 a.client=&http.Client{Transport:telegramMock(func(r *http.Request)(*http.Response,error){
  calls++
  return &http.Response{StatusCode:400,Body:ioNop("{}"),Header:make(http.Header)},nil
 })}
 m:=http.NewServeMux();a.register(m);c:=&cms{auth:a,root:t.TempDir()};c.register(m)
 call:=func(method,path,raw string)*httptest.ResponseRecorder{
  r:=httptest.NewRequest(method,path,nil);if raw!=""{r.Header.Set("Authorization","tma "+raw)}
  w:=httptest.NewRecorder();m.ServeHTTP(w,r);return w
 }
 admin:=signedInit(42,time.Now(),a.token)
 other:=signedInit(4,time.Now(),a.token)
 if w:=call("GET","/api/me",admin);w.Code!=200{t.Fatal(w.Code,w.Body.String())}
 if w:=call("GET","/api/content/calendar?edit=1",admin);w.Code!=200{t.Fatal(w.Code,w.Body.String())}else{
  cookies:=w.Result().Cookies();if len(cookies)!=1{t.Fatal("media cookie missing")}
  r:=httptest.NewRequest("GET","/api/content/calendar/file/example",nil);r.AddCookie(cookies[0]);w2:=httptest.NewRecorder()
  if u,ok:=c.mediaUser(w2,r);!ok||u.ID!=42{t.Fatal("admin media authorization failed")}
  t.Setenv("ADMIN_USER_IDS","77")
  w2=httptest.NewRecorder();if _,ok:=c.mediaUser(w2,r);ok{t.Fatal("removed admin still has group-independent access")}
  t.Setenv("ADMIN_USER_IDS","42,77")
 }
 if calls!=1{t.Fatal("unexpected Telegram request count (only optional profile photo)",calls)}
 if w:=call("GET","/api/me",other);w.Code!=503{t.Fatal("ordinary employee admitted without group",w.Code)}
 if w:=call("GET","/api/me","");w.Code!=401{t.Fatal("anonymous access",w.Code)}
 forged:=signedInit(42,time.Now(),"wrong-token")
 if w:=call("GET","/api/me",forged);w.Code!=401{t.Fatal("forged admin admitted",w.Code)}
 a.chat=-100123
 if w:=call("GET","/api/me",admin);w.Code!=200{t.Fatal("admin blocked by unavailable Telegram membership check",w.Code)}
 if w:=call("GET","/api/me",other);w.Code!=503{t.Fatal("ordinary employee admitted on Telegram failure",w.Code)}
 t.Setenv("BOT_TOKEN","")
 if a:=newEmployeeAPI(store);a.store!=nil||a.setupError==""{t.Fatal("missing token not rejected")}
}
