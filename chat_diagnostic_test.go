package main
import("testing";"net/http";"net/http/httptest";"strings";"encoding/json";"path/filepath";"strconv";"time")
func TestExistingGroupMembersFirstLogin(t *testing.T){
 t.Setenv("ADMIN_USER_IDS","")
 store,e:=openStore(filepath.Join(t.TempDir(),"employees.json"));if e!=nil{t.Fatal(e)}
 status:=map[string]string{"10":"creator","11":"administrator","12":"member","13":"restricted","14":"left","15":"kicked","16":"restricted"}
 a:=&employeeAPI{token:"test-token",chat:-1001234567890,store:store,client:&http.Client{Transport:telegramMock(func(r *http.Request)(*http.Response,error){
  _=r.ParseForm();body:="{\"ok\":true,\"result\":{\"photos\":[]}}"
  if strings.HasSuffix(r.URL.Path,"/getChatMember"){
   if r.Form.Get("chat_id")!="-1001234567890"{t.Error("wrong group ID")}
   id:=r.Form.Get("user_id");s:=status[id]
   body="{\"ok\":true,\"result\":{\"status\":"+strconv.Quote(s)+",\"is_member\":"+strconv.FormatBool(id=="13")+"}}"
  }
  return &http.Response{StatusCode:200,Body:ioNop(body),Header:make(http.Header)},nil
 })}}
 m:=http.NewServeMux();a.register(m)
 call:=func(id int64,path string)*httptest.ResponseRecorder{r:=httptest.NewRequest("GET",path,nil);r.Header.Set("Authorization","tma "+signedInit(id,time.Now(),a.token));w:=httptest.NewRecorder();m.ServeHTTP(w,r);return w}
 for _,id:=range []int64{10,11,12,13,14,15,16}{
  if _,ok:=store.get(strconv.FormatInt(id,10));ok{t.Fatal("pre-existing profile")}
  want:=200;if id>=14{want=403}
  if w:=call(id,"/api/me");w.Code!=want{t.Fatal(id,w.Code,w.Body.String())}
 }
 status["12"]="left"
 if w:=call(12,"/api/me");w.Code!=403{t.Fatal("former member admitted",w.Code)}
 w:=call(10,"/api/team");if w.Code!=200||strings.Contains(w.Body.String(),"\"id\":\"12\""){t.Fatal("former member visible",w.Code,w.Body.String())}
 var people []colleague;if json.Unmarshal(w.Body.Bytes(),&people)!=nil||len(people)!=3{t.Fatal("unexpected directory",w.Body.String())}
}
func TestChatDiagnosticAuthorization(t *testing.T){
 a:=testCMSAuth(t,t.TempDir());c:=&cms{auth:a,root:t.TempDir()}
 a.client=&http.Client{Transport:telegramMock(func(r *http.Request)(*http.Response,error){
  _=r.ParseForm();method:=r.URL.Path[strings.LastIndex(r.URL.Path,"/")+1:];body:=""
  switch method{case "getMe":body="{\"id\":7,\"is_bot\":true,\"username\":\"test_bot\"}";case "getChat":body="{\"id\":-100123,\"title\":\"Test group\",\"type\":\"supergroup\"}";case "getChatMemberCount":body="12";case "getChatMember":body="{\"status\":\"administrator\"}"}
  return &http.Response{StatusCode:200,Body:ioNop("{\"ok\":true,\"result\":"+body+"}"),Header:make(http.Header)},nil
 })}
 m:=http.NewServeMux();c.register(m)
 for _,id:=range []int64{4,42}{r:=httptest.NewRequest("GET","/api/admin/chat-diagnostic",nil);r.Header.Set("Authorization","tma "+signedInit(id,time.Now(),a.token));w:=httptest.NewRecorder();m.ServeHTTP(w,r);if id==4{if w.Code!=403{t.Fatal(w.Code)}}else{if w.Code!=200{t.Fatal(w.Code,w.Body.String())};var steps []chatDiagnosticStep;if json.Unmarshal(w.Body.Bytes(),&steps)!=nil||len(steps)!=5{t.Fatal(w.Body.String())};for _,s:=range steps{if !s.OK{t.Fatal(s)}}}}
}
