package main
import("testing";"path/filepath";"os";"strings";"net/http";"net/http/httptest";"time";"encoding/json")
func TestCMSStore(t *testing.T){
 c:=&cms{root:t.TempDir()}
 p,e:=c.load("calendar");if e!=nil||len(p.Blocks)==0{t.Fatal(p,e)}
 p.Title="Edited";p.Revision++;p.Blocks=[]contentBlock{}
 if e=c.save(p);e!=nil{t.Fatal(e)}
 q,e:=c.load("calendar");if e!=nil||q.Title!="Edited"||q.Revision!=1||len(q.Blocks)!=0{t.Fatal(q,e)}
 if _,e=c.load("../outside");e==nil{t.Fatal("traversal accepted")}
 if _,e=os.Stat(filepath.Join(c.root,"calendar","content.json"));e!=nil{t.Fatal(e)}
}
func TestCMSAdminGate(t *testing.T){
 t.Setenv("ADMIN_USER_IDS","42, 77")
 if !isAdmin(42)||!isAdmin(77)||isAdmin(4)||isAdmin(0){t.Fatal("incorrect admin list")}
 store,e:=openStore(filepath.Join(t.TempDir(),"employees.json"));if e!=nil{t.Fatal(e)}
 a:=&employeeAPI{token:"test-token",chat:-100123,store:store,client:&http.Client{Transport:telegramMock(func(r *http.Request)(*http.Response,error){return &http.Response{StatusCode:200,Body:ioNop("{\"ok\":true,\"result\":{\"status\":\"member\"}}"),Header:make(http.Header)},nil})}}
 c:=&cms{auth:a,root:t.TempDir()};mux:=http.NewServeMux();c.register(mux)
 call:=func(method,path,body string,id int64)*httptest.ResponseRecorder{r:=httptest.NewRequest(method,path,strings.NewReader(body));if id>0{r.Header.Set("Authorization","tma "+signedInit(id,time.Now(),a.token))};w:=httptest.NewRecorder();mux.ServeHTTP(w,r);return w}
 if w:=call("GET","/api/content/calendar","",0);w.Code!=401{t.Fatal(w.Code)}
 if w:=call("GET","/api/content/calendar?edit=1","",4);w.Code!=403{t.Fatal(w.Code)}
 if w:=call("PUT","/api/content/calendar","{}",4);w.Code!=403{t.Fatal(w.Code)}
 p,_:=c.load("calendar");p.Title="New title";b,_:=json.Marshal(p)
 if w:=call("PUT","/api/content/calendar",string(b),42);w.Code!=200{t.Fatal(w.Code,w.Body.String())}
 if w:=call("PUT","/api/content/calendar",string(b),42);w.Code!=409{t.Fatal("revision conflict",w.Code)}
 if w:=call("GET","/api/content/calendar/file/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","",0);w.Code!=401{t.Fatal(w.Code)}
}
func TestCMSValidation(t *testing.T){
 if validLink("javascript:alert(1)")||validLink("https://a@b.com")||!validLink("https://yandex.ru/routing/"){t.Fatal("bad link validation")}
 if _,ok:=uploadType("image","evil.svg",[]byte("<svg/>"));ok{t.Fatal("SVG allowed")}
 if _,ok:=uploadType("document","sample.pdf",[]byte("%PDF-1.7\n"));!ok{t.Fatal("PDF rejected")}
}
