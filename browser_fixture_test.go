package main
import (
 "bytes"
 "encoding/json"
 "io/fs"
 "mime/multipart"
 "net/http"
 "net/http/httptest"
 "os"
 "path/filepath"
 "strconv"
 "strings"
 "testing"
 "time"
)
func testCMSAuth(t *testing.T, dir string) *employeeAPI {
 t.Helper()
 t.Setenv("ADMIN_USER_IDS","42")
 s,e:=openStore(filepath.Join(dir,"employees.json"));if e!=nil{t.Fatal(e)}
 return &employeeAPI{token:"browser-test-only",chat:-100123,store:s,client:&http.Client{Transport:telegramMock(func(r *http.Request)(*http.Response,error){
  _=r.ParseForm()
  body:="{\"ok\":true,\"result\":{\"photos\":[]}}"
  if strings.HasSuffix(r.URL.Path,"/getChatMember"){
   status:="member";if r.Form.Get("user_id")=="99"{status="left"}
   body="{\"ok\":true,\"result\":{\"status\":\""+status+"\"}}"
  }
  return &http.Response{StatusCode:200,Body:ioNop(body),Header:make(http.Header)},nil
 })}}
}
func TestCMSFileLifecycle(t *testing.T){
 dir:=t.TempDir();a:=testCMSAuth(t,dir);c:=&cms{auth:a,root:filepath.Join(dir,"projects")};m:=http.NewServeMux();c.register(m)
 request:=func(method,path string,id int64,body []byte,ctype string,ck *http.Cookie,rangeHeader string)*httptest.ResponseRecorder{
  r:=httptest.NewRequest(method,path,bytes.NewReader(body));if id>0{r.Header.Set("Authorization","tma "+signedInit(id,time.Now(),a.token))}
  if ctype!=""{r.Header.Set("Content-Type",ctype)};if ck!=nil{r.AddCookie(ck)};if rangeHeader!=""{r.Header.Set("Range",rangeHeader)}
  w:=httptest.NewRecorder();m.ServeHTTP(w,r);return w
 }
 var buf bytes.Buffer;form:=multipart.NewWriter(&buf)
 for k,v:=range map[string]string{"kind":"document","title":"Test file","cover":"presentation","published":"false"}{_ = form.WriteField(k,v)}
 f,e:=form.CreateFormFile("file","example.pdf");if e!=nil{t.Fatal(e)};payload:=[]byte("%PDF-1.7\nsynthetic test document\n");_,_=f.Write(payload);_ = form.Close()
 endpoint:="/api/content/calendar/blocks"
 if w:=request("POST",endpoint,4,buf.Bytes(),form.FormDataContentType(),nil,"");w.Code!=403{t.Fatal("ordinary employee can upload",w.Code)}
 w:=request("POST",endpoint,42,buf.Bytes(),form.FormDataContentType(),nil,"");if w.Code!=201{t.Fatal(w.Code,w.Body.String())}
 var p projectContent;if e=json.Unmarshal(w.Body.Bytes(),&p);e!=nil{t.Fatal(e)}
 b:=p.Blocks[len(p.Blocks)-1];path:="/api/content/calendar/file/"+b.ID
 if got,e:=os.ReadFile(filepath.Join(c.dir("calendar"),"files",b.ID));e!=nil||!bytes.Equal(got,payload){t.Fatal("file not persisted",e)}
 if w=request("GET",path,4,nil,"",nil,"");w.Code!=404{t.Fatal("draft leaked",w.Code)}
 p.Blocks[len(p.Blocks)-1].Published=true;p.Blocks=append([]contentBlock{p.Blocks[len(p.Blocks)-1]},p.Blocks[:len(p.Blocks)-1]...);raw,_:=json.Marshal(p)
 if w=request("PUT","/api/content/calendar",42,raw,"application/json",nil,"");w.Code!=200{t.Fatal(w.Code,w.Body.String())}
 if w=request("GET",path,0,nil,"",nil,"");w.Code!=401{t.Fatal("public media",w.Code)}
 w=request("GET","/api/content/calendar",4,nil,"",nil,"");cookies:=w.Result().Cookies();if w.Code!=200||len(cookies)!=1||!cookies[0].HttpOnly||!cookies[0].Secure{t.Fatal("session cookie invalid")}
 if w=request("GET",path,0,nil,"",cookies[0],"bytes=0-4");w.Code!=206||w.Body.String()!="%PDF-"{t.Fatal("range",w.Code,w.Body.String())}
 if w=request("GET",path,99,nil,"",nil,"");w.Code!=403{t.Fatal("former member",w.Code)}
 if w=request("GET",path,4,nil,"",nil,"");w.Code!=200||!bytes.Equal(w.Body.Bytes(),payload){t.Fatal("download",w.Code)}
 c2:=&cms{auth:a,root:c.root};q,e:=c2.load("calendar");if e!=nil||q.Blocks[0].ID!=b.ID{t.Fatal("order not persisted",e)}
 q.Blocks=q.Blocks[1:];raw,_=json.Marshal(q)
 if w=request("PUT","/api/content/calendar",42,raw,"application/json",nil,"");w.Code!=200{t.Fatal(w.Code,w.Body.String())}
 if _,e=os.Stat(filepath.Join(c.dir("calendar"),"files",b.ID));!os.IsNotExist(e){t.Fatal("deleted file remains")}
}
func TestBrowserFixture(t *testing.T){
 if os.Getenv("RUN_BROWSER_FIXTURE")!="1"{t.Skip("browser fixture only")}
 dir:=t.TempDir();a:=testCMSAuth(t,dir);m:=http.NewServeMux();a.register(m);(&cms{auth:a,root:filepath.Join(dir,"projects")}).register(m)
 static,e:=fs.Sub(webFiles,"web");if e!=nil{t.Fatal(e)}
 m.Handle("/",http.FileServer(http.FS(static)))
 // Local test-only signed identities. No production route or bypass is added.
 m.HandleFunc("GET /test-init/{id}",func(w http.ResponseWriter,r *http.Request){id,e:=strconv.ParseInt(r.PathValue("id"),10,64);if e!=nil||id<1{http.Error(w,"bad id",400);return};_,_=w.Write([]byte(signedInit(id,time.Now(),a.token)))})
 t.Fatal(http.ListenAndServeTLS("127.0.0.1:18080",os.Getenv("FIXTURE_CERT"),os.Getenv("FIXTURE_KEY"),m))
}
