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

func TestCMSDeleteBlock(t *testing.T) {
	dir := t.TempDir()
	c := &cms{auth: testCMSAuth(t, dir), root: filepath.Join(dir, "projects")}
	mux := http.NewServeMux()
	c.register(mux)
	p, err := c.load("industry")
	if err != nil {
		t.Fatal(err)
	}
	id := strings.Repeat("a", 32)
	p.Blocks = []contentBlock{{ID: id, Kind: "document", Title: "Материал", Filename: "sample.txt", MIME: "text/plain", Published: true}}
	if err := c.save(p); err != nil {
		t.Fatal(err)
	}
	files := filepath.Join(c.dir(p.ID), "files")
	if err := os.MkdirAll(files, 0700); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(files, id)
	if err := os.WriteFile(file, []byte("document"), 0600); err != nil {
		t.Fatal(err)
	}
	call := func(user int64, path, body string, want int) {
		t.Helper()
		r := httptest.NewRequest(http.MethodDelete, path, strings.NewReader(body))
		if user != 0 {
			r.Header.Set("Authorization", "tma "+signedInit(user, time.Now(), c.auth.token))
		}
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, r)
		if w.Code != want {
			t.Fatalf("%s: got %d, want %d: %s", path, w.Code, want, w.Body.String())
		}
	}
	path := "/api/content/industry/blocks/"
	const revision = `{"revision":0}`
	call(0, path+id, revision, 401)
	call(4, path+id, revision, 403)
	call(42, path+id, "{}", 400)
	call(42, path+id, `{"revision":1}`, 409)
	call(42, path+"unknown", revision, 404)
	call(42, "/api/content/calendar/blocks/"+id, revision, 404)
	if _, err := os.Stat(file); err != nil {
		t.Fatal(err)
	}
	call(42, path+id, revision, 200)
	if _, err := os.Stat(file); !os.IsNotExist(err) {
		t.Fatal("file remains", err)
	}
	reopened := &cms{root: c.root}
	saved, err := reopened.load("industry")
	if err != nil || saved.Revision != 1 || len(saved.Blocks) != 0 {
		t.Fatal(saved, err)
	}
	call(42, path+id, revision, 409)
	call(42, path+id, `{"revision":1}`, 404)
}
