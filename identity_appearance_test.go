package main
import("encoding/json";"net/http";"net/http/httptest";"strings";"testing";"time")
func TestIdentityAppearance(t *testing.T){
 a:=testCMSAuth(t,t.TempDir());m:=http.NewServeMux();a.register(m)
 call:=func(id int64,method,path,body string)*httptest.ResponseRecorder{r:=httptest.NewRequest(method,path,strings.NewReader(body));r.Header.Set("Authorization","tma "+signedInit(id,time.Now(),a.token));w:=httptest.NewRecorder();m.ServeHTTP(w,r);return w}
 for _,id:=range []int64{4,5,42}{if w:=call(id,"GET","/api/me","");w.Code!=200{t.Fatal(w.Code,w.Body.String())}}
 if w:=call(4,"PUT","/api/me/appearance",`{"appearance":"expert"}`);w.Code!=403{t.Fatal("unearned appearance",w.Code)}
 for _,body:=range []string{`{"id":"4","action":"start","startMonth":"2020-01"}`,`{"id":"4","action":"leader"}`,`{"id":"4","action":"expert"}`}{if w:=call(42,"PUT","/api/admin/identity",body);w.Code!=200{t.Fatal(w.Code,w.Body.String())}}
 if w:=call(4,"PUT","/api/me/appearance",`{"appearance":"expert"}`);w.Code!=200{t.Fatal(w.Code,w.Body.String())}
 if w:=call(5,"GET","/api/team","");w.Code!=200||!strings.Contains(w.Body.String(),`"appearance":"expert"`){t.Fatal(w.Code,w.Body.String())}
 p,_:=a.store.get("4");p.Identity=employeeIdentity{};p.Name="Edited";raw,_:=json.Marshal(p)
 if w:=call(4,"PUT","/api/me",string(raw));w.Code!=200{t.Fatal(w.Code,w.Body.String())}
 s,e:=openStore(a.store.path);if e!=nil{t.Fatal(e)}
 old,_:=s.get("4");if old.Identity.SelectedAppearance!="expert"||len(old.Identity.LeaderMonths)!=1{t.Fatal("appearance or awards lost")}
 now:=time.Date(2026,11,1,0,0,0,0,identityZone)
 for _,x:=range []struct{start string;want int}{{"2026-08",3},{"2026-05",6},{"2025-11",12},{"2024-11",24},{"2023-11",36},{"",0},{"2027-01",0}}{if got:=identityMonths(x.start,now);got!=x.want{t.Fatal(x,got)}}
 if w:=call(4,"PUT","/api/me/appearance",`{"appearance":"none"}`);w.Code!=200{t.Fatal(w.Code)}
 if w:=call(42,"PUT","/api/admin/identity",`{"id":"4","action":"expert"}`);w.Code!=200{t.Fatal(w.Code)}
 if w:=call(4,"GET","/api/me","");w.Code!=200||!strings.Contains(w.Body.String(),`"appearance":"none"`){t.Fatal("new award changed selection",w.Code,w.Body.String())}
}
