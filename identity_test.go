package main
import("encoding/json";"net/http";"net/http/httptest";"strings";"testing";"time")
func TestIdentityPersistence(t *testing.T){
 a:=testCMSAuth(t,t.TempDir());m:=http.NewServeMux();a.register(m)
 call:=func(id int64,method,path,body string)*httptest.ResponseRecorder{r:=httptest.NewRequest(method,path,strings.NewReader(body));r.Header.Set("Authorization","tma "+signedInit(id,time.Now(),a.token));w:=httptest.NewRecorder();m.ServeHTTP(w,r);return w}
 for _,id:=range []int64{4,5,42}{if w:=call(id,"GET","/api/me","");w.Code!=200{t.Fatal(w.Code,w.Body.String())}}
 if w:=call(4,"PUT","/api/admin/identity",`{"id":"4","action":"leader"}`);w.Code!=403{t.Fatal(w.Code)}
 for _,body:=range []string{`{"id":"4","action":"start","startMonth":"2020-01"}`,`{"id":"4","action":"leader"}`,`{"id":"4","action":"leader"}`,`{"id":"4","action":"expert"}`}{if w:=call(42,"PUT","/api/admin/identity",body);w.Code!=200{t.Fatal(w.Code,w.Body.String())}}
 p,_:=a.store.get("4");if len(p.Identity.LeaderMonths)!=1||len(p.Identity.ExpertDates)!=1{t.Fatal("duplicate award")}
 p.Identity=employeeIdentity{};p.Name="Edited";raw,_:=json.Marshal(p)
 if w:=call(4,"PUT","/api/me",string(raw));w.Code!=200{t.Fatal(w.Code,w.Body.String())}
 if w:=call(42,"PUT","/api/admin/identity",`{"id":"5","action":"leader"}`);w.Code!=200{t.Fatal(w.Code,w.Body.String())}
 s,e:=openStore(a.store.path);if e!=nil{t.Fatal(e)}
 old,_:=s.get("4");current,_:=s.get("5")
 if old.Identity.StartMonth!="2020-01"||len(old.Identity.ExpertDates)!=1||len(old.Identity.LeaderMonths)!=0||len(current.Identity.LeaderMonths)!=1{t.Fatal("identity not persisted")}
 now:=time.Date(2026,11,1,0,0,0,0,identityZone);if identityYears("2025-12",now)!=0||identityYears("2025-11",now)!=1{t.Fatal("incorrect tenure")}
}
