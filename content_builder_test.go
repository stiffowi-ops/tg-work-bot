package main

import (
 "bytes"
 "encoding/json"
 "fmt"
 "mime/multipart"
 "net/http"
 "net/http/httptest"
 "os"
 "path/filepath"
 "strings"
 "testing"
 "time"
)

func TestContentBuilderInsertDuplicate(t *testing.T) {
 dir:=t.TempDir()
 c:=&cms{auth:testCMSAuth(t,dir),root:filepath.Join(dir,"projects")}
 mux:=http.NewServeMux();c.register(mux)
 call:=func(method,path,typ string,body *bytes.Buffer,who int64)*httptest.ResponseRecorder{
  r:=httptest.NewRequest(method,path,body)
  r.Header.Set("Authorization","tma "+signedInit(who,time.Now(),c.auth.token))
  r.Header.Set("Content-Type",typ)
  w:=httptest.NewRecorder();mux.ServeHTTP(w,r);return w
 }
 insert:=func(kind,title,before string,rev int,withFile bool)*httptest.ResponseRecorder{
  var body bytes.Buffer;form:=multipart.NewWriter(&body)
  for k,v:=range map[string]string{"kind":kind,"title":title,"before":before,"revision":fmt.Sprint(rev),"cover":"analytics","published":"true","url":"https://example.com/"}{if e:=form.WriteField(k,v);e!=nil{t.Fatal(e)}}
  if withFile{w,e:=form.CreateFormFile("file","sample.txt");if e!=nil{t.Fatal(e)};if _,e=w.Write([]byte("original bytes"));e!=nil{t.Fatal(e)}}
  if e:=form.Close();e!=nil{t.Fatal(e)}
  return call("POST","/api/content/calendar/blocks",form.FormDataContentType(),&body,42)
 }
 decode:=func(w *httptest.ResponseRecorder)projectContent{
  t.Helper();if w.Code!=201{t.Fatalf("status %d: %s",w.Code,w.Body.String())}
  var p projectContent;if e:=json.Unmarshal(w.Body.Bytes(),&p);e!=nil{t.Fatal(e)};return p
 }
 p:=decode(insert("text","First","demo-overview",0,false))
 if p.Blocks[0].Title!="First"{t.Fatal("not inserted first")}
 if w:=insert("text","Stale","demo-overview",0,false);w.Code!=409{t.Fatal(w.Code)}
 if w:=insert("document","Bad anchor","missing",1,true);w.Code!=409{t.Fatal(w.Code)}
 p=decode(insert("document","Document","demo-link",1,true))
 original:=p.Blocks[2];if original.Kind!="document"||original.Cover!="analytics"{t.Fatal(original)}
 path:="/api/content/calendar/blocks/"+original.ID+"/duplicate"
 if w:=call("POST",path,"application/json",bytes.NewBufferString("{\"revision\":2}"),4);w.Code!=403{t.Fatal(w.Code)}
 p=decode(call("POST",path,"application/json",bytes.NewBufferString("{\"revision\":2}"),42))
 duplicate:=p.Blocks[3]
 if duplicate.ID==original.ID||duplicate.Published||duplicate.Cover!=original.Cover{t.Fatal(duplicate)}
 for _,id:=range []string{original.ID,duplicate.ID}{b,e:=os.ReadFile(filepath.Join(c.dir("calendar"),"files",id));if e!=nil||string(b)!="original bytes"{t.Fatal(id,e)}}
 w:=call("DELETE","/api/content/calendar/blocks/"+original.ID,"application/json",bytes.NewBufferString("{\"revision\":3}"),42)
 if w.Code!=200{t.Fatal(w.Code,w.Body.String())}
 if b,e:=os.ReadFile(filepath.Join(c.dir("calendar"),"files",duplicate.ID));e!=nil||string(b)!="original bytes"{t.Fatal(e)}
 reopened,e:=(&cms{root:c.root}).load("calendar");if e!=nil||reopened.Blocks[2].ID!=duplicate.ID{t.Fatal(e,reopened)}
 w=call("GET","/api/content/calendar","application/json",bytes.NewBuffer(nil),4)
 if w.Code!=200||strings.Contains(w.Body.String(),duplicate.ID){t.Fatal(w.Code,w.Body.String())}
}
