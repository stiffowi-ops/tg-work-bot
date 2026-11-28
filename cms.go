package main

import (
 "crypto/rand"
 "crypto/hmac"
 "encoding/hex"
 "encoding/json"
 "errors"
 "fmt"
 "io"
 "mime"
 "net/http"
 "net/url"
 "os"
 "path/filepath"
 "strconv"
 "strings"
 "sync"
 "time"
 "unicode/utf8"
)

type contentBlock struct {
 ID string `json:"id"`
 Kind string `json:"kind"`
 Title string `json:"title"`
 Text string `json:"text"`
 URL string `json:"url"`
 Cover string `json:"cover"`
 LinkStyle string `json:"linkStyle,omitempty"`
 Filename string `json:"filename,omitempty"`
 MIME string `json:"mime,omitempty"`
 Published bool `json:"published"`
}
type projectContent struct {
 ID string `json:"id"`
 Title string `json:"title"`
 Description string `json:"description"`
 Revision int64 `json:"revision"`
 Blocks []contentBlock `json:"blocks"`
}
type cms struct {
 auth *employeeAPI
 root string
 mu sync.Mutex
}
func isAdmin(id int64)bool{
 if id<=0{return false}
 for _,s:=range strings.Split(os.Getenv("ADMIN_USER_IDS"),","){if strings.TrimSpace(s)==strconv.FormatInt(id,10){return true}}
 return false
}
func projectSeed(id string)(projectContent,bool){
 switch id{
 case "calendar":return projectContent{ID:id,Title:"Календарное планирование",Description:"Планирование работы полевых сотрудников",Blocks:[]contentBlock{
 {ID:"demo-overview",Kind:"text",Title:"О проекте",Text:"Календарное планирование помогает организовать визиты выездных сотрудников, распределять задачи и учитывать рабочие ограничения. Демонстрационное описание: администратор может заменить его или удалить.",Published:true},
 {ID:"demo-link",Kind:"link",Title:"Сайт продукта",URL:"https://yandex.ru/routing/",Cover:"route",Published:true},
 {ID:"demo-faq",Kind:"faq",Title:"Где найти материалы проекта?",Text:"Материалы появятся в этой ленте после публикации администратором.",Published:true},
 }},true
 case "industry":return projectContent{ID:id,Title:"Отраслевое деление",Description:"Материалы по отраслям и эксперты",Blocks:[]contentBlock{}},true
 }
 return projectContent{},false
}
func (c *cms) dir(id string)string{return filepath.Join(c.root,id)}
func (c *cms) load(id string)(projectContent,error){
 p,ok:=projectSeed(id);if !ok{return p,errors.New("unknown project")}
 raw,err:=os.ReadFile(filepath.Join(c.dir(id),"content.json"))
 if errors.Is(err,os.ErrNotExist){return p,nil}
 if err!=nil{return p,err}
 if err=json.Unmarshal(raw,&p);err!=nil||p.ID!=id||p.Blocks==nil{return p,errors.New("invalid project data")}
 return p,nil
}
func (c *cms) save(p projectContent)error{
 dir:=c.dir(p.ID);if err:=os.MkdirAll(dir,0700);err!=nil{return err}
 raw,err:=json.Marshal(p);if err!=nil{return err}
 f,err:=os.CreateTemp(dir,".content-");if err!=nil{return err}
 defer os.Remove(f.Name())
 if _,err=f.Write(raw);err!=nil{f.Close();return err}
 if err=f.Sync();err!=nil{f.Close();return err};if err=f.Close();err!=nil{return err}
 return os.Rename(f.Name(),filepath.Join(dir,"content.json"))
}
func cleanLength(s string,n int)bool{return utf8.ValidString(s)&&utf8.RuneCountInString(s)<=n}
func validLink(s string)bool{u,err:=url.Parse(s);return err==nil&&u.Scheme=="https"&&u.Hostname()!=""&&u.User==nil}
func fileKind(k string)bool{return k=="document"||k=="video"||k=="image"}
func validBlock(b contentBlock)bool{
 if strings.TrimSpace(b.Title)==""||!cleanLength(b.Title,140)||!cleanLength(b.Text,12000)||!cleanLength(b.URL,2048)||!cleanLength(b.Cover,40){return false}
 switch b.Kind{case "text","faq","section":return true;case "link":return validLink(b.URL)&&(b.LinkStyle==""||b.LinkStyle=="compact"||b.LinkStyle=="wide"||b.LinkStyle=="minimal");case "document","video","image":return b.Filename!=""&&b.MIME!="";default:return false}
}
func randomID()(string,error){b:=make([]byte,16);_,err:=rand.Read(b);return hex.EncodeToString(b),err}
func validFileID(id string)bool{b,err:=hex.DecodeString(id);return err==nil&&len(b)==16}
func (c *cms) admin(w http.ResponseWriter,r *http.Request)(tgUser,bool){
 u,ok:=c.auth.authorize(w,r);if !ok{return u,false}
 if !isAdmin(u.ID){apiError(w,403,"Доступно только администраторам.");return u,false};return u,true
}
const mediaCookie="__Host-routing-media"
func (c *cms) setMediaCookie(w http.ResponseWriter,id int64){
 exp:=time.Now().Add(45*time.Minute).Unix()
 val:=fmt.Sprintf("%d.%d",id,exp);sig:=hex.EncodeToString(hmac256([]byte(c.auth.token),[]byte("media-v1:"+val)))
 http.SetCookie(w,&http.Cookie{Name:mediaCookie,Value:val+"."+sig,Path:"/",Secure:true,HttpOnly:true,SameSite:http.SameSiteLaxMode,MaxAge:2700})
}
func (c *cms) mediaUser(w http.ResponseWriter,r *http.Request)(tgUser,bool){
 if strings.HasPrefix(r.Header.Get("Authorization"),"tma "){return c.auth.authorize(w,r)}
 deny:=func()(tgUser,bool){apiError(w,401,"Откройте проект заново в Telegram.");return tgUser{},false}
 if c.auth.store==nil||c.auth.token==""{return deny()}
 ck,err:=r.Cookie(mediaCookie);if err!=nil{return deny()}
 p:=strings.Split(ck.Value,".");if len(p)!=3{return deny()}
 sig,err:=hex.DecodeString(p[2]);if err!=nil||!hmac.Equal(sig,hmac256([]byte(c.auth.token),[]byte("media-v1:"+p[0]+"."+p[1]))){return deny()}
 id,e1:=strconv.ParseInt(p[0],10,64);exp,e2:=strconv.ParseInt(p[1],10,64)
 if e1!=nil||e2!=nil||id<=0||exp<time.Now().Unix(){return deny()}
 if isAdmin(id){return tgUser{ID:id},true}
 if c.auth.chat>=0{apiError(w,503,"Рабочая группа не настроена: проверьте WORK_CHAT_ID.");return tgUser{},false}
 member,err:=c.auth.member(r.Context(),id);if err!=nil{apiError(w,503,"Не удалось проверить доступ.");return tgUser{},false}
 if !member{apiError(w,403,"Доступ закрыт.");return tgUser{},false}
 return tgUser{ID:id},true
}
type materialSummary struct{
 ID string `json:"id"`
 Project string `json:"project"`
 Title string `json:"title"`
 Kind string `json:"kind"`
}
func (c *cms) setSavedMaterial(w http.ResponseWriter,r *http.Request){
 u,ok:=c.auth.authorize(w,r);if !ok{return}
 var body struct{Project string;ID string;Saved *bool}
 dec:=json.NewDecoder(http.MaxBytesReader(w,r.Body,4096))
 if dec.Decode(&body)!=nil||dec.Decode(new(any))!=io.EOF||body.Saved==nil||body.ID==""||len(body.ID)>80||strings.ContainsAny(body.ID,":/"){apiError(w,400,"Некорректный материал.");return}
 if _,ok:=projectSeed(body.Project);!ok{apiError(w,404,"Проект не найден.");return}
 if *body.Saved{
  c.mu.Lock();p,err:=c.load(body.Project);found:=false
  if err==nil{for _,b:=range p.Blocks{if b.ID==body.ID&&b.Published{found=true;break}}}
  c.mu.Unlock()
  if err!=nil{apiError(w,500,"Не удалось загрузить проект.");return}
  if !found{apiError(w,404,"Материал не опубликован или удалён.");return}
 }
 refs,err:=c.auth.store.setSavedMaterial(strconv.FormatInt(u.ID,10),body.Project+":"+body.ID,*body.Saved)
 if err!=nil{apiError(w,409,"Не удалось изменить сохранённые материалы. Проверьте профиль и лимит 500 материалов.");return}
 writeJSON(w,200,map[string]any{"savedMaterials":refs})
}
func (c *cms) index(w http.ResponseWriter,r *http.Request){
 u,ok:=c.auth.authorize(w,r);if !ok{return}
 c.mu.Lock();defer c.mu.Unlock()
 list:=[]projectContent{};materials:=[]materialSummary{}
 for _,id:=range []string{"calendar","industry"}{p,err:=c.load(id);if err!=nil{apiError(w,500,"Не удалось загрузить проекты.");return};for _,b:=range p.Blocks{if b.Published{materials=append(materials,materialSummary{b.ID,id,b.Title,b.Kind})}};p.Blocks=nil;list=append(list,p)}
 writeJSON(w,200,map[string]any{"admin":isAdmin(u.ID),"projects":list,"materials":materials})
}
func (c *cms) refreshMediaSession(w http.ResponseWriter,r *http.Request){
 u,ok:=c.auth.authorize(w,r);if !ok{return}
 c.setMediaCookie(w,u.ID);writeJSON(w,200,map[string]bool{"ok":true})
}
func (c *cms) get(w http.ResponseWriter,r *http.Request){
 u,ok:=c.auth.authorize(w,r);if !ok{return}
 id:=r.PathValue("project");if _,ok:=projectSeed(id);!ok{apiError(w,404,"Проект не найден.");return}
 edit:=r.URL.Query().Get("edit")=="1";if edit&&!isAdmin(u.ID){apiError(w,403,"Доступно только администраторам.");return}
 c.mu.Lock();p,err:=c.load(id);c.mu.Unlock()
 if err!=nil{apiError(w,500,"Не удалось загрузить проект.");return}
 if !edit{blocks:=[]contentBlock{};for _,b:=range p.Blocks{if b.Published{blocks=append(blocks,b)}};p.Blocks=blocks}
 c.setMediaCookie(w,u.ID);writeJSON(w,200,p)
}
func (c *cms) update(w http.ResponseWriter,r *http.Request){
 if _,ok:=c.admin(w,r);!ok{return}
 id:=r.PathValue("project");if _,ok:=projectSeed(id);!ok{apiError(w,404,"Проект не найден.");return}
 var next projectContent
 dec:=json.NewDecoder(http.MaxBytesReader(w,r.Body,2<<20))
 if dec.Decode(&next)!=nil||dec.Decode(new(any))!=io.EOF||next.ID!=id||strings.TrimSpace(next.Title)==""||!cleanLength(next.Title,120)||!cleanLength(next.Description,2000)||next.Blocks==nil||len(next.Blocks)>200{apiError(w,400,"Некорректные данные проекта.");return}
 c.mu.Lock();defer c.mu.Unlock();old,err:=c.load(id)
 if err!=nil{apiError(w,500,"Ошибка хранения.");return}
 if next.Revision!=old.Revision{apiError(w,409,"Проект был изменён. Обновите редактор.");return}
 existing:=map[string]contentBlock{};for _,b:=range old.Blocks{existing[b.ID]=b}
 seen:=map[string]bool{}
 for i,b:=range next.Blocks{
  prev,ok:=existing[b.ID];if !ok||seen[b.ID]||b.Kind!=prev.Kind{apiError(w,400,"Некорректный блок.");return}
  seen[b.ID]=true;b.Filename=prev.Filename;b.MIME=prev.MIME
  if !validBlock(b){apiError(w,400,"Проверьте содержимое блока.");return};next.Blocks[i]=b
 }
 next.Revision++
 if err=c.save(next);err!=nil{apiError(w,500,"Не удалось сохранить проект.");return}
 // Remove only files referenced by deleted blocks, after committing metadata.
 for _,b:=range old.Blocks{if !seen[b.ID]{c.removeBlockFile(id,b)}}
 writeJSON(w,200,next)
}
func (c *cms) removeBlockFile(project string, b contentBlock) {
	if fileKind(b.Kind) && validFileID(b.ID) {
		_ = os.Remove(filepath.Join(c.dir(project), "files", b.ID))
	}
}

func (c *cms) deleteBlock(w http.ResponseWriter, r *http.Request) {
	if _, ok := c.admin(w, r); !ok {
		return
	}
	project := r.PathValue("project")
	if _, ok := projectSeed(project); !ok {
		apiError(w, 404, "Проект не найден.")
		return
	}
	var body struct{ Revision *int64 }
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1024))
	if dec.Decode(&body) != nil || dec.Decode(new(any)) != io.EOF || body.Revision == nil {
		apiError(w, 400, "Укажите версию проекта.")
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	p, err := c.load(project)
	if err != nil {
		apiError(w, 500, "Не удалось загрузить проект.")
		return
	}
	if p.Revision != *body.Revision {
		apiError(w, 409, "Проект изменился. Откройте редактор заново и проверьте список.")
		return
	}
	for i, b := range p.Blocks {
		if b.ID != r.PathValue("block") {
			continue
		}
		p.Blocks = append(p.Blocks[:i], p.Blocks[i+1:]...)
		p.Revision++
		if err := c.save(p); err != nil {
			apiError(w, 500, "Не удалось удалить материал.")
			return
		}
		c.removeBlockFile(project, b)
		writeJSON(w, 200, p)
		return
	}
	apiError(w, 404, "Материал не найден.")
}

func uploadType(kind,name string,head []byte)(string,bool){
 ext:=strings.ToLower(filepath.Ext(name));det:=http.DetectContentType(head)
 switch kind{
 case "video":
  if ext==".mp4"&&det=="video/mp4"{return "video/mp4",true};if ext==".webm"&&det=="video/webm"{return "video/webm",true}
 case "image":
  if ext==".png"&&det=="image/png"|| (ext==".jpg"||ext==".jpeg")&&det=="image/jpeg"||ext==".webp"&&det=="image/webp"{return det,true}
 case "document":
  if ext==".pdf"&&det=="application/pdf"{return det,true}
  if ext==".docx"||ext==".pptx"||ext==".xlsx"{if len(head)>=4&&string(head[:4])=="PK\x03\x04"{return "application/octet-stream",true}}
  if ext==".txt"&&strings.HasPrefix(det,"text/plain"){return "text/plain; charset=utf-8",true}
 }
 return "",false
}
func (c *cms) add(w http.ResponseWriter,r *http.Request){
 if _,ok:=c.admin(w,r);!ok{return}
 id:=r.PathValue("project");if _,ok:=projectSeed(id);!ok{apiError(w,404,"Проект не найден.");return}
 r.Body=http.MaxBytesReader(w,r.Body,130<<20)
 if err:=r.ParseMultipartForm(1<<20);err!=nil{apiError(w,400,"Не удалось прочитать загрузку (лимит 128 МБ).");return}
 defer r.MultipartForm.RemoveAll()
 bid,err:=randomID();if err!=nil{apiError(w,500,"Ошибка создания блока.");return}
 b:=contentBlock{ID:bid,Kind:r.FormValue("kind"),Title:strings.TrimSpace(r.FormValue("title")),Text:r.FormValue("text"),URL:r.FormValue("url"),Cover:r.FormValue("cover"),LinkStyle:r.FormValue("linkStyle"),Published:r.FormValue("published")=="true"}
 var dest string
 if fileKind(b.Kind){
  f,header,e:=r.FormFile("file");if e!=nil{apiError(w,400,"Выберите файл.");return};defer f.Close()
  if header.Size<=0||header.Size>128<<20{apiError(w,400,"Файл должен быть не больше 128 МБ.");return}
  b.Filename=filepath.Base(strings.ReplaceAll(header.Filename,"\\","/"))
  if !cleanLength(b.Filename,250){apiError(w,400,"Некорректное имя файла.");return}
  head:=make([]byte,512);n,e:=io.ReadFull(f,head);if e!=nil&&e!=io.ErrUnexpectedEOF{apiError(w,400,"Не удалось прочитать файл.");return}
  var good bool;b.MIME,good=uploadType(b.Kind,b.Filename,head[:n]);if !good{apiError(w,400,"Формат файла не поддерживается.");return}
  if _,e=f.Seek(0,io.SeekStart);e!=nil{apiError(w,400,"Ошибка чтения файла.");return}
  if !validBlock(b){apiError(w,400,"Проверьте поля материала.");return}
  dir:=filepath.Join(c.dir(id),"files");if e=os.MkdirAll(dir,0700);e!=nil{apiError(w,500,"Не удалось создать папку.");return}
  dest=filepath.Join(dir,b.ID);out,e:=os.OpenFile(dest,os.O_CREATE|os.O_EXCL|os.O_WRONLY,0600);if e!=nil{apiError(w,500,"Не удалось создать файл.");return}
  count,copyErr:=io.Copy(out,io.LimitReader(f,(128<<20)+1));syncErr:=out.Sync();closeErr:=out.Close()
  if copyErr!=nil||syncErr!=nil||closeErr!=nil||count!=header.Size{_ = os.Remove(dest);apiError(w,500,"Ошибка сохранения файла.");return}
 }
 if !validBlock(b){if dest!=""{_ = os.Remove(dest)};apiError(w,400,"Проверьте поля материала.");return}
 c.mu.Lock();defer c.mu.Unlock();p,err:=c.load(id)
 if err==nil{
  if rev:=r.FormValue("revision");rev!=""{
   n,e:=strconv.ParseInt(rev,10,64);if e!=nil||n!=p.Revision{if dest!=""{_ = os.Remove(dest)};apiError(w,409,"Проект изменился. Откройте редактор заново.");return}
  }
  pos:=len(p.Blocks)
  if before:=r.FormValue("before");before!=""{
   pos=-1;for i,old:=range p.Blocks{if old.ID==before{pos=i;break}}
   if pos<0{if dest!=""{_ = os.Remove(dest)};apiError(w,409,"Место вставки изменилось.");return}
  }
  if len(p.Blocks)>=200{if dest!=""{_ = os.Remove(dest)};apiError(w,400,"В проекте не более 200 блоков.");return}
  p.Blocks=append(p.Blocks,contentBlock{});copy(p.Blocks[pos+1:],p.Blocks[pos:]);p.Blocks[pos]=b;p.Revision++;err=c.save(p)
 }
 if err!=nil{if dest!=""{_ = os.Remove(dest)};apiError(w,500,"Не удалось сохранить материал.");return}
 writeJSON(w,201,p)
}
// Duplicate into a new draft, with an independent file for file-backed blocks.
func (c *cms) duplicateBlock(w http.ResponseWriter,r *http.Request){
 if _,ok:=c.admin(w,r);!ok{return}
 id:=r.PathValue("project");if _,ok:=projectSeed(id);!ok{apiError(w,404,"Проект не найден.");return}
 var body struct{Revision *int64}
 dec:=json.NewDecoder(http.MaxBytesReader(w,r.Body,1024))
 if dec.Decode(&body)!=nil||dec.Decode(new(any))!=io.EOF||body.Revision==nil{apiError(w,400,"Укажите версию проекта.");return}
 c.mu.Lock();defer c.mu.Unlock()
 p,err:=c.load(id);if err!=nil{apiError(w,500,"Ошибка хранения.");return}
 if p.Revision!=*body.Revision{apiError(w,409,"Проект изменился. Откройте редактор заново.");return}
 if len(p.Blocks)>=200{apiError(w,400,"В проекте не более 200 блоков.");return}
 for i,old:=range p.Blocks{
  if old.ID!=r.PathValue("block"){continue}
  b:=old;b.ID,err=randomID();b.Published=false
  if err!=nil{apiError(w,500,"Ошибка создания блока.");return}
  var dest string
  if fileKind(b.Kind){
   if !validFileID(old.ID){apiError(w,400,"Некорректный файл.");return}
   dir:=filepath.Join(c.dir(id),"files")
   src,e:=os.Open(filepath.Join(dir,old.ID));if e!=nil{apiError(w,500,"Не удалось открыть исходный файл.");return}
   defer src.Close()
   dest=filepath.Join(dir,b.ID)
   out,e:=os.OpenFile(dest,os.O_CREATE|os.O_EXCL|os.O_WRONLY,0600);if e!=nil{apiError(w,500,"Не удалось создать копию файла.");return}
   n,copyErr:=io.Copy(out,io.LimitReader(src,(128<<20)+1));syncErr:=out.Sync();closeErr:=out.Close()
   if n<=0||n>128<<20||copyErr!=nil||syncErr!=nil||closeErr!=nil{_ = os.Remove(dest);apiError(w,500,"Не удалось скопировать файл.");return}
  }
  p.Blocks=append(p.Blocks,contentBlock{});copy(p.Blocks[i+2:],p.Blocks[i+1:]);p.Blocks[i+1]=b;p.Revision++
  if err=c.save(p);err!=nil{if dest!=""{_ = os.Remove(dest)};apiError(w,500,"Не удалось сохранить копию.");return}
  writeJSON(w,201,p);return
 }
 apiError(w,404,"Материал не найден.")
}
func (c *cms) media(w http.ResponseWriter,r *http.Request){
 u,ok:=c.mediaUser(w,r);if !ok{return}
 id,bid:=r.PathValue("project"),r.PathValue("block")
 if _,ok:=projectSeed(id);!ok||!validFileID(bid){apiError(w,404,"Файл не найден.");return}
 c.mu.Lock();p,err:=c.load(id);var b contentBlock
 if err==nil{for _,v:=range p.Blocks{if v.ID==bid&&(v.Published||isAdmin(u.ID))&&fileKind(v.Kind){b=v;break}}}
 if b.ID==""{c.mu.Unlock();apiError(w,404,"Файл не найден.");return}
 f,err:=os.Open(filepath.Join(c.dir(id),"files",bid));c.mu.Unlock()
 if err!=nil{apiError(w,404,"Файл не найден.");return};defer f.Close()
 st,err:=f.Stat();if err!=nil{apiError(w,500,"Ошибка чтения файла.");return}
 w.Header().Set("Cache-Control","private, no-store")
 w.Header().Set("X-Content-Type-Options","nosniff")
 w.Header().Set("Content-Type",b.MIME)
 disposition:="inline";if b.Kind=="document"{disposition="attachment"}
 w.Header().Set("Content-Disposition",mime.FormatMediaType(disposition,map[string]string{"filename":b.Filename}))
 // ServeContent provides byte ranges for video seeking.
 http.ServeContent(w,r,b.Filename,st.ModTime(),f)
}
func (c *cms) register(mux *http.ServeMux){
 mux.HandleFunc("GET /api/admin/chat-diagnostic",c.chatDiagnostic)
 mux.HandleFunc("GET /api/content",c.index)
 mux.HandleFunc("PUT /api/me/materials",c.setSavedMaterial)
 mux.HandleFunc("POST /api/content/media-session",c.refreshMediaSession)
 mux.HandleFunc("GET /api/content/{project}",c.get)
 mux.HandleFunc("PUT /api/content/{project}",c.update)
 mux.HandleFunc("POST /api/content/{project}/blocks",c.add)
 mux.HandleFunc("DELETE /api/content/{project}/blocks/{block}",c.deleteBlock)
 mux.HandleFunc("POST /api/content/{project}/blocks/{block}/duplicate",c.duplicateBlock)
 mux.HandleFunc("GET /api/content/{project}/file/{block}",c.media)
}
