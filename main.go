package main

import (
 "context"
 "embed"
 "io/fs"
 "encoding/json"
 "errors"
 "log"
 "net/http"
 "os"
 "os/signal"
 "path/filepath"
 "syscall"
 "time"
)

//go:embed web
var webFiles embed.FS

func routes() http.Handler {
 mux := http.NewServeMux()
 mux.HandleFunc("GET /api/health", func(w http.ResponseWriter, r *http.Request) {
  w.Header().Set("Content-Type", "application/json")
  w.Header().Set("Cache-Control", "no-store")
  _ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "service": "workhub", "stage": "employee-access"})
 })
 employee := newEmployeeAPI()
 employee.register(mux)
 dir := os.Getenv("DATA_DIR"); if dir == "" { dir = "/app/runtime" }
 (&cms{auth:employee,root:filepath.Join(dir,"data","projects")}).register(mux)
 static, err := fs.Sub(webFiles, "web")
 if err != nil { panic(err) }
 mux.Handle("GET /", http.HandlerFunc(func(w http.ResponseWriter,r *http.Request){w.Header().Set("Cache-Control","no-cache");w.Header().Set("X-Content-Type-Options","nosniff");http.FileServer(http.FS(static)).ServeHTTP(w,r)}))
 return mux
}

func main() {
 port := os.Getenv("PORT")
 if port == "" { port = "8080" }
 srv := &http.Server{Addr: ":" + port, Handler: routes(), ReadHeaderTimeout: 5*time.Second, ReadTimeout: 10*time.Minute, WriteTimeout: 10*time.Minute, IdleTimeout: 60*time.Second}
 ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
 defer stop()
 go func() {
  <-ctx.Done()
  shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
  defer cancel()
  if err := srv.Shutdown(shutdownCtx); err != nil { log.Printf("shutdown: %v", err) }
 }()
 log.Printf("WorkHub listening on :%s", port)
 if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) { log.Fatal(err) }
}
