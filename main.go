package main

import (
 "context"
 "encoding/json"
 "errors"
 "log"
 "net/http"
 "os"
 "os/signal"
 "syscall"
 "time"
)

func routes() http.Handler {
 mux := http.NewServeMux()
 mux.HandleFunc("GET /api/health", func(w http.ResponseWriter, r *http.Request) {
  w.Header().Set("Content-Type", "application/json")
  w.Header().Set("Cache-Control", "no-store")
  _ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "service": "workhub", "stage": "bootstrap"})
 })
 mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
  w.Header().Set("Content-Type", "text/html; charset=utf-8")
  w.Header().Set("Cache-Control", "no-store")
  _, _ = w.Write([]byte("<!doctype html><html lang='ru'><meta charset='utf-8'><meta name='viewport' content='width=device-width, initial-scale=1'><title>WorkHub</title><h1>WorkHub</h1><p>Тестовый сервер запущен. Интерфейс и Telegram будут подключены отдельно.</p><a href='/api/health'>Проверка сервера</a></html>"))
 })
 return mux
}

func main() {
 port := os.Getenv("PORT")
 if port == "" { port = "8080" }
 srv := &http.Server{Addr: ":" + port, Handler: routes(), ReadHeaderTimeout: 5*time.Second, ReadTimeout: 15*time.Second, WriteTimeout: 15*time.Second, IdleTimeout: 60*time.Second}
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
