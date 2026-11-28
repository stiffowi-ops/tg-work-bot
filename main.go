package main

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
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

const releaseVersion = "V9.6.2-admin-cards"

func routes(dir string) (http.Handler, error) {
	if err := checkStorageDir(dir); err != nil {
		return nil, err
	}
	store, err := openStore(filepath.Join(dir, "employees.json"))
	if err != nil {
		return nil, fmt.Errorf("read profile store in %q: %w", dir, err)
	}
	content := &cms{root: filepath.Join(dir, "data", "projects")}
	for _, id := range projectIDs {
		if err := checkStorageDir(content.dir(id)); err != nil {
			return nil, err
		}
		if err := checkStorageDir(filepath.Join(content.dir(id), "files")); err != nil {
			return nil, err
		}
		if _, err := content.load(id); err != nil {
			return nil, fmt.Errorf("read project %q: %w", id, err)
		}
	}
	log.Printf("storage ready: DATA_DIR=%q", dir)
	employee := newEmployeeAPI(store)
	content.auth = employee
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "service": "workhub", "stage": "employee-access", "version": releaseVersion})
	})
	employee.register(mux)
	content.register(mux)
	static, err := fs.Sub(webFiles, "web")
	if err != nil {
		panic(err)
	}
	mux.Handle("/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" && r.Method != "HEAD" {
			w.Header().Set("Allow", "GET, HEAD")
			http.Error(w, "Method Not Allowed", 405)
			return
		}
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		http.FileServer(http.FS(static)).ServeHTTP(w, r)
	}))
	return mux, nil
}

func main() {
	log.Printf("WorkHub release: %s", releaseVersion)
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	dir, err := dataDirectory()
	if err != nil {
		log.Fatal(err)
	}
	handler, err := routes(dir)
	if err != nil {
		log.Fatalf("storage setup failed (DATA_DIR=%q): %v", dir, err)
	}
	srv := &http.Server{Addr: ":" + port, Handler: handler, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Minute, WriteTimeout: 10 * time.Minute, IdleTimeout: 60 * time.Second}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	shutdownDone := make(chan struct{})
	go func() {
		defer close(shutdownDone)
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			log.Printf("shutdown: %v", err)
		}
	}()
	log.Printf("WorkHub listening on :%s", port)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
	<-shutdownDone
}
