package main

import (
 "encoding/json"
 "net/http"
 "net/http/httptest"
 "testing"
)

func TestHealth(t *testing.T) {
 r := httptest.NewRecorder()
 routes().ServeHTTP(r, httptest.NewRequest(http.MethodGet, "/api/health", nil))
 var body struct { OK bool `json:"ok"` }
 if r.Code != http.StatusOK || json.Unmarshal(r.Body.Bytes(), &body) != nil || !body.OK { t.Fatalf("health: %d %s", r.Code, r.Body.String()) }
}

func TestRoutes(t *testing.T) {
 for path, want := range map[string]int{"/":200, "/not-found":404} {
  r := httptest.NewRecorder()
  routes().ServeHTTP(r, httptest.NewRequest(http.MethodGet, path, nil))
  if r.Code != want { t.Errorf("%s: got %d want %d", path, r.Code, want) }
 }
}
