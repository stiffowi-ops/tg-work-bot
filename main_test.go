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

func TestReleaseVersion(t *testing.T) {
 r := httptest.NewRecorder()
 routes().ServeHTTP(r, httptest.NewRequest(http.MethodGet, "/api/health", nil))
 var body struct { Version string `json:"version"` }
 if r.Code != 200 || json.Unmarshal(r.Body.Bytes(), &body) != nil || body.Version != releaseVersion { t.Fatalf("release marker: %d %s", r.Code, r.Body.String()) }
}

func TestDataDirectory(t *testing.T) {
 for _, tc := range []struct{ value, want string }{
  {"", "/app/data"},
  {"  ", "/app/data"},
  {" /custom/workhub ", "/custom/workhub"},
  {"/app/runtime", "/app/runtime"},
 } {
  t.Setenv("DATA_DIR", tc.value)
  if got := dataDirectory(); got != tc.want {
   t.Fatalf("DATA_DIR=%q: got %q, want %q", tc.value, got, tc.want)
  }
 }
}
