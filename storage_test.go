package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestResolveStorage(t *testing.T) {
	for _, tc := range []struct {
		value string
		want  string
	}{
		{"", "/app/data"},
		{"  ", "/app/data"},
		{" /app/data/ ", "/app/data"},
		{"/app/runtime", "/app/runtime"},
		{"/custom/storage", "/custom/storage"},
	} {
		got := resolveStorage(tc.value)
		if got.root != tc.want || got.employees != filepath.Join(tc.want, "employees.json") || got.projects != filepath.Join(tc.want, "data", "projects") {
			t.Fatalf("DATA_DIR %q: %+v", tc.value, got)
		}
	}
}

func TestStorageExistingLayout(t *testing.T) {
	root := t.TempDir()
	t.Setenv("BOT_TOKEN", "test-token")
	t.Setenv("WORK_CHAT_ID", "-100123")
	t.Setenv("ADMIN_USER_IDS", "42")
	paths := resolveStorage(root)
	// Seed files at the pre-upgrade locations and reopen without migration.
	if err := os.WriteFile(filepath.Join(root, "employees.json"), []byte("{}"), 0600); err != nil {
		t.Fatal(err)
	}
	c := &cms{root: filepath.Join(root, "data", "projects")}
	p, err := c.load("calendar")
	if err != nil {
		t.Fatal(err)
	}
	p.Title = "Existing project"
	if err := c.save(p); err != nil {
		t.Fatal(err)
	}
	a := newEmployeeAPI(paths)
	if a.store == nil || a.store.path != paths.employees {
		t.Fatalf("employee store not reopened: %s", a.setupError)
	}
	reopened := &cms{auth: a, root: paths.projects}
	saved, err := reopened.load("calendar")
	if err != nil || saved.Title != "Existing project" {
		t.Fatalf("existing project not reopened: %+v %v", saved, err)
	}
}
