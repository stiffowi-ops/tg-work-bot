package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestVolumeLocation(t *testing.T) {
	if got, err := defaultDirectoryFor(t.TempDir()); err != nil || got != "/app/data" {
		t.Fatalf("fresh install: %q, %v", got, err)
	}
	for _, marker := range []string{"employees.json", "data/projects/industry/content.json"} {
		previous := t.TempDir()
		path := filepath.Join(previous, marker)
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte("{}"), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := defaultDirectoryFor(previous); err == nil {
			t.Fatal("silently ignored existing", marker)
		}
	}
	for _, dir := range []string{"/app/runtime", filepath.Join(t.TempDir(), "existing volume")} {
		t.Setenv("DATA_DIR", dir)
		if got, err := dataDirectory(); err != nil || got != dir {
			t.Fatalf("override: %q, %v", got, err)
		}
	}
}

func TestStartupRejectsBrokenStorage(t *testing.T) {
	for _, path := range []string{"employees.json", "data/projects/industry/content.json", "data/projects/calendar/files"} {
		t.Run(path, func(t *testing.T) {
			dir := t.TempDir()
			dest := filepath.Join(dir, path)
			if err := os.MkdirAll(filepath.Dir(dest), 0700); err != nil {
				t.Fatal(err)
			}
			original := "original data"
			if filepath.Base(dest) == "content.json" {
				original = "{}" // An existing but incomplete project must not turn into seed data.
			}
			if err := os.WriteFile(dest, []byte(original), 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := routes(dir); err == nil {
				t.Fatal("server accepted broken storage")
			}
			if got, err := os.ReadFile(dest); err != nil || string(got) != original {
				t.Fatal("startup changed existing data", err)
			}
		})
	}
}
