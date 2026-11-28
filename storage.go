package main

import (
	"path/filepath"
	"strings"
)

// defaultDataDir matches the Docker image and the production volume mount.
const defaultDataDir = "/app/data"

type storagePaths struct {
	root      string
	employees string
	projects  string
}

// Resolve DATA_DIR exactly once at application startup. Keep the existing
// on-disk layout: changing it would hide already persisted employee/CMS data.
func resolveStorage(value string) storagePaths {
	root := strings.TrimSpace(value)
	if root == "" {
		root = defaultDataDir
	}
	root = filepath.Clean(root)
	return storagePaths{
		root:      root,
		employees: filepath.Join(root, "employees.json"),
		projects:  filepath.Join(root, "data", "projects"),
	}
}
