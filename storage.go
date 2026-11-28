package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const defaultDataDir = "/app/data"

func dataDirectory() (string, error) {
	dir := os.Getenv("DATA_DIR")
	if dir != "" {
		if strings.TrimSpace(dir) != dir {
			return "", errors.New("DATA_DIR contains leading or trailing whitespace: set it to the existing volume path")
		}
		return dir, nil
	}
	// Older standalone binaries used /app/runtime when no path was supplied.
	// Require an explicit choice before a new default can hide that store.
	return defaultDirectoryFor("/app/runtime")
}

func defaultDirectoryFor(previous string) (string, error) {
	markers := []string{filepath.Join(previous, "employees.json")}
	for _, id := range projectIDs {
		markers = append(markers, filepath.Join(previous, "data", "projects", id, "content.json"))
	}
	for _, path := range markers {
		if _, err := os.Stat(path); err == nil {
			return "", fmt.Errorf("existing WorkHub data at %q: set DATA_DIR explicitly to the volume you want to use", previous)
		} else if !errors.Is(err, os.ErrNotExist) {
			return "", fmt.Errorf("check previous data path: %w", err)
		}
	}
	return defaultDataDir, nil
}

// Probe as the service user, after the host has mounted its volume.
func checkStorageDir(dir string) error {
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	probe, err := os.CreateTemp(dir, ".workhub-check-*")
	if err != nil {
		return err
	}
	closeErr := probe.Close()
	return errors.Join(closeErr, os.Remove(probe.Name()))
}
