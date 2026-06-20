package main

import (
	"os"
	"testing"
)

func TestPathExists_Exists(t *testing.T) {
	// pathExists must return true for a path that exists on disk (file or dir).
	f, err := os.CreateTemp(t.TempDir(), "exists")
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
	if !pathExists(f.Name()) {
		t.Errorf("expected pathExists=true for existing file %s", f.Name())
	}
}

func TestPathExists_NotExists(t *testing.T) {
	// pathExists must return false for a path that does not exist, preventing
	// the merge command from accidentally overwriting an absent output file.
	if pathExists("/this/path/does/not/exist/hopefully") {
		t.Error("expected pathExists=false for nonexistent path")
	}
}
