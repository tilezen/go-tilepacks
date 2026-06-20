package tilepack

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/paulmach/orb"
	"github.com/paulmach/orb/maptile"
)

func TestNewDiskOutputter_MissingRequired(t *testing.T) {
	// NewDiskOutputter must return an error when required DSN keys are absent.
	// The 'root' key is required; an empty DSN string should fail.
	_, err := NewDiskOutputter("")
	if err == nil {
		t.Fatal("expected error for empty DSN")
	}
}

func TestDiskOutputter_Save_CreatesSubdirs(t *testing.T) {
	// Save must create intermediate Z/X directories when they don't yet exist,
	// not just fail because the parent dir is missing.
	dir := t.TempDir()
	o, err := NewDiskOutputter("root=" + dir + " format=pbf")
	if err != nil {
		t.Fatalf("NewDiskOutputter: %v", err)
	}
	if err := o.CreateTiles(); err != nil {
		t.Fatalf("CreateTiles: %v", err)
	}

	// Save a tile at a zoom level that requires nested directories.
	tile := maptile.New(15, 10, 5)
	if err := o.Save(tile, []byte("data")); err != nil {
		t.Fatalf("Save: %v", err)
	}

	expectedPath := filepath.Join(dir, "5/15/10.pbf")
	if _, err := os.Stat(expectedPath); err != nil {
		t.Errorf("expected tile file at %s: %v", expectedPath, err)
	}
}

func TestNewDiskOutputter_Valid(t *testing.T) {
	// NewDiskOutputter must succeed for a valid DSN pointing at an existing directory.
	dir := t.TempDir()
	o, err := NewDiskOutputter("root=" + dir + " format=pbf")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if o == nil {
		t.Fatal("expected non-nil outputter")
	}
}

func TestDiskOutputter_CreateTiles_CreatesDir(t *testing.T) {
	// CreateTiles must create the root directory when it does not yet exist.
	dir := filepath.Join(t.TempDir(), "tiles")
	o, err := NewDiskOutputter("root=" + dir + " format=pbf")
	if err != nil {
		t.Fatalf("NewDiskOutputter: %v", err)
	}
	if err := o.CreateTiles(); err != nil {
		t.Fatalf("CreateTiles: %v", err)
	}
	if _, err := os.Stat(dir); err != nil {
		t.Errorf("expected root directory to exist after CreateTiles: %v", err)
	}
}

func TestDiskOutputter_CreateTiles_ExistingDir(t *testing.T) {
	// CreateTiles must succeed (no error) when the root directory already exists.
	dir := t.TempDir()
	o, err := NewDiskOutputter("root=" + dir + " format=pbf")
	if err != nil {
		t.Fatalf("NewDiskOutputter: %v", err)
	}
	if err := o.CreateTiles(); err != nil {
		t.Fatalf("CreateTiles on existing dir: %v", err)
	}
}

func TestDiskOutputter_CreateTiles_RootIsFile(t *testing.T) {
	// CreateTiles must return an error when the root path exists but is a file,
	// not a directory — writing Z/X/Y tiles inside a file is impossible.
	f, err := os.CreateTemp(t.TempDir(), "notadir")
	if err != nil {
		t.Fatalf("create temp file: %v", err)
	}
	f.Close()

	o, err := NewDiskOutputter("root=" + f.Name() + " format=pbf")
	if err != nil {
		t.Fatalf("NewDiskOutputter: %v", err)
	}
	if err := o.CreateTiles(); err == nil {
		t.Fatal("expected error when root is a file, got nil")
	}
}

func TestDiskOutputter_CreateTiles_Idempotent(t *testing.T) {
	// Calling CreateTiles twice must succeed; the second call is a no-op.
	dir := t.TempDir()
	o, err := NewDiskOutputter("root=" + dir + " format=pbf")
	if err != nil {
		t.Fatalf("NewDiskOutputter: %v", err)
	}
	if err := o.CreateTiles(); err != nil {
		t.Fatalf("first CreateTiles: %v", err)
	}
	if err := o.CreateTiles(); err != nil {
		t.Fatalf("second CreateTiles: %v", err)
	}
}

func TestDiskOutputter_Save(t *testing.T) {
	// Save must write tile data to the correct Z/X/Y.format path under the root.
	dir := t.TempDir()
	o, err := NewDiskOutputter("root=" + dir + " format=pbf")
	if err != nil {
		t.Fatalf("NewDiskOutputter: %v", err)
	}
	if err := o.CreateTiles(); err != nil {
		t.Fatalf("CreateTiles: %v", err)
	}

	tile := maptile.New(3, 2, 5)
	data := []byte("tile-bytes")
	if err := o.Save(tile, data); err != nil {
		t.Fatalf("Save: %v", err)
	}

	expectedPath := filepath.Join(dir, "5/3/2.pbf")
	got, err := os.ReadFile(expectedPath)
	if err != nil {
		t.Fatalf("expected file at %s: %v", expectedPath, err)
	}
	if string(got) != string(data) {
		t.Errorf("file content mismatch: got %q, want %q", got, data)
	}
}

func TestDiskOutputter_AssignSpatialMetadata(t *testing.T) {
	// AssignSpatialMetadata is a no-op for disk output (metadata is not stored
	// in a flat-file tile tree), but must not return an error.
	dir := t.TempDir()
	o, err := NewDiskOutputter("root=" + dir + " format=pbf")
	if err != nil {
		t.Fatalf("NewDiskOutputter: %v", err)
	}
	bounds := orb.Bound{Min: orb.Point{-180, -85}, Max: orb.Point{180, 85}}
	if err := o.AssignSpatialMetadata(bounds, 0, 5); err != nil {
		t.Fatalf("AssignSpatialMetadata: %v", err)
	}
}

func TestDiskOutputter_Close(t *testing.T) {
	// Close is a no-op for disk output (no connection to tear down), but must
	// not return an error.
	dir := t.TempDir()
	o, err := NewDiskOutputter("root=" + dir + " format=pbf")
	if err != nil {
		t.Fatalf("NewDiskOutputter: %v", err)
	}
	if err := o.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}
