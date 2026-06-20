package tilepack

import (
	"testing"
)

func TestMbtilesMetadata_Get_Exists(t *testing.T) {
	// Get must return the value and ok=true for a key that was set.
	m := NewMbtilesMetadata(map[string]string{"name": "my-tileset"})
	v, ok := m.Get("name")
	if !ok {
		t.Fatal("expected key 'name' to exist")
	}
	if v != "my-tileset" {
		t.Errorf("expected 'my-tileset', got %q", v)
	}
}

func TestMbtilesMetadata_Get_Missing(t *testing.T) {
	// Get must return ok=false for a key that was never set.
	m := NewMbtilesMetadata(map[string]string{})
	_, ok := m.Get("missing")
	if ok {
		t.Fatal("expected missing key to return ok=false")
	}
}

func TestMbtilesMetadata_Bounds_Valid(t *testing.T) {
	// Bounds must parse the MBTiles spec "minx,miny,maxx,maxy" format into an orb.Bound.
	m := NewMbtilesMetadata(map[string]string{
		"bounds": "-122.4,37.7,-122.3,37.8",
	})
	b, err := m.Bounds()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if b.Min[0] != -122.4 || b.Min[1] != 37.7 || b.Max[0] != -122.3 || b.Max[1] != 37.8 {
		t.Errorf("unexpected bounds: %+v", b)
	}
}

func TestMbtilesMetadata_Bounds_Missing(t *testing.T) {
	// A missing 'bounds' key must return an error, not a zero value.
	m := NewMbtilesMetadata(map[string]string{})
	_, err := m.Bounds()
	if err == nil {
		t.Fatal("expected error for missing bounds key")
	}
}

func TestMbtilesMetadata_Bounds_WrongPartCount(t *testing.T) {
	// A bounds string with fewer than 4 comma-separated fields is malformed.
	m := NewMbtilesMetadata(map[string]string{"bounds": "-122.4,37.7"})
	_, err := m.Bounds()
	if err == nil {
		t.Fatal("expected error for malformed bounds")
	}
}

func TestMbtilesMetadata_Bounds_NonNumeric(t *testing.T) {
	// Non-numeric values in the bounds string must return a parse error.
	m := NewMbtilesMetadata(map[string]string{"bounds": "west,south,east,north"})
	_, err := m.Bounds()
	if err == nil {
		t.Fatal("expected error for non-numeric bounds")
	}
}

func TestMbtilesMetadata_Center_Valid(t *testing.T) {
	// Center must parse the MBTiles spec "lon,lat,zoom" format into a point and zoom level.
	m := NewMbtilesMetadata(map[string]string{"center": "-77.0,38.9,5"})
	pt, z, err := m.Center()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if pt[0] != -77.0 || pt[1] != 38.9 {
		t.Errorf("unexpected center point: %+v", pt)
	}
	if z != 5 {
		t.Errorf("expected zoom 5, got %d", z)
	}
}

func TestMbtilesMetadata_Center_Missing(t *testing.T) {
	// A missing 'center' key must return an error.
	m := NewMbtilesMetadata(map[string]string{})
	_, _, err := m.Center()
	if err == nil {
		t.Fatal("expected error for missing center key")
	}
}

func TestMbtilesMetadata_Center_WrongPartCount(t *testing.T) {
	// Center must have exactly 3 comma-separated fields: lon,lat,zoom.
	m := NewMbtilesMetadata(map[string]string{"center": "-77.0,38.9"})
	_, _, err := m.Center()
	if err == nil {
		t.Fatal("expected error for malformed center")
	}
}

func TestMbtilesMetadata_MinZoom_Valid(t *testing.T) {
	// MinZoom must parse the integer string value from the metadata map.
	m := NewMbtilesMetadata(map[string]string{"minzoom": "2"})
	z, err := m.MinZoom()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if z != 2 {
		t.Errorf("expected minzoom 2, got %d", z)
	}
}

func TestMbtilesMetadata_MinZoom_Missing(t *testing.T) {
	// A missing 'minzoom' key must return an error, not a zero value.
	m := NewMbtilesMetadata(map[string]string{})
	_, err := m.MinZoom()
	if err == nil {
		t.Fatal("expected error for missing minzoom")
	}
}

func TestMbtilesMetadata_MinZoom_NonNumeric(t *testing.T) {
	// A non-integer 'minzoom' value must return a parse error.
	m := NewMbtilesMetadata(map[string]string{"minzoom": "two"})
	_, err := m.MinZoom()
	if err == nil {
		t.Fatal("expected error for non-numeric minzoom")
	}
}

func TestMbtilesMetadata_MaxZoom_Valid(t *testing.T) {
	// MaxZoom must parse the integer string value from the metadata map.
	m := NewMbtilesMetadata(map[string]string{"maxzoom": "14"})
	z, err := m.MaxZoom()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if z != 14 {
		t.Errorf("expected maxzoom 14, got %d", z)
	}
}

func TestMbtilesMetadata_MaxZoom_Missing(t *testing.T) {
	// A missing 'maxzoom' key must return an error, not a zero value.
	m := NewMbtilesMetadata(map[string]string{})
	_, err := m.MaxZoom()
	if err == nil {
		t.Fatal("expected error for missing maxzoom")
	}
}

func TestMbtilesMetadata_MaxZoom_NonNumeric(t *testing.T) {
	// A non-integer 'maxzoom' value must return a parse error.
	m := NewMbtilesMetadata(map[string]string{"maxzoom": "fourteen"})
	_, err := m.MaxZoom()
	if err == nil {
		t.Fatal("expected error for non-numeric maxzoom")
	}
}

func TestMbtilesMetadata_Format(t *testing.T) {
	// Format must return the tile format string (e.g. "pbf", "png") from the metadata map.
	m := NewMbtilesMetadata(map[string]string{"format": "pbf"})
	f, err := m.Format()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if f != "pbf" {
		t.Errorf("expected format 'pbf', got %q", f)
	}
}

func TestMbtilesMetadata_Name(t *testing.T) {
	// Name must return the human-readable tileset name from the metadata map.
	m := NewMbtilesMetadata(map[string]string{"name": "test-tiles"})
	n, err := m.Name()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != "test-tiles" {
		t.Errorf("expected name 'test-tiles', got %q", n)
	}
}

func TestMbtilesMetadata_Keys(t *testing.T) {
	// Keys must return exactly the set of keys present in the metadata map.
	m := NewMbtilesMetadata(map[string]string{"name": "x", "format": "pbf", "minzoom": "0"})
	keys := m.Keys()
	if len(keys) != 3 {
		t.Errorf("expected 3 keys, got %d: %v", len(keys), keys)
	}
}

func TestMbtilesMetadata_Bounds_NonNumeric_Miny(t *testing.T) {
	// Each of the four bounds fields must individually fail on non-numeric input.
	m := NewMbtilesMetadata(map[string]string{"bounds": "-122.4,bad,-122.3,37.8"})
	_, err := m.Bounds()
	if err == nil {
		t.Fatal("expected error for non-numeric miny")
	}
}

func TestMbtilesMetadata_Bounds_NonNumeric_Maxx(t *testing.T) {
	m := NewMbtilesMetadata(map[string]string{"bounds": "-122.4,37.7,bad,37.8"})
	_, err := m.Bounds()
	if err == nil {
		t.Fatal("expected error for non-numeric maxx")
	}
}

func TestMbtilesMetadata_Bounds_NonNumeric_Maxy(t *testing.T) {
	m := NewMbtilesMetadata(map[string]string{"bounds": "-122.4,37.7,-122.3,bad"})
	_, err := m.Bounds()
	if err == nil {
		t.Fatal("expected error for non-numeric maxy")
	}
}

func TestMbtilesMetadata_Center_NonNumeric_X(t *testing.T) {
	// A non-numeric longitude in the center string must return a parse error.
	m := NewMbtilesMetadata(map[string]string{"center": "bad,38.9,5"})
	_, _, err := m.Center()
	if err == nil {
		t.Fatal("expected error for non-numeric center x")
	}
}

func TestMbtilesMetadata_Center_NonNumeric_Y(t *testing.T) {
	// A non-numeric latitude in the center string must return a parse error.
	m := NewMbtilesMetadata(map[string]string{"center": "-77.0,bad,5"})
	_, _, err := m.Center()
	if err == nil {
		t.Fatal("expected error for non-numeric center y")
	}
}

func TestMbtilesMetadata_Center_NonNumeric_Zoom(t *testing.T) {
	// A non-numeric zoom in the center string must return a parse error.
	m := NewMbtilesMetadata(map[string]string{"center": "-77.0,38.9,five"})
	_, _, err := m.Center()
	if err == nil {
		t.Fatal("expected error for non-numeric center zoom")
	}
}

func TestMbtilesMetadata_Set(t *testing.T) {
	// Set must overwrite an existing key and create a new one.
	m := NewMbtilesMetadata(map[string]string{"name": "old"})
	m.Set("name", "new")
	v, _ := m.Get("name")
	if v != "new" {
		t.Errorf("expected 'new', got %q", v)
	}
	m.Set("format", "mvt")
	f, ok := m.Get("format")
	if !ok || f != "mvt" {
		t.Errorf("expected 'mvt', got %q (ok=%v)", f, ok)
	}
}
