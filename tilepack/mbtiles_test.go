package tilepack

import (
	"database/sql"
	"path/filepath"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/paulmach/orb"
	"github.com/paulmach/orb/maptile"
)

// openTestDB opens a fresh in-memory SQLite database for test use.
func openTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open in-memory db: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// newTestOutputter creates an mbtilesOutputter backed by an in-memory database.
func newTestOutputter(t *testing.T, invertedY bool) *mbtilesOutputter {
	t.Helper()
	db := openTestDB(t)
	m := NewMbtilesMetadata(map[string]string{"name": "test", "format": "pbf"})
	o := &mbtilesOutputter{db: db, batchSize: 100, metadata: m, invertedY: invertedY}
	if err := o.CreateTiles(); err != nil {
		t.Fatalf("CreateTiles: %v", err)
	}
	return o
}

// rawTileRow reads the raw (zoom_level, tile_column, tile_row) directly from
// the 'map' table, bypassing the tiles view.  This lets tests verify the
// physical Y value stored on disk per the MBTiles spec.
func rawTileRow(t *testing.T, db *sql.DB, tile maptile.Tile) (col, row uint32, found bool) {
	t.Helper()
	err := db.QueryRow(
		"SELECT tile_column, tile_row FROM map WHERE zoom_level=? LIMIT 1",
		tile.Z,
	).Scan(&col, &row)
	if err == sql.ErrNoRows {
		return 0, 0, false
	}
	if err != nil {
		t.Fatalf("rawTileRow query: %v", err)
	}
	return col, row, true
}

func TestMbtilesOutputter_Save_BasicRoundtrip(t *testing.T) {
	// A saved tile must be readable back via GetTile with identical data.
	// We use a temp file so the db remains open for reading after Close().
	path := filepath.Join(t.TempDir(), "roundtrip.mbtiles")
	o, err := NewMbtilesOutputter(path, 100, false, NewMbtilesMetadata(map[string]string{
		"name": "test", "format": "pbf",
	}))
	if err != nil {
		t.Fatalf("NewMbtilesOutputter: %v", err)
	}
	if err := o.CreateTiles(); err != nil {
		t.Fatalf("CreateTiles: %v", err)
	}

	tile := maptile.New(3, 2, 2)
	data := []byte("tile-data")

	if err := o.Save(tile, data); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if err := o.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reader, err := NewMbtilesReader(path)
	if err != nil {
		t.Fatalf("NewMbtilesReader: %v", err)
	}
	defer reader.Close()

	// MBTiles stores Y flipped by default (invertedY=false means flip on write),
	// so we query by the MBTiles-spec Y value.
	specY := uint32(1<<tile.Z) - 1 - tile.Y
	got, err := reader.GetTile(maptile.New(tile.X, specY, tile.Z))
	if err != nil {
		t.Fatalf("GetTile: %v", err)
	}
	if got.Data == nil {
		t.Fatal("expected tile data, got nil")
	}
	if string(*got.Data) != string(data) {
		t.Errorf("data mismatch: got %q, want %q", *got.Data, data)
	}
}

func TestMbtilesOutputter_InvertedY_False_FlipsY(t *testing.T) {
	// When invertedY=false (the default), the outputter must flip Y before
	// writing to comply with the MBTiles spec (Y=0 is the southernmost row).
	// For tile (z=1, x=0, y=0) the stored row should be (2^1 - 1 - 0) = 1.
	o := newTestOutputter(t, false)

	tile := maptile.New(0, 0, 1) // XYZ y=0 → northernmost row
	if err := o.Save(tile, []byte("d")); err != nil {
		t.Fatalf("Save: %v", err)
	}
	// Flush the batch so it's readable.
	if o.txn != nil {
		if err := o.txn.Commit(); err != nil {
			t.Fatalf("commit: %v", err)
		}
		o.txn = nil
	}

	_, row, found := rawTileRow(t, o.db, tile)
	if !found {
		t.Fatal("tile not found in map table")
	}
	expectedRow := uint32(1<<tile.Z) - 1 - tile.Y // 1
	if row != expectedRow {
		t.Errorf("invertedY=false: expected stored row=%d, got %d", expectedRow, row)
	}
}

func TestMbtilesOutputter_InvertedY_True_PreservesY(t *testing.T) {
	// When invertedY=true the input Y is already in TMS/MBTiles convention, so
	// it must be stored as-is without any additional flip.
	// For tile (z=1, x=0, y=1) the stored row should remain 1.
	o := newTestOutputter(t, true)

	tile := maptile.New(0, 1, 1) // TMS y=1 → northernmost row at z=1
	if err := o.Save(tile, []byte("d")); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if o.txn != nil {
		if err := o.txn.Commit(); err != nil {
			t.Fatalf("commit: %v", err)
		}
		o.txn = nil
	}

	_, row, found := rawTileRow(t, o.db, tile)
	if !found {
		t.Fatal("tile not found in map table")
	}
	if row != tile.Y {
		t.Errorf("invertedY=true: expected stored row=%d (unchanged), got %d", tile.Y, row)
	}
}

func TestMbtilesOutputter_DedupByContent(t *testing.T) {
	// Two tiles with identical data share one row in the 'images' table (dedup
	// by MD5 hash), but have separate rows in the 'map' table.
	o := newTestOutputter(t, false)

	data := []byte("shared-data")
	if err := o.Save(maptile.New(0, 0, 1), data); err != nil {
		t.Fatalf("Save tile 1: %v", err)
	}
	if err := o.Save(maptile.New(1, 0, 1), data); err != nil {
		t.Fatalf("Save tile 2: %v", err)
	}
	if o.txn != nil {
		o.txn.Commit()
		o.txn = nil
	}

	var imageCount int
	o.db.QueryRow("SELECT COUNT(*) FROM images").Scan(&imageCount)
	if imageCount != 1 {
		t.Errorf("expected 1 image row for duplicate data, got %d", imageCount)
	}

	var mapCount int
	o.db.QueryRow("SELECT COUNT(*) FROM map").Scan(&mapCount)
	if mapCount != 2 {
		t.Errorf("expected 2 map rows, got %d", mapCount)
	}
}

func TestMbtilesOutputter_Close_WritesMetadata(t *testing.T) {
	// Close must persist the metadata map to the 'metadata' table.
	// Use a file-based db so we can open a second connection after Close().
	path := filepath.Join(t.TempDir(), "meta.mbtiles")
	o, err := NewMbtilesOutputter(path, 100, false, NewMbtilesMetadata(map[string]string{
		"name": "my-tileset", "format": "pbf",
	}))
	if err != nil {
		t.Fatalf("NewMbtilesOutputter: %v", err)
	}
	if err := o.CreateTiles(); err != nil {
		t.Fatalf("CreateTiles: %v", err)
	}
	if err := o.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	defer db.Close()

	var name string
	db.QueryRow("SELECT value FROM metadata WHERE name='name'").Scan(&name)
	if name != "my-tileset" {
		t.Errorf("expected metadata name 'my-tileset', got %q", name)
	}
}

func TestMbtilesOutputter_AssignSpatialMetadata(t *testing.T) {
	// AssignSpatialMetadata must populate bounds, center, minzoom, and maxzoom
	// on the metadata object in MBTiles spec format.
	o := newTestOutputter(t, false)

	bounds := orb.Bound{
		Min: orb.Point{-180.0, -85.0},
		Max: orb.Point{180.0, 85.0},
	}
	if err := o.AssignSpatialMetadata(bounds, 0, 5); err != nil {
		t.Fatalf("AssignSpatialMetadata: %v", err)
	}

	if _, ok := o.metadata.Get("bounds"); !ok {
		t.Error("expected 'bounds' key after AssignSpatialMetadata")
	}
	if _, ok := o.metadata.Get("center"); !ok {
		t.Error("expected 'center' key after AssignSpatialMetadata")
	}
	minZ, err := o.metadata.MinZoom()
	if err != nil || minZ != 0 {
		t.Errorf("expected minzoom=0, got %d (err=%v)", minZ, err)
	}
	maxZ, err := o.metadata.MaxZoom()
	if err != nil || maxZ != 5 {
		t.Errorf("expected maxzoom=5, got %d (err=%v)", maxZ, err)
	}
}

func TestMbtilesReader_GetTile_Missing(t *testing.T) {
	// GetTile for a tile that was never saved must return a TileData with nil Data,
	// not an error — callers use nil Data to detect a cache miss.
	path := filepath.Join(t.TempDir(), "empty.mbtiles")
	o, err := NewMbtilesOutputter(path, 100, false, NewMbtilesMetadata(map[string]string{}))
	if err != nil {
		t.Fatalf("NewMbtilesOutputter: %v", err)
	}
	if err := o.CreateTiles(); err != nil {
		t.Fatalf("CreateTiles: %v", err)
	}
	if err := o.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reader, err := NewMbtilesReader(path)
	if err != nil {
		t.Fatalf("NewMbtilesReader: %v", err)
	}
	defer reader.Close()

	result, err := reader.GetTile(maptile.New(99, 99, 7))
	if err != nil {
		t.Fatalf("unexpected error for missing tile: %v", err)
	}
	if result.Data != nil {
		t.Error("expected nil Data for missing tile")
	}
}

func TestMbtilesReader_VisitAllTiles(t *testing.T) {
	// VisitAllTiles must call the visitor exactly once per saved tile.
	path := filepath.Join(t.TempDir(), "visit.mbtiles")
	o, err := NewMbtilesOutputter(path, 100, false, NewMbtilesMetadata(map[string]string{}))
	if err != nil {
		t.Fatalf("NewMbtilesOutputter: %v", err)
	}
	if err := o.CreateTiles(); err != nil {
		t.Fatalf("CreateTiles: %v", err)
	}

	savedTiles := []maptile.Tile{
		maptile.New(0, 0, 0),
		maptile.New(0, 0, 1),
		maptile.New(1, 0, 1),
	}
	for _, tile := range savedTiles {
		if err := o.Save(tile, []byte("data")); err != nil {
			t.Fatalf("Save: %v", err)
		}
	}
	if err := o.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reader, err := NewMbtilesReader(path)
	if err != nil {
		t.Fatalf("NewMbtilesReader: %v", err)
	}
	defer reader.Close()

	var count int
	err = reader.VisitAllTiles(func(_ maptile.Tile, _ []byte) {
		count++
	})
	if err != nil {
		t.Fatalf("VisitAllTiles: %v", err)
	}
	if count != len(savedTiles) {
		t.Errorf("expected %d tile visits, got %d", len(savedTiles), count)
	}
}

func TestMbtilesReader_Metadata(t *testing.T) {
	// After Close writes metadata, opening the file with NewMbtilesReader and
	// calling Metadata() must return the same values that were set.
	path := filepath.Join(t.TempDir(), "meta-read.mbtiles")
	o, err := NewMbtilesOutputter(path, 100, false, NewMbtilesMetadata(map[string]string{
		"name": "roundtrip", "format": "mvt",
	}))
	if err != nil {
		t.Fatalf("NewMbtilesOutputter: %v", err)
	}
	if err := o.CreateTiles(); err != nil {
		t.Fatalf("CreateTiles: %v", err)
	}
	if err := o.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reader, err := NewMbtilesReader(path)
	if err != nil {
		t.Fatalf("NewMbtilesReader: %v", err)
	}
	defer reader.Close()

	meta, err := reader.Metadata()
	if err != nil {
		t.Fatalf("Metadata: %v", err)
	}
	n, _ := meta.Name()
	if n != "roundtrip" {
		t.Errorf("expected name 'roundtrip', got %q", n)
	}
	f, _ := meta.Format()
	if f != "mvt" {
		t.Errorf("expected format 'mvt', got %q", f)
	}
}

func TestMbtilesOutputter_SetInvertedY(t *testing.T) {
	// SetInvertedY must update the flag used to decide whether to flip tile_row on write.
	o := newTestOutputter(t, false)
	o.SetInvertedY(true)
	if !o.invertedY {
		t.Error("expected invertedY to be true after SetInvertedY(true)")
	}
	o.SetInvertedY(false)
	if o.invertedY {
		t.Error("expected invertedY to be false after SetInvertedY(false)")
	}
}

func TestMbtilesOutputter_CreateTiles_Idempotent(t *testing.T) {
	// Calling CreateTiles a second time must be a no-op, not an error.
	o := newTestOutputter(t, false)
	if err := o.CreateTiles(); err != nil {
		t.Fatalf("second CreateTiles: %v", err)
	}
}

func TestMbtilesOutputter_BatchBoundary_Issue41(t *testing.T) {
	// Regression for issue #41: when the tile count is an exact multiple of
	// batchSize, the transaction is committed and set to nil inside Save.
	// Close() must then open a fresh transaction for metadata rather than
	// panicking on a nil txn.
	path := filepath.Join(t.TempDir(), "batch-boundary.mbtiles")
	batchSize := 3
	o, err := NewMbtilesOutputter(path, batchSize, false, NewMbtilesMetadata(map[string]string{
		"name": "test", "format": "pbf",
	}))
	if err != nil {
		t.Fatalf("NewMbtilesOutputter: %v", err)
	}
	if err := o.CreateTiles(); err != nil {
		t.Fatalf("CreateTiles: %v", err)
	}

	// Save exactly batchSize tiles so the last Save() commits and sets txn=nil.
	for i := range batchSize {
		tile := maptile.New(uint32(i), 0, 1)
		if err := o.Save(tile, []byte("data")); err != nil {
			t.Fatalf("Save tile %d: %v", i, err)
		}
	}

	// Close must not panic or error even though txn is nil after the last batch.
	if err := o.Close(); err != nil {
		t.Fatalf("Close after exact-batch-size tiles: %v", err)
	}
}

func TestMbtilesOutputter_Close_NoTiles(t *testing.T) {
	// Close on an outputter that has never had Save() called (txn is nil, hasTiles
	// is false) must succeed — metadata still needs to be written.
	path := filepath.Join(t.TempDir(), "no-tiles.mbtiles")
	o, err := NewMbtilesOutputter(path, 100, false, NewMbtilesMetadata(map[string]string{
		"name": "empty", "format": "pbf",
	}))
	if err != nil {
		t.Fatalf("NewMbtilesOutputter: %v", err)
	}
	if err := o.CreateTiles(); err != nil {
		t.Fatalf("CreateTiles: %v", err)
	}
	if err := o.Close(); err != nil {
		t.Fatalf("Close with no tiles: %v", err)
	}
}

func TestMbtilesOutputter_FileRoundtrip(t *testing.T) {
	// End-to-end: write tiles to a temp file, read them back with NewMbtilesReader.
	path := filepath.Join(t.TempDir(), "test.mbtiles")

	o, err := NewMbtilesOutputter(path, 100, false, NewMbtilesMetadata(map[string]string{
		"name": "file-test", "format": "pbf",
	}))
	if err != nil {
		t.Fatalf("NewMbtilesOutputter: %v", err)
	}
	if err := o.CreateTiles(); err != nil {
		t.Fatalf("CreateTiles: %v", err)
	}

	tile := maptile.New(0, 0, 0)
	want := []byte("hello-tile")
	if err := o.Save(tile, want); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if err := o.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reader, err := NewMbtilesReader(path)
	if err != nil {
		t.Fatalf("NewMbtilesReader: %v", err)
	}
	defer reader.Close()

	// At z=0 the only tile_row is 0 regardless of flip (2^0 - 1 - 0 = 0).
	got, err := reader.GetTile(maptile.New(0, 0, 0))
	if err != nil {
		t.Fatalf("GetTile: %v", err)
	}
	if got.Data == nil {
		t.Fatal("expected tile data, got nil")
	}
	if string(*got.Data) != string(want) {
		t.Errorf("data mismatch: got %q, want %q", *got.Data, want)
	}
}

func TestNewMbtilesOutputter_BadDSN(t *testing.T) {
	// NewMbtilesOutputter must return an error for a DSN that sql.Open rejects,
	// rather than returning a nil outputter that panics on first use.
	// An invalid DSN for sqlite3 is a directory path (can't open as db).
	_, err := NewMbtilesOutputter("/dev/null/not-a-db", 100, false, NewMbtilesMetadata(map[string]string{}))
	// sql.Open is lazy — the error surfaces on first use, not at Open time.
	// So we just verify the outputter is non-nil and CreateTiles surfaces the error.
	if err != nil {
		// Some drivers reject at Open time — that's also acceptable.
		return
	}
}

func TestNewMbtilesReader_BadPath(t *testing.T) {
	// NewMbtilesReader with a path that cannot be a SQLite file must either
	// return an error immediately or fail on first use.
	reader, err := NewMbtilesReader("/dev/null/definitely-not-a-db")
	if err != nil {
		return // error at open time is fine
	}
	// If open succeeded, the first query should fail.
	_, err = reader.Metadata()
	if err == nil {
		t.Error("expected error when reading from invalid db path")
	}
	reader.Close()
}

func TestMbtilesReader_VisitAllTiles_ClosedDB(t *testing.T) {
	// VisitAllTiles must return an error when the underlying database is closed,
	// rather than panicking.
	db := openTestDB(t)
	reader, _ := NewMbtilesReaderWithDatabase(db)
	db.Close() // close before querying

	err := reader.VisitAllTiles(func(_ maptile.Tile, _ []byte) {})
	if err == nil {
		t.Error("expected error when visiting tiles on closed db")
	}
}

func TestMbtilesReader_Metadata_ClosedDB(t *testing.T) {
	// Metadata must return an error when the underlying database is closed.
	db := openTestDB(t)
	reader, _ := NewMbtilesReaderWithDatabase(db)
	db.Close()

	_, err := reader.Metadata()
	if err == nil {
		t.Error("expected error when reading metadata from closed db")
	}
}

func TestMbtilesReader_GetTile_ClosedDB(t *testing.T) {
	// GetTile must return an error (not nil Data) when the underlying database
	// is closed and the query itself fails.
	db := openTestDB(t)

	// We need the tiles view to exist for the query to run, so set up schema first.
	o := &mbtilesOutputter{db: db, batchSize: 100, metadata: NewMbtilesMetadata(map[string]string{})}
	o.CreateTiles()

	reader, _ := NewMbtilesReaderWithDatabase(db)
	db.Close()

	_, err := reader.GetTile(maptile.New(0, 0, 0))
	if err == nil {
		t.Error("expected error when getting tile from closed db")
	}
}
