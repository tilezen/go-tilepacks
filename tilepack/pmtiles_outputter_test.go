package tilepack

import (
	"bytes"
	"compress/gzip"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/paulmach/orb"
	"github.com/paulmach/orb/maptile"
	"github.com/protomaps/go-pmtiles/pmtiles"
)

// readPmtilesFile parses a pmtiles archive and returns the header, root
// directory entries, and a function that reads a tile's raw bytes by tile ID.
func readPmtilesFile(t *testing.T, path string) (pmtiles.HeaderV3, []pmtiles.EntryV3, func(uint64) []byte) {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open pmtiles: %v", err)
	}
	t.Cleanup(func() { f.Close() })

	headerBuf := make([]byte, pmtiles.HeaderV3LenBytes)
	if _, err := io.ReadFull(f, headerBuf); err != nil {
		t.Fatalf("read header: %v", err)
	}
	header, err := pmtiles.DeserializeHeader(headerBuf)
	if err != nil {
		t.Fatalf("deserialize header: %v", err)
	}

	rootBuf := make([]byte, header.RootLength)
	if _, err := f.ReadAt(rootBuf, int64(header.RootOffset)); err != nil {
		t.Fatalf("read root dir: %v", err)
	}
	entries := pmtiles.DeserializeEntries(bytes.NewBuffer(rootBuf), pmtiles.Gzip)

	readTile := func(id uint64) []byte {
		// Walk entries to find the one matching id.
		for _, e := range entries {
			if e.TileID == id && e.RunLength > 0 {
				buf := make([]byte, e.Length)
				offset := int64(header.TileDataOffset + e.Offset)
				if _, err := f.ReadAt(buf, offset); err != nil {
					t.Fatalf("read tile data at offset %d: %v", offset, err)
				}
				return buf
			}
		}
		return nil
	}

	return header, entries, readTile
}

func newTestPmtilesOutputter(t *testing.T, outputType string) (*pmtilesOutputter, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "test.pmtiles")
	o, err := NewPmtilesOutputter(path, outputType, NewMbtilesMetadata(map[string]string{}))
	if err != nil {
		t.Fatalf("NewPmtilesOutputter: %v", err)
	}
	return o, path
}

func TestNewPmtilesOutputter_BadPath(t *testing.T) {
	// NewPmtilesOutputter must return an error when the output path cannot be created.
	_, err := NewPmtilesOutputter("/dev/null/cannot-create.pmtiles", "mvt", NewMbtilesMetadata(map[string]string{}))
	if err == nil {
		t.Fatal("expected error for unwritable path, got nil")
	}
}

func TestPmtilesOutputter_CreateTiles_IsNoop(t *testing.T) {
	// CreateTiles is a no-op for the pmtiles outputter (no schema to set up)
	// and must not return an error.
	o, _ := newTestPmtilesOutputter(t, "mvt")
	if err := o.CreateTiles(); err != nil {
		t.Fatalf("CreateTiles: %v", err)
	}
}

func TestPmtilesOutputter_AssignSpatialMetadata(t *testing.T) {
	// AssignSpatialMetadata must store min/max zoom and bounding box in the header
	// in units of 1e-7 degrees (as required by the pmtiles spec).
	o, _ := newTestPmtilesOutputter(t, "mvt")
	bounds := orb.Bound{Min: orb.Point{-180.0, -85.0}, Max: orb.Point{180.0, 85.0}}
	if err := o.AssignSpatialMetadata(bounds, 0, 14); err != nil {
		t.Fatalf("AssignSpatialMetadata: %v", err)
	}

	if o.header.MinZoom != 0 || o.header.MaxZoom != 14 {
		t.Errorf("zoom range: got %d-%d, want 0-14", o.header.MinZoom, o.header.MaxZoom)
	}
	if o.header.MinLonE7 != int32(-180.0*1e7) {
		t.Errorf("MinLonE7: got %d, want %d", o.header.MinLonE7, int32(-180.0*1e7))
	}
	if o.header.MaxLatE7 != int32(85.0*1e7) {
		t.Errorf("MaxLatE7: got %d, want %d", o.header.MaxLatE7, int32(85.0*1e7))
	}
}

func TestPmtilesOutputter_TileTypeHeader_MVT(t *testing.T) {
	// When outputType is "mvt", the header must declare TileType=Mvt and
	// TileCompression=Gzip.
	o, _ := newTestPmtilesOutputter(t, "mvt")
	if o.header.TileType != pmtiles.Mvt {
		t.Errorf("expected TileType Mvt, got %v", o.header.TileType)
	}
	if o.header.TileCompression != pmtiles.Gzip {
		t.Errorf("expected TileCompression Gzip, got %v", o.header.TileCompression)
	}
}

func TestPmtilesOutputter_TileTypeHeader_PNG(t *testing.T) {
	// When outputType is "png", the header must declare TileType=Png and
	// TileCompression=NoCompression (raster tiles are not gzip-compressed).
	o, _ := newTestPmtilesOutputter(t, "png")
	if o.header.TileType != pmtiles.Png {
		t.Errorf("expected TileType Png, got %v", o.header.TileType)
	}
	if o.header.TileCompression != pmtiles.NoCompression {
		t.Errorf("expected TileCompression NoCompression, got %v", o.header.TileCompression)
	}
}

func TestPmtilesOutputter_Save_WritesEntry(t *testing.T) {
	// Every saved tile must produce one entry in p.entries and one bit set in
	// the tile bitmap.  The offset map must have exactly one entry (not deduped).
	o, _ := newTestPmtilesOutputter(t, "mvt")
	tile := maptile.New(0, 0, 0)
	if err := o.Save(tile, []byte("tile-data")); err != nil {
		t.Fatalf("Save: %v", err)
	}

	if len(o.entries) != 1 {
		t.Errorf("expected 1 entry, got %d", len(o.entries))
	}
	if o.tileset.GetCardinality() != 1 {
		t.Errorf("expected 1 tile in bitmap, got %d", o.tileset.GetCardinality())
	}
	if len(o.offsetMap) != 1 {
		t.Errorf("expected 1 offset map entry, got %d", len(o.offsetMap))
	}
}

func TestPmtilesOutputter_Save_DeduplicatesByHash(t *testing.T) {
	// Two tiles with identical content must produce two entries (one per tile) but
	// only one entry in the offset map (deduplicated by FNV hash).
	o, _ := newTestPmtilesOutputter(t, "mvt")
	data := []byte("shared-data")
	if err := o.Save(maptile.New(0, 0, 1), data); err != nil {
		t.Fatalf("Save tile 1: %v", err)
	}
	if err := o.Save(maptile.New(1, 0, 1), data); err != nil {
		t.Fatalf("Save tile 2: %v", err)
	}

	if len(o.entries) != 2 {
		t.Errorf("expected 2 entries, got %d", len(o.entries))
	}
	if len(o.offsetMap) != 1 {
		t.Errorf("expected 1 offset map entry for identical data, got %d", len(o.offsetMap))
	}
}

func TestPmtilesOutputter_Save_CompressesUncompressedMVT(t *testing.T) {
	// For MVT output (TileCompression=Gzip), uncompressed input data must be
	// gzip-compressed before being written to the tile data temp file.
	o, _ := newTestPmtilesOutputter(t, "mvt")
	raw := []byte("uncompressed-mvt")
	if err := o.Save(maptile.New(0, 0, 0), raw); err != nil {
		t.Fatalf("Save: %v", err)
	}

	// Read back from the temp file and verify it's valid gzip.
	o.tileData.Seek(0, io.SeekStart)
	gz, err := gzip.NewReader(o.tileData)
	if err != nil {
		t.Fatalf("stored data is not gzip: %v", err)
	}
	got, _ := io.ReadAll(gz)
	if string(got) != string(raw) {
		t.Errorf("decompressed data mismatch: got %q, want %q", got, raw)
	}
}

func TestPmtilesOutputter_Save_PassesThroughAlreadyGzipped(t *testing.T) {
	// Data that is already gzip-compressed (magic bytes 0x1f 0x8b) must be stored
	// as-is without double-compression, regardless of the TileCompression setting.
	o, _ := newTestPmtilesOutputter(t, "mvt")

	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	w.Write([]byte("already-compressed"))
	w.Close()
	compressed := buf.Bytes()

	if err := o.Save(maptile.New(0, 0, 0), compressed); err != nil {
		t.Fatalf("Save: %v", err)
	}

	// The stored bytes must be identical to what we passed in.
	stored := make([]byte, len(compressed))
	o.tileData.Seek(0, io.SeekStart)
	io.ReadFull(o.tileData, stored)
	if !bytes.Equal(stored, compressed) {
		t.Error("already-compressed data was re-compressed")
	}
}

func TestPmtilesOutputter_Close_ProducesValidFile(t *testing.T) {
	// Close must write a valid pmtiles v3 file: correct magic, spec version 3,
	// and the tile data accessible via the directory.
	o, path := newTestPmtilesOutputter(t, "mvt")
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

	header, entries, readTile := readPmtilesFile(t, path)

	if header.SpecVersion != 3 {
		t.Errorf("expected spec version 3, got %d", header.SpecVersion)
	}
	if header.TileEntriesCount != 1 {
		t.Errorf("expected 1 tile entry, got %d", header.TileEntriesCount)
	}

	// The z=0 tile ID is 0.
	tileID := pmtiles.ZxyToID(0, 0, 0)
	found := false
	for _, e := range entries {
		if e.TileID == tileID {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("tile ID %d not found in root directory", tileID)
	}

	// The stored bytes must decompress to the original data (MVT outputter gzips).
	rawStored := readTile(tileID)
	if rawStored == nil {
		t.Fatal("could not read tile data from archive")
	}
	gr, err := gzip.NewReader(bytes.NewReader(rawStored))
	if err != nil {
		t.Fatalf("stored tile is not gzip: %v", err)
	}
	got, _ := io.ReadAll(gr)
	if !bytes.Equal(got, want) {
		t.Errorf("roundtrip data mismatch: got %q, want %q", got, want)
	}
}

func TestPmtilesOutputter_Close_NoTiles(t *testing.T) {
	// Close on an outputter with no tiles saved must succeed and produce a valid
	// pmtiles file with zero tile entries.
	o, path := newTestPmtilesOutputter(t, "mvt")
	if err := o.CreateTiles(); err != nil {
		t.Fatalf("CreateTiles: %v", err)
	}
	if err := o.Close(); err != nil {
		t.Fatalf("Close with no tiles: %v", err)
	}

	header, _, _ := readPmtilesFile(t, path)
	if header.TileEntriesCount != 0 {
		t.Errorf("expected 0 entries for empty archive, got %d", header.TileEntriesCount)
	}
}

func TestPmtilesOutputter_Close_EntriesSortedByTileID(t *testing.T) {
	// The pmtiles spec requires entries to be sorted by tile ID for efficient lookup.
	// We save tiles in reverse order and verify the written entries are sorted.
	o, path := newTestPmtilesOutputter(t, "mvt")
	if err := o.CreateTiles(); err != nil {
		t.Fatalf("CreateTiles: %v", err)
	}

	// Save z=1 tiles in reverse tile-ID order.
	tiles := []maptile.Tile{
		maptile.New(1, 1, 1), // tile ID > (0,0,1)
		maptile.New(0, 0, 1),
	}
	for _, tile := range tiles {
		if err := o.Save(tile, []byte("data")); err != nil {
			t.Fatalf("Save %v: %v", tile, err)
		}
	}
	if err := o.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	_, entries, _ := readPmtilesFile(t, path)
	for i := 1; i < len(entries); i++ {
		if entries[i].TileID < entries[i-1].TileID {
			t.Errorf("entries not sorted: entry[%d].TileID=%d < entry[%d].TileID=%d",
				i, entries[i].TileID, i-1, entries[i-1].TileID)
		}
	}
}

func TestPmtilesOutputter_Close_CountsMatchSaved(t *testing.T) {
	// The header counts (AddressedTilesCount, TileEntriesCount, TileContentsCount)
	// must reflect the tiles that were saved.  With all-distinct data, contents == entries.
	o, path := newTestPmtilesOutputter(t, "mvt")
	if err := o.CreateTiles(); err != nil {
		t.Fatalf("CreateTiles: %v", err)
	}

	for i := range 3 {
		tile := maptile.New(uint32(i), 0, 2)
		if err := o.Save(tile, []byte{byte(i), byte(i + 1)}); err != nil {
			t.Fatalf("Save: %v", err)
		}
	}
	if err := o.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	header, _, _ := readPmtilesFile(t, path)
	if header.AddressedTilesCount != 3 {
		t.Errorf("AddressedTilesCount: got %d, want 3", header.AddressedTilesCount)
	}
	if header.TileEntriesCount != 3 {
		t.Errorf("TileEntriesCount: got %d, want 3", header.TileEntriesCount)
	}
	if header.TileContentsCount != 3 {
		t.Errorf("TileContentsCount: got %d, want 3", header.TileContentsCount)
	}
}

func TestPmtilesOutputter_Close_DedupReducesContentsCount(t *testing.T) {
	// When two tiles share the same content, TileContentsCount must be 1
	// while TileEntriesCount remains 2 and AddressedTilesCount is 2.
	o, path := newTestPmtilesOutputter(t, "mvt")
	if err := o.CreateTiles(); err != nil {
		t.Fatalf("CreateTiles: %v", err)
	}

	shared := []byte("same-content")
	if err := o.Save(maptile.New(0, 0, 1), shared); err != nil {
		t.Fatalf("Save 1: %v", err)
	}
	if err := o.Save(maptile.New(1, 0, 1), shared); err != nil {
		t.Fatalf("Save 2: %v", err)
	}
	if err := o.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	header, _, _ := readPmtilesFile(t, path)
	if header.AddressedTilesCount != 2 {
		t.Errorf("AddressedTilesCount: got %d, want 2", header.AddressedTilesCount)
	}
	if header.TileEntriesCount != 2 {
		t.Errorf("TileEntriesCount: got %d, want 2", header.TileEntriesCount)
	}
	if header.TileContentsCount != 1 {
		t.Errorf("TileContentsCount: got %d, want 1 (dedup)", header.TileContentsCount)
	}
}
