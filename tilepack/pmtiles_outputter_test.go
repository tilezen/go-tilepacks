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
		// Walk entries. A tile-data entry covers RunLength consecutive tile IDs
		// starting at TileID, so a hit occurs when id falls in that range.
		// (RunLength=0 entries are leaf-directory pointers, not tile data.)
		for _, e := range entries {
			if e.RunLength > 0 && id >= e.TileID && id < e.TileID+uint64(e.RunLength) {
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
	// (The two tile IDs are non-contiguous so RLE does not merge them.)
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

func TestPmtilesOutputter_Close_ClusteredFlagSet(t *testing.T) {
	// The Clustered flag must be true in the header because entries are sorted by
	// tile ID. Readers use this flag to determine whether binary-search lookup is
	// valid; writing an unclustered archive is a spec violation.
	o, path := newTestPmtilesOutputter(t, "mvt")
	o.CreateTiles()
	o.Save(maptile.New(0, 0, 0), []byte("d"))
	if err := o.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	header, _, _ := readPmtilesFile(t, path)
	if !header.Clustered {
		t.Error("expected Clustered=true in header")
	}
}

func TestPmtilesOutputter_Close_RunLengthEncoding(t *testing.T) {
	// Consecutive tiles with identical content and contiguous Hilbert IDs must
	// be collapsed into a single directory entry with RunLength > 1.  This
	// shrinks the directory for uniform regions (e.g. ocean tiles) without
	// altering the tile data section.
	//
	// At z=1 the four tiles have Hilbert IDs 1,2,3,4 (contiguous after sorting).
	// Saving them all with the same bytes means all four should collapse into one
	// entry with RunLength=4.
	o, path := newTestPmtilesOutputter(t, "mvt")
	o.CreateTiles()

	shared := []byte("ocean-tile")
	for _, tile := range []maptile.Tile{
		maptile.New(0, 0, 1), // Hilbert ID 1
		maptile.New(0, 1, 1), // Hilbert ID 2
		maptile.New(1, 1, 1), // Hilbert ID 3
		maptile.New(1, 0, 1), // Hilbert ID 4
	} {
		if err := o.Save(tile, shared); err != nil {
			t.Fatalf("Save: %v", err)
		}
	}
	if err := o.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	header, entries, _ := readPmtilesFile(t, path)

	// Four addressed tiles, but dedup leaves only one content blob.
	if header.AddressedTilesCount != 4 {
		t.Errorf("AddressedTilesCount: got %d, want 4", header.AddressedTilesCount)
	}
	if header.TileContentsCount != 1 {
		t.Errorf("TileContentsCount: got %d, want 1", header.TileContentsCount)
	}
	// RLE must have reduced four entries to one.
	if len(entries) != 1 {
		t.Errorf("expected 1 RLE entry in directory, got %d", len(entries))
	}
	if entries[0].RunLength != 4 {
		t.Errorf("RunLength: got %d, want 4", entries[0].RunLength)
	}
}

func TestRunLengthEncodeEntries_Empty(t *testing.T) {
	// runLengthEncodeEntries must not panic on an empty slice.
	result := runLengthEncodeEntries([]pmtiles.EntryV3{})
	if len(result) != 0 {
		t.Errorf("expected empty result, got %d entries", len(result))
	}
}

func TestRunLengthEncodeEntries_NoRun(t *testing.T) {
	// Entries with different content (different offsets) must not be merged.
	entries := []pmtiles.EntryV3{
		{TileID: 0, Offset: 0, Length: 10, RunLength: 1},
		{TileID: 1, Offset: 10, Length: 10, RunLength: 1},
		{TileID: 2, Offset: 20, Length: 10, RunLength: 1},
	}
	result := runLengthEncodeEntries(entries)
	if len(result) != 3 {
		t.Errorf("expected 3 entries (no run), got %d", len(result))
	}
}

func TestRunLengthEncodeEntries_PartialRun(t *testing.T) {
	// A run of two identical tiles followed by a different tile must produce
	// one merged entry and one standalone entry.
	entries := []pmtiles.EntryV3{
		{TileID: 0, Offset: 0, Length: 5, RunLength: 1},
		{TileID: 1, Offset: 0, Length: 5, RunLength: 1}, // same offset → run
		{TileID: 2, Offset: 5, Length: 5, RunLength: 1}, // different offset → new entry
	}
	result := runLengthEncodeEntries(entries)
	if len(result) != 2 {
		t.Errorf("expected 2 entries, got %d", len(result))
	}
	if result[0].RunLength != 2 {
		t.Errorf("first entry RunLength: got %d, want 2", result[0].RunLength)
	}
	if result[1].RunLength != 1 {
		t.Errorf("second entry RunLength: got %d, want 1", result[1].RunLength)
	}
}

func TestRunLengthEncodeEntries_NonContiguousIDs(t *testing.T) {
	// Identical content at non-contiguous tile IDs (gap in the ID sequence) must
	// NOT be merged — they are separate tiles in different locations.
	entries := []pmtiles.EntryV3{
		{TileID: 0, Offset: 0, Length: 5, RunLength: 1},
		{TileID: 5, Offset: 0, Length: 5, RunLength: 1}, // same content, but ID gap
	}
	result := runLengthEncodeEntries(entries)
	if len(result) != 2 {
		t.Errorf("expected 2 entries for non-contiguous IDs, got %d", len(result))
	}
}

func TestPmtilesOutputter_Close_CenterDefaultsToMidpoint(t *testing.T) {
	// When AssignSpatialMetadata is called, Close must derive center as the midpoint
	// of the bounds at the caller-supplied min zoom. Uses western-hemisphere
	// coordinates to catch int32 overflow in E7 midpoint arithmetic.
	o, path := newTestPmtilesOutputter(t, "mvt")
	o.CreateTiles()
	o.Save(maptile.New(0, 0, 0), []byte("d"))
	o.AssignSpatialMetadata(
		orb.Bound{Min: orb.Point{-122.5, 37.7}, Max: orb.Point{-122.4, 37.8}},
		2, 10,
	)
	if err := o.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	header, _, _ := readPmtilesFile(t, path)

	if header.CenterZoom != 2 {
		t.Errorf("CenterZoom: got %d, want 2 (caller-supplied min zoom)", header.CenterZoom)
	}
	// midpoint of -122.5 and -122.4 = -122.45; must not overflow int32
	wantLon := int32(-122.45 * 1e7)
	if header.CenterLonE7 != wantLon {
		t.Errorf("CenterLonE7: got %d (%.7f°), want %d (%.7f°)",
			header.CenterLonE7, float64(header.CenterLonE7)/1e7,
			wantLon, float64(wantLon)/1e7)
	}
	wantLat := int32(37.75 * 1e7)
	if header.CenterLatE7 != wantLat {
		t.Errorf("CenterLatE7: got %d, want %d", header.CenterLatE7, wantLat)
	}
}

func TestPmtilesOutputter_Close_MetadataWritten(t *testing.T) {
	// Metadata keys passed to NewPmtilesOutputter must appear in the JSON
	// metadata section of the written archive.
	path := filepath.Join(t.TempDir(), "meta.pmtiles")
	o, err := NewPmtilesOutputter(path, "mvt", NewMbtilesMetadata(map[string]string{
		"name":   "test-tileset",
		"format": "mvt",
	}))
	if err != nil {
		t.Fatalf("NewPmtilesOutputter: %v", err)
	}
	o.CreateTiles()
	o.Save(maptile.New(0, 0, 0), []byte("d"))
	if err := o.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Read the metadata section back and verify the keys are present.
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer f.Close()

	headerBuf := make([]byte, pmtiles.HeaderV3LenBytes)
	io.ReadFull(f, headerBuf)
	header, _ := pmtiles.DeserializeHeader(headerBuf)

	metaBuf := make([]byte, header.MetadataLength)
	f.ReadAt(metaBuf, int64(header.MetadataOffset))

	metaJSON, err := pmtiles.DeserializeMetadata(bytes.NewReader(metaBuf), pmtiles.Gzip)
	if err != nil {
		t.Fatalf("deserialize metadata: %v", err)
	}
	if metaJSON["name"] != "test-tileset" {
		t.Errorf("metadata name: got %v, want \"test-tileset\"", metaJSON["name"])
	}
	if metaJSON["format"] != "mvt" {
		t.Errorf("metadata format: got %v, want \"mvt\"", metaJSON["format"])
	}
}

func TestNewPmtilesOutputter_CleansUpTempFileOnOutputError(t *testing.T) {
	// If the output file cannot be created, NewPmtilesOutputter must not leak
	// the temp data file.  We check indirectly: if the constructor returns an
	// error the caller should not need to clean up anything.
	_, err := NewPmtilesOutputter("/dev/null/cannot-create.pmtiles", "mvt", NewMbtilesMetadata(map[string]string{}))
	if err == nil {
		t.Fatal("expected error for unwritable path")
	}
}

func TestPmtilesOutputter_Close_NoSpatialMetadata(t *testing.T) {
	// If AssignSpatialMetadata is never called, Close must still produce a valid
	// archive. Spatial bounds remain zero. MinZoom/MaxZoom are inferred from the
	// actual tile data. Center fields remain zero (centerSet is false).
	o, path := newTestPmtilesOutputter(t, "mvt")
	o.CreateTiles()
	o.Save(maptile.New(0, 0, 0), []byte("d"))
	if err := o.Close(); err != nil {
		t.Fatalf("Close without spatial metadata: %v", err)
	}

	header, _, _ := readPmtilesFile(t, path)
	if header.CenterZoom != 0 || header.CenterLonE7 != 0 || header.CenterLatE7 != 0 {
		t.Errorf("expected zero center when AssignSpatialMetadata not called, got zoom=%d lon=%d lat=%d",
			header.CenterZoom, header.CenterLonE7, header.CenterLatE7)
	}
	// MinZoom and MaxZoom must be inferred from the single z=0 tile.
	if header.MinZoom != 0 || header.MaxZoom != 0 {
		t.Errorf("expected MinZoom=0 MaxZoom=0 for z=0 tile, got min=%d max=%d", header.MinZoom, header.MaxZoom)
	}
}

func TestPmtilesOutputter_PNG_EndToEnd(t *testing.T) {
	// For PNG output (TileCompression=NoCompression) tile bytes must be stored
	// verbatim — no gzip wrapping — and must round-trip identically.
	o, path := newTestPmtilesOutputter(t, "png")
	o.CreateTiles()

	want := []byte("\x89PNG\r\n\x1a\n") // PNG magic bytes
	tile := maptile.New(0, 0, 0)
	if err := o.Save(tile, want); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if err := o.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	header, _, readTile := readPmtilesFile(t, path)
	if header.TileType != pmtiles.Png {
		t.Errorf("TileType: got %v, want Png", header.TileType)
	}
	if header.TileCompression != pmtiles.NoCompression {
		t.Errorf("TileCompression: got %v, want NoCompression", header.TileCompression)
	}

	tileID := pmtiles.ZxyToID(0, 0, 0)
	got := readTile(tileID)
	if got == nil {
		t.Fatal("tile not found in archive")
	}
	// Bytes must be identical to what was passed to Save — no compression applied.
	if !bytes.Equal(got, want) {
		t.Errorf("PNG tile data mismatch: got %x, want %x", got, want)
	}
}

func TestPmtilesOutputter_Close_RLEReadback(t *testing.T) {
	// After RLE, reading any tile in a run must return the correct bytes.
	// Saves all four z=1 tiles with identical data, then verifies the last
	// tile in the run (Hilbert ID 4) is readable via the single RLE entry.
	o, path := newTestPmtilesOutputter(t, "mvt")
	o.CreateTiles()

	shared := []byte("ocean")
	for _, tile := range []maptile.Tile{
		maptile.New(0, 0, 1), // Hilbert ID 1
		maptile.New(0, 1, 1), // Hilbert ID 2
		maptile.New(1, 1, 1), // Hilbert ID 3
		maptile.New(1, 0, 1), // Hilbert ID 4
	} {
		if err := o.Save(tile, shared); err != nil {
			t.Fatalf("Save: %v", err)
		}
	}
	if err := o.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	_, _, readTile := readPmtilesFile(t, path)

	// Read the last tile in the run (ID 4, tile (1,0,1)) — it falls inside the single RLE entry.
	lastID := pmtiles.ZxyToID(1, 1, 0)
	got := readTile(lastID)
	if got == nil {
		t.Fatal("last tile in RLE run not found via readTile")
	}
	// The stored bytes are gzip-compressed; decompress to verify.
	gr, err := gzip.NewReader(bytes.NewReader(got))
	if err != nil {
		t.Fatalf("decompress: %v", err)
	}
	decompressed, _ := io.ReadAll(gr)
	if !bytes.Equal(decompressed, shared) {
		t.Errorf("RLE readback mismatch: got %q, want %q", decompressed, shared)
	}
}

func TestNewPmtilesOutputter_UnknownOutputType(t *testing.T) {
	// NewPmtilesOutputter must return an error for unrecognised output types rather
	// than silently producing an archive with TileType=0 (UnknownTileType), which
	// would be rejected or misidentified by pmtiles readers.
	path := filepath.Join(t.TempDir(), "test.pmtiles")
	_, err := NewPmtilesOutputter(path, "jpeg", NewMbtilesMetadata(map[string]string{}))
	if err == nil {
		t.Fatal("expected error for unknown outputType \"jpeg\", got nil")
	}
}

func TestPmtilesOutputter_Save_DuplicateTileIDReturnsError(t *testing.T) {
	// Saving the same tile coordinate twice must return an error. Duplicate tile IDs
	// produce two directory entries for the same ID; binary-search lookup in
	// findTile returns the first entry, making the second permanently unreachable.
	o, _ := newTestPmtilesOutputter(t, "mvt")
	tile := maptile.New(0, 0, 0)
	if err := o.Save(tile, []byte("first")); err != nil {
		t.Fatalf("first Save: %v", err)
	}
	if err := o.Save(tile, []byte("second")); err == nil {
		t.Fatal("expected error on duplicate tile save, got nil")
	}
}

func TestPmtilesOutputter_Close_ZoomRangeInferredFromTiles(t *testing.T) {
	// When AssignSpatialMetadata is not called, Close must still derive
	// MinZoom and MaxZoom from the Hilbert IDs of the first and last entries,
	// matching the reference setZoomCenterDefaults behavior.
	o, path := newTestPmtilesOutputter(t, "mvt")
	o.CreateTiles()
	// Save tiles at z=5 only.
	o.Save(maptile.New(0, 0, 5), []byte("a"))
	o.Save(maptile.New(1, 0, 5), []byte("b"))
	if err := o.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	header, _, _ := readPmtilesFile(t, path)
	if header.MinZoom != 5 {
		t.Errorf("MinZoom: got %d, want 5", header.MinZoom)
	}
	if header.MaxZoom != 5 {
		t.Errorf("MaxZoom: got %d, want 5", header.MaxZoom)
	}
}

