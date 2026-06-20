package tilepack

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"hash"
	"hash/fnv"
	"io"
	"log"
	"math"
	"os"
	"sort"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/paulmach/orb"
	"github.com/paulmach/orb/maptile"
	"github.com/protomaps/go-pmtiles/pmtiles"
)

// offsetLen records where a tile content blob was written in the temp data file.
type offsetLen struct {
	offset uint64
	length uint32
}

// pmtilesOutputter writes a PMTiles v3 archive.
//
// Tile data is accumulated in a temporary file during Save calls. On Close the
// outputter sorts the directory entries by Hilbert tile ID, performs run-length
// encoding on runs of consecutive identical tiles, builds the two-level
// (root + optional leaf) directory structure, and writes the final archive in
// one sequential pass: header → root dir → metadata → leaf dirs → tile data.
//
// The layout follows the PMTiles v3 specification:
//
//	[127-byte header][root directory][metadata][leaf directories][tile data]
type pmtilesOutputter struct {
	tileset        *roaring64.Bitmap    // set of all addressed tile IDs (for count reporting)
	hashFunc       hash.Hash            // FNV-128a; reset per tile for deduplication
	offsetMap      map[string]offsetLen // hash → position in tileData; drives dedup
	dataOffset     uint64               // running byte offset into tileData
	tileData       *os.File             // temp file accumulating raw tile blobs
	entries        []pmtiles.EntryV3    // one entry per Save call before RLE
	compressBuffer *bytes.Buffer
	compressor     *gzip.Writer
	header         pmtiles.HeaderV3
	metadata       *MbtilesMetadata // written into the JSON metadata section on Close
	centerSet      bool             // true once AssignSpatialMetadata has been called
	outFile        *os.File
	logger         *log.Logger
}

func (p *pmtilesOutputter) CreateTiles() error {
	return nil
}

// Save records a tile in the archive.
//
// If the tile's content has been seen before (same FNV-128a hash) the blob is
// reused and no bytes are written to the temp file. Otherwise the data is
// optionally gzip-compressed and appended. Either way one directory entry is
// appended; run-length encoding is applied later in Close once all entries are
// sorted by tile ID.
func (p *pmtilesOutputter) Save(tile maptile.Tile, data []byte) error {
	// Hilbert tile ID is the canonical ordering key used by the PMTiles spec.
	id := pmtiles.ZxyToID(uint8(tile.Z), tile.X, tile.Y)
	if p.tileset.Contains(id) {
		// Duplicate tile IDs produce two directory entries for the same ID, making
		// one unreachable via binary search. Reject early to keep the directory valid.
		return fmt.Errorf("duplicate tile %v (Hilbert ID %d)", tile, id)
	}
	p.tileset.Add(id)

	// Hash the raw (pre-compression) bytes for content-based deduplication.
	p.hashFunc.Reset()
	p.hashFunc.Write(data)
	var empty []byte
	sumString := string(p.hashFunc.Sum(empty))
	found, ok := p.offsetMap[sumString]

	if !ok {
		// New content: compress if needed, append to the temp data file.
		var newData []byte
		// Pass-through when the archive stores tiles uncompressed (e.g. PNG/JPEG),
		// or when the data already carries the gzip magic bytes (0x1f 0x8b) so we
		// do not double-compress.
		if p.header.TileCompression == pmtiles.NoCompression ||
			(len(data) >= 2 && data[0] == 0x1f && data[1] == 0x8b) {
			newData = data
		} else {
			p.compressBuffer.Reset()
			p.compressor.Reset(p.compressBuffer)
			if _, err := p.compressor.Write(data); err != nil {
				return fmt.Errorf("gzip write: %w", err)
			}
			if err := p.compressor.Close(); err != nil {
				return fmt.Errorf("gzip close: %w", err)
			}
			newData = p.compressBuffer.Bytes()
		}

		bytesWritten, err := p.tileData.Write(newData)
		if err != nil {
			return err
		}

		found = offsetLen{
			offset: p.dataOffset,
			length: uint32(bytesWritten),
		}
		p.offsetMap[sumString] = found
		p.dataOffset += uint64(bytesWritten)
	}

	p.entries = append(p.entries, pmtiles.EntryV3{
		TileID:    id,
		Offset:    found.offset,
		Length:    found.length,
		RunLength: 1,
	})

	return nil
}

// AssignSpatialMetadata sets the geographic extent and zoom range in the header.
// Coordinates are stored in the PMTiles binary format as integer values scaled
// by 1e7 (i.e. degrees × 10 000 000). Center defaults to the midpoint of the
// bounds at the minimum zoom if not overridden before Close.
func (p *pmtilesOutputter) AssignSpatialMetadata(bound orb.Bound, minZoom maptile.Zoom, maxZoom maptile.Zoom) error {
	p.header.MinZoom = uint8(minZoom)
	p.header.MaxZoom = uint8(maxZoom)
	p.header.MinLonE7 = int32(bound.Min[0] * 1e7)
	p.header.MinLatE7 = int32(bound.Min[1] * 1e7)
	p.header.MaxLonE7 = int32(bound.Max[0] * 1e7)
	p.header.MaxLatE7 = int32(bound.Max[1] * 1e7)
	p.centerSet = true
	return nil
}

// Close finalises the archive.
//
// Steps:
//  1. Sort entries by Hilbert tile ID (required by the spec for binary-search
//     lookup and for the Clustered flag to be valid).
//  2. Collapse runs of consecutive tiles that share the same offset (identical
//     content, contiguous IDs) into single entries with RunLength > 1.
//  3. Build the two-level directory (root + leaf pages) via optimizeDirectories.
//  4. Derive center coordinates if AssignSpatialMetadata was called.
//  5. Write: header → root dir → metadata JSON → leaf dirs → tile data blob.
func (p *pmtilesOutputter) Close() error {
	p.logger.Printf("Writing %d tiles to pmtiles", p.tileset.GetCardinality())

	// Step 1: sort by Hilbert tile ID so the directory is bsearch-able and the
	// archive qualifies as "clustered" per the PMTiles v3 spec.
	sort.Slice(p.entries, func(i, j int) bool {
		return p.entries[i].TileID < p.entries[j].TileID
	})

	// Step 2: run-length encode consecutive entries that point to the same tile
	// content blob (identical offset). After sorting, such runs are adjacent.
	// A single entry with RunLength=N replaces N individual entries, shrinking
	// the directory significantly for sparse or uniform tilesets.
	p.entries = runLengthEncodeEntries(p.entries)

	p.header.AddressedTilesCount = p.tileset.GetCardinality()
	p.header.TileEntriesCount = uint64(len(p.entries))
	p.header.TileContentsCount = uint64(len(p.offsetMap))

	// Step 3: pack the directory into root + optional leaf pages. The root must
	// fit in 16384 - HeaderV3LenBytes bytes so it can be fetched together with
	// the header in one HTTP range request.
	rootBytes, leavesBytes, numLeaves := optimizeDirectories(p.entries, 16384-pmtiles.HeaderV3LenBytes, pmtiles.Gzip)

	if numLeaves > 0 {
		p.logger.Printf("Root dir bytes: %d", len(rootBytes))
		p.logger.Printf("Leaves dir bytes: %d", len(leavesBytes))
		p.logger.Printf("Num leaf dirs: %d", numLeaves)
		p.logger.Printf("Total dir bytes: %d", len(rootBytes)+len(leavesBytes))
		p.logger.Printf("Average leaf dir bytes: %d", len(leavesBytes)/numLeaves)
		p.logger.Printf("Average bytes per addressed tile: %.2f",
			float64(len(rootBytes)+len(leavesBytes))/float64(p.tileset.GetCardinality()))
	} else {
		p.logger.Printf("Total dir bytes: %d", len(rootBytes))
		if p.tileset.GetCardinality() > 0 {
			p.logger.Printf("Average bytes per addressed tile: %.2f",
				float64(len(rootBytes))/float64(p.tileset.GetCardinality()))
		}
	}

	// Step 4: derive center and zoom range.
	//
	// When AssignSpatialMetadata was called, the caller has supplied explicit
	// zoom and bounds — use them verbatim and derive center from the midpoint.
	// When it was not called, infer MinZoom/MaxZoom from the actual tile data
	// (matching the reference setZoomCenterDefaults behavior) so that readers
	// see an accurate zoom range even without explicit metadata.
	if p.centerSet {
		p.header.CenterZoom = p.header.MinZoom
		// Compute midpoint in int64 to avoid int32 overflow for western/southern
		// hemispheres where two large-magnitude negative E7 values sum below -2^31.
		p.header.CenterLonE7 = int32((int64(p.header.MinLonE7) + int64(p.header.MaxLonE7)) / 2)
		p.header.CenterLatE7 = int32((int64(p.header.MinLatE7) + int64(p.header.MaxLatE7)) / 2)
	} else if len(p.entries) > 0 {
		minZ, _, _ := pmtiles.IDToZxy(p.entries[0].TileID)
		maxZ, _, _ := pmtiles.IDToZxy(p.entries[len(p.entries)-1].TileID)
		p.header.MinZoom = minZ
		p.header.MaxZoom = maxZ
	}

	// Build JSON metadata from the MbtilesMetadata fields. The PMTiles spec does
	// not prescribe a schema for the JSON blob; we mirror the keys used by the
	// reference pmtiles tooling (name, description, format, attribution, version)
	// so that downstream consumers (e.g. pmtiles CLI, MapLibre) can read them.
	jsonMetadata := p.buildJSONMetadata()

	metadataBytes, err := pmtiles.SerializeMetadata(jsonMetadata, pmtiles.Gzip)
	if err != nil {
		return fmt.Errorf("error serializing pmtiles metadata: %w", err)
	}

	// Step 5: assemble the final file layout.
	// The spec mandates this exact section order so readers can fetch the header
	// and root directory in a single range request ([0, 16384)).
	p.header.SpecVersion = 3
	p.header.Clustered = true // entries are sorted by tile ID (Step 1)
	p.header.InternalCompression = pmtiles.Gzip
	p.header.RootOffset = pmtiles.HeaderV3LenBytes
	p.header.RootLength = uint64(len(rootBytes))
	p.header.MetadataOffset = p.header.RootOffset + p.header.RootLength
	p.header.MetadataLength = uint64(len(metadataBytes))
	p.header.LeafDirectoryOffset = p.header.MetadataOffset + p.header.MetadataLength
	p.header.LeafDirectoryLength = uint64(len(leavesBytes))
	p.header.TileDataOffset = p.header.LeafDirectoryOffset + p.header.LeafDirectoryLength
	p.header.TileDataLength = p.dataOffset

	// Remove the temp file once we have finished copying its contents; it is
	// not needed after Close returns.
	defer func() {
		name := p.tileData.Name()
		p.tileData.Close()
		os.Remove(name)
	}()

	// Ensure the output file is always closed even on early error returns.
	// The explicit Close at the end of the happy path captures any flush error;
	// this defer is a safety net for the error paths.
	defer p.outFile.Close()

	if _, err = p.outFile.Write(pmtiles.SerializeHeader(p.header)); err != nil {
		return fmt.Errorf("error writing pmtiles header: %w", err)
	}
	if _, err = p.outFile.Write(rootBytes); err != nil {
		return fmt.Errorf("error writing pmtiles root directory: %w", err)
	}
	if _, err = p.outFile.Write(metadataBytes); err != nil {
		return fmt.Errorf("error writing pmtiles metadata: %w", err)
	}
	if _, err = p.outFile.Write(leavesBytes); err != nil {
		return fmt.Errorf("error writing pmtiles leaf directories: %w", err)
	}
	if _, err = p.tileData.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("error seeking to start of tile data: %w", err)
	}
	if _, err = io.Copy(p.outFile, p.tileData); err != nil {
		return fmt.Errorf("error copying tile data to output file: %w", err)
	}

	// Flush and close the output file explicitly so we can return any error.
	// The deferred Close is still registered but will be a no-op on a closed file.
	if err = p.outFile.Close(); err != nil {
		return fmt.Errorf("error closing output file: %w", err)
	}
	return nil
}

// buildJSONMetadata converts the MbtilesMetadata key/value pairs into the
// map[string]interface{} that pmtiles.SerializeMetadata expects. Keys known to
// the PMTiles ecosystem (name, description, attribution, format, version) are
// passed through directly; unknown keys are included as-is so callers can embed
// custom fields.
func (p *pmtilesOutputter) buildJSONMetadata() map[string]interface{} {
	meta := make(map[string]interface{})
	for _, key := range p.metadata.Keys() {
		if v, ok := p.metadata.Get(key); ok {
			meta[key] = v
		}
	}
	return meta
}

// runLengthEncodeEntries collapses runs of consecutive entries whose tile IDs
// are contiguous and which point to the same tile content (same offset in the
// data blob) into a single entry with RunLength equal to the run length.
//
// The PMTiles v3 spec defines a run as: a sequence of entries where each entry's
// TileID is exactly the previous entry's TileID + RunLength, and all entries
// share the same Offset. This allows very compact encoding of uniform regions
// (e.g. ocean tiles) without altering the tile data section.
//
// Entries must be sorted by TileID before calling this function.
func runLengthEncodeEntries(entries []pmtiles.EntryV3) []pmtiles.EntryV3 {
	if len(entries) == 0 {
		return entries
	}

	out := make([]pmtiles.EntryV3, 0, len(entries))
	cur := entries[0]

	for _, e := range entries[1:] {
		// A run continues when the next tile ID is exactly where the current run
		// ends (cur.TileID + cur.RunLength) and the content is identical (same
		// offset in the data file). We also guard against uint32 overflow.
		sameContent := e.Offset == cur.Offset
		contiguous := e.TileID == cur.TileID+uint64(cur.RunLength)
		wouldOverflow := uint64(cur.RunLength)+1 > math.MaxUint32

		if sameContent && contiguous && !wouldOverflow {
			cur.RunLength++
		} else {
			out = append(out, cur)
			cur = e
		}
	}
	out = append(out, cur)
	return out
}

// optimizeDirectories decides whether all entries fit in a single root page or
// require a two-level (root + leaf) layout.
//
// The PMTiles v3 spec reserves the first 16 KiB of the file for the fixed
// header plus the root directory so that readers can retrieve both in one HTTP
// range request. targetRootLen is that budget minus the header size.
//
// Case 1: all entries serialise to ≤ targetRootLen → root-only, no leaves.
// Case 3: entries are split into equally-sized leaf pages; the root holds only
//
//	leaf-directory pointers. Leaf page size grows by 20 % each iteration
//	until the root fits within the budget.
//
// (Case 2 — mixed tile entries and leaf pointers — is not yet implemented.)
func optimizeDirectories(entries []pmtiles.EntryV3, targetRootLen int, compression pmtiles.Compression) ([]byte, []byte, int) {
	// Case 1: attempt to fit everything into the root. Try regardless of entry
	// count — after RLE a large addressed-tile set may compress to well under the
	// target length. Only fall through to leaf layout if the serialized bytes
	// actually exceed the budget.
	testRootBytes := pmtiles.SerializeEntries(entries, compression)
	if len(testRootBytes) <= targetRootLen {
		return testRootBytes, make([]byte, 0), 0
	}

	// Case 3: root contains only leaf-directory pointers.
	leafSize := float32(len(entries)) / 3500
	if leafSize < 4096 {
		leafSize = 4096
	}

	for {
		rootBytes, leavesBytes, numLeaves := buildRootsLeaves(entries, int(leafSize), compression)
		if len(rootBytes) <= targetRootLen {
			return rootBytes, leavesBytes, numLeaves
		}
		leafSize *= 1.2
	}
}

// buildRootsLeaves partitions entries into leaf pages of leafSize entries each
// and builds a root directory whose entries point to those leaf pages.
//
// Each root entry has RunLength=0, which signals to readers that the entry is a
// leaf-directory pointer rather than a tile-data pointer.
func buildRootsLeaves(entries []pmtiles.EntryV3, leafSize int, compression pmtiles.Compression) ([]byte, []byte, int) {
	rootEntries := make([]pmtiles.EntryV3, 0)
	leavesBytes := make([]byte, 0)
	numLeaves := 0

	for i := 0; i < len(entries); i += leafSize {
		numLeaves++
		end := i + leafSize
		if end > len(entries) {
			end = len(entries)
		}
		serialized := pmtiles.SerializeEntries(entries[i:end], compression)

		rootEntries = append(rootEntries, pmtiles.EntryV3{
			TileID: entries[i].TileID,
			// Offset is relative to the start of the leaf-directory section.
			Offset:    uint64(len(leavesBytes)),
			Length:    uint32(len(serialized)),
			RunLength: 0, // 0 = leaf-directory pointer, not a tile entry
		})
		leavesBytes = append(leavesBytes, serialized...)
	}

	rootBytes := pmtiles.SerializeEntries(rootEntries, compression)
	return rootBytes, leavesBytes, numLeaves
}

// NewPmtilesOutputter creates a new pmtiles outputter that writes to dsn.
//
// outputType controls the tile type and compression stored in the header:
//   - "mvt": vector tiles (TileType=Mvt, TileCompression=Gzip)
//   - "png": PNG raster tiles (TileType=Png, TileCompression=NoCompression)
//
// metadata is written into the archive's JSON metadata section on Close.
func NewPmtilesOutputter(dsn string, outputType string, metadata *MbtilesMetadata) (*pmtilesOutputter, error) {
	tmpFile, err := os.CreateTemp("", "pmtiles-tiledata-*")
	if err != nil {
		return nil, fmt.Errorf("error creating temp file: %w", err)
	}

	outFile, err := os.Create(dsn)
	if err != nil {
		tmpFile.Close()
		os.Remove(tmpFile.Name())
		return nil, fmt.Errorf("error creating pmtiles output file: %w", err)
	}

	compressBuf := &bytes.Buffer{}
	compressor, err := gzip.NewWriterLevel(compressBuf, gzip.BestCompression)
	if err != nil {
		tmpFile.Close()
		os.Remove(tmpFile.Name())
		outFile.Close()
		return nil, fmt.Errorf("error creating gzip compressor: %w", err)
	}

	outputter := &pmtilesOutputter{
		outFile:        outFile,
		tileset:        roaring64.New(),
		hashFunc:       fnv.New128a(),
		tileData:       tmpFile,
		offsetMap:      make(map[string]offsetLen),
		entries:        make([]pmtiles.EntryV3, 0),
		header:         pmtiles.HeaderV3{},
		compressBuffer: compressBuf,
		compressor:     compressor,
		metadata:       metadata,
		logger:         log.New(os.Stderr, "pmtiles: ", 0),
	}

	switch outputType {
	case "mvt":
		outputter.header.TileType = pmtiles.Mvt
		outputter.header.TileCompression = pmtiles.Gzip
	case "png":
		outputter.header.TileType = pmtiles.Png
		outputter.header.TileCompression = pmtiles.NoCompression
	default:
		tmpFile.Close()
		os.Remove(tmpFile.Name())
		outFile.Close()
		return nil, fmt.Errorf("unsupported outputType %q: must be \"mvt\" or \"png\"", outputType)
	}

	return outputter, nil
}
