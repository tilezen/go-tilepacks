package tilepack

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/paulmach/orb"
	"github.com/paulmach/orb/maptile"
	"github.com/protomaps/go-pmtiles/pmtiles"
	"hash"
	"hash/fnv"
	"io"
	"os"
)

type offsetLen struct {
	offset uint64
	length uint32
}

type pmtilesOutputter struct {
	tileset        *roaring64.Bitmap
	hashFunc       hash.Hash
	offsetMap      map[string]offsetLen
	tileData       *os.File
	entries        []pmtiles.EntryV3
	compressBuffer *bytes.Buffer
	compressor     *gzip.Writer
	header         pmtiles.HeaderV3
	outFile        *os.File
}

func (p *pmtilesOutputter) CreateTiles() error {
	return nil
}

func (p *pmtilesOutputter) Save(tile maptile.Tile, data []byte) error {
	flippedY := (1 << uint(tile.Z)) - 1 - tile.Y

	// Store tile IDs so we can iterate through them in the correct order later
	id := pmtiles.ZxyToID(uint8(tile.Z), tile.X, flippedY)
	p.tileset.Add(id)

	// Hash the tile data to use as a key for dedupe
	p.hashFunc.Reset()
	p.hashFunc.Write(data)
	var empty []byte
	sumString := string(p.hashFunc.Sum(empty))
	found, ok := p.offsetMap[sumString]

	// If the hash is not found, append the tile data to the temp file and store the
	// offset+length
	if !ok {
		offset, err := p.tileData.Seek(0, io.SeekEnd)
		if err != nil {
			return err
		}

		// Compress the data if it isn't already
		var newData []byte
		if len(data) >= 2 && data[0] == 31 && data[1] == 139 {
			// data is already compressed
			newData = data
		} else {
			// data is not compressed, compress it
			p.compressBuffer.Reset()
			p.compressor.Reset(p.compressBuffer)
			p.compressor.Write(data)
			p.compressor.Close()
			newData = p.compressBuffer.Bytes()
		}

		bytesWritten, err := p.tileData.Write(newData)
		if err != nil {
			return err
		}

		found = offsetLen{
			offset: uint64(offset),
			length: uint32(bytesWritten),
		}

		p.offsetMap[sumString] = found
	}

	// Add a pmtiles entry for the tile
	entry := pmtiles.EntryV3{
		TileID:    id,
		Offset:    found.offset,
		Length:    found.length,
		RunLength: 1, // TODO: implement run-length encoding. I think this requires sorting by tile ID first, though.
	}
	p.entries = append(p.entries, entry)

	return nil
}

func (p *pmtilesOutputter) AssignSpatialMetadata(bound orb.Bound, minZoom maptile.Zoom, maxZoom maptile.Zoom) error {
	return nil
}

func (p *pmtilesOutputter) Close() error {
	fmt.Printf("Writing %d tiles to pmtiles\n", p.tileset.GetCardinality())
	p.header.AddressedTilesCount = p.tileset.GetCardinality()
	p.header.TileEntriesCount = uint64(len(p.entries))
	p.header.TileContentsCount = uint64(len(p.offsetMap))

	defer p.outFile.Close()

	rootBytes, leavesBytes, numLeaves := optimizeDirectories(p.entries, 16384-pmtiles.HeaderV3LenBytes, pmtiles.Gzip)

	if numLeaves > 0 {
		fmt.Println("Root dir bytes: ", len(rootBytes))
		fmt.Println("Leaves dir bytes: ", len(leavesBytes))
		fmt.Println("Num leaf dirs: ", numLeaves)
		fmt.Println("Total dir bytes: ", len(rootBytes)+len(leavesBytes))
		fmt.Println("Average leaf dir bytes: ", len(leavesBytes)/numLeaves)
		fmt.Printf("Average bytes per addressed tile: %.2f\n", float64(len(rootBytes)+len(leavesBytes))/float64(p.tileset.GetCardinality()))
	} else {
		fmt.Println("Total dir bytes: ", len(rootBytes))
		fmt.Printf("Average bytes per addressed tile: %.2f\n", float64(len(rootBytes))/float64(p.tileset.GetCardinality()))
	}

	jsonMetadata := make(map[string]interface{}) // TODO

	metadataBytes, err := pmtiles.SerializeMetadata(jsonMetadata, pmtiles.Gzip)
	if err != nil {
		return fmt.Errorf("error serializing pmtiles metadata: %v", err)
	}

	offset, err := p.tileData.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}

	p.header.InternalCompression = pmtiles.Gzip
	if p.header.TileType == pmtiles.Mvt {
		p.header.InternalCompression = pmtiles.Gzip
	}
	p.header.RootOffset = pmtiles.HeaderV3LenBytes
	p.header.RootLength = uint64(len(rootBytes))
	p.header.MetadataOffset = p.header.RootOffset + p.header.RootLength
	p.header.MetadataLength = uint64(len(metadataBytes))
	p.header.LeafDirectoryOffset = p.header.MetadataOffset + p.header.MetadataLength
	p.header.LeafDirectoryLength = uint64(len(leavesBytes))
	p.header.TileDataOffset = p.header.LeafDirectoryOffset + p.header.LeafDirectoryLength
	p.header.TileDataLength = uint64(offset)

	headerBytes := pmtiles.SerializeHeader(p.header)

	_, err = p.outFile.Write(headerBytes)
	if err != nil {
		return fmt.Errorf("error writing pmtiles header: %w", err)
	}

	_, err = p.outFile.Write(rootBytes)
	if err != nil {
		return fmt.Errorf("error writing pmtiles root directory: %w", err)
	}

	_, err = p.outFile.Write(metadataBytes)
	if err != nil {
		return fmt.Errorf("error writing pmtiles metadata: %w", err)
	}

	_, err = p.outFile.Write(leavesBytes)
	if err != nil {
		return fmt.Errorf("error writing pmtiles leaf directory: %w", err)
	}

	_, err = p.tileData.Seek(offset, io.SeekStart)
	if err != nil {
		return fmt.Errorf("error seeking to start of tile data: %w", err)
	}

	_, err = io.Copy(p.outFile, p.tileData)
	if err != nil {
		return fmt.Errorf("error copying tile data to outfile: %w", err)
	}

	return nil
}

func optimizeDirectories(entries []pmtiles.EntryV3, targetRootLen int, compression pmtiles.Compression) ([]byte, []byte, int) {
	if len(entries) < 16384 {
		testRootBytes := pmtiles.SerializeEntries(entries, compression)
		if len(testRootBytes) <= targetRootLen {
			// Case 1: The entire directory fits into the target length
			return testRootBytes, make([]byte, 0), 0
		}
	}

	// TODO: Case 2: Mixed tile entries/directory entries in root

	// Case 3: Root directory is leaf pointers only
	// Use an iterative method, increasing the size of the leaf directory until the root fits

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

func buildRootsLeaves(entries []pmtiles.EntryV3, leafSize int, compression pmtiles.Compression) ([]byte, []byte, int) {
	rootEntries := make([]pmtiles.EntryV3, 0)
	leavesBytes := make([]byte, 0)
	numLeaves := 0

	for i := 0; i < len(entries); i += leafSize {
		numLeaves++
		end := i + leafSize
		if i+leafSize > len(entries) {
			end = len(entries)
		}
		serialized := pmtiles.SerializeEntries(entries[i:end], compression)

		rootEntries = append(rootEntries, pmtiles.EntryV3{
			TileID:    entries[i].TileID,
			Offset:    uint64(len(leavesBytes)),
			Length:    uint32(len(serialized)),
			RunLength: 0,
		})
		leavesBytes = append(leavesBytes, serialized...)
	}

	rootBytes := pmtiles.SerializeEntries(rootEntries, compression)
	return rootBytes, leavesBytes, numLeaves
}

func NewPmtilesOutputter(dsn string, metadata *MbtilesMetadata) (*pmtilesOutputter, error) {
	tmpFile, err := os.CreateTemp("", "pmtiles-tiledata")
	if err != nil {
		return nil, fmt.Errorf("error creating temp file: %w", err)
	}

	outFile, err := os.Create(dsn)
	if err != nil {
		return nil, fmt.Errorf("error creating pmtiles output file: %w", err)
	}

	outputter := &pmtilesOutputter{
		outFile:   outFile,
		tileset:   roaring64.New(),
		hashFunc:  fnv.New128a(),
		tileData:  tmpFile,
		offsetMap: make(map[string]offsetLen),
		entries:   make([]pmtiles.EntryV3, 0),
		header:    pmtiles.HeaderV3{},
	}
	return outputter, nil
}
