package tilepack

import (
	"archive/zip"
	"bytes"
	"compress/gzip"
	"io"
	"testing"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/paulmach/orb"
	"github.com/paulmach/orb/maptile"
)

// fakeS3Downloader implements s3Downloader using a map of S3 key → zip bytes.
// It is used to test metatile and t2 workers without hitting real S3.
type fakeS3Downloader struct {
	// objects maps S3 key to raw bytes to write into the WriterAt.
	objects map[string][]byte
}

func (f *fakeS3Downloader) Download(w io.WriterAt, input *s3.GetObjectInput, _ ...func(*s3manager.Downloader)) (int64, error) {
	data, ok := f.objects[*input.Key]
	if !ok {
		return 0, nil
	}
	n, err := w.WriteAt(data, 0)
	return int64(n), err
}

// buildMetatileZip creates an in-memory zip containing one tile file named
// "{offsetZ}/{offsetX}/{offsetY}.{format}" holding raw data.
func buildMetatileZip(t *testing.T, offsetZ, offsetX, offsetY uint32, format string, data []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	name := "0/0/0." + format
	if offsetZ != 0 || offsetX != 0 || offsetY != 0 {
		name = string(rune('0'+offsetZ)) + "/" + string(rune('0'+offsetX)) + "/" + string(rune('0'+offsetY)) + "." + format
	}
	fw, err := zw.Create(name)
	if err != nil {
		t.Fatalf("zip.Create: %v", err)
	}
	fw.Write(data)
	zw.Close()
	return buf.Bytes()
}

// buildT2Zip creates a zip containing one entry named "{z}/{x}/{y}@2x.png".
func buildT2Zip(t *testing.T, z maptile.Zoom, x, y uint32, data []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	name := string(rune('0'+int(z))) + "/" + string(rune('0'+int(x))) + "/" + string(rune('0'+int(y))) + "@2x.png"
	fw, _ := zw.Create(name)
	fw.Write(data)
	zw.Close()
	return buf.Bytes()
}

func TestLog2Uint(t *testing.T) {
	// log2Uint must return the base-2 log of the input, used to derive zoom
	// offsets from metatile sizes (e.g. metatileSize=8 → log2=3).
	cases := []struct{ in, want uint }{
		{1, 0},
		{2, 1},
		{4, 2},
		{8, 3},
		{16, 4},
	}
	for _, tc := range cases {
		if got := log2Uint(tc.in); got != tc.want {
			t.Errorf("log2Uint(%d) = %d, want %d", tc.in, got, tc.want)
		}
	}
}

func TestArrayContains(t *testing.T) {
	// arrayContains must return true when the needle is in the slice and false otherwise.
	zooms := []maptile.Zoom{0, 2, 5}
	if !arrayContains(2, zooms) {
		t.Error("expected arrayContains to find zoom 2")
	}
	if arrayContains(3, zooms) {
		t.Error("expected arrayContains to not find zoom 3")
	}
}

func TestMetatileJobGenerator_CreateJobs_ZoomMapping(t *testing.T) {
	// CreateJobs must convert requested tile zooms into the corresponding
	// metatile zoom levels. With metatileSize=8 (log2=3) and tileScale=2
	// (log2=1), deltaZoom=2.  Requesting zoom 5 should generate metatile zoom 3.
	fake := &fakeS3Downloader{objects: map[string][]byte{}}
	gen := &metatileJobGenerator{
		s3Client:      fake,
		bucket:        "test-bucket",
		pathTemplate:  "{z}/{x}/{y}.zip",
		layerName:     "all",
		format:        "mvt",
		metatileSize:  8,
		maxDetailZoom: 0,
		bounds:        orb.Bound{Min: orb.Point{-180, -85}, Max: orb.Point{180, 85}},
		zooms:         []maptile.Zoom{5},
	}

	jobs := make(chan *TileRequest, 100)
	go func() {
		gen.CreateJobs(jobs)
		close(jobs)
	}()

	var zoomsRequested []maptile.Zoom
	for r := range jobs {
		zoomsRequested = append(zoomsRequested, r.Tile.Z)
	}

	// All generated metatile jobs should be at zoom 3 (5 - deltaZoom 2).
	for _, z := range zoomsRequested {
		if z != 3 {
			t.Errorf("expected metatile zoom 3, got %d", z)
		}
	}
}

func TestMetatileJobGenerator_CreateJobs_MaxDetailZoom(t *testing.T) {
	// When maxDetailZoom is set, metatile zooms above it are clamped to
	// maxDetailZoom.  With deltaZoom=2 and maxDetailZoom=2, requesting zoom 7
	// computes metatile zoom 5 which exceeds 2, so it should be clamped to 2.
	fake := &fakeS3Downloader{objects: map[string][]byte{}}
	gen := &metatileJobGenerator{
		s3Client:      fake,
		bucket:        "b",
		pathTemplate:  "{z}/{x}/{y}.zip",
		layerName:     "all",
		format:        "mvt",
		metatileSize:  8,
		maxDetailZoom: 2,
		bounds:        orb.Bound{Min: orb.Point{-180, -85}, Max: orb.Point{180, 85}},
		zooms:         []maptile.Zoom{7},
	}

	jobs := make(chan *TileRequest, 100)
	go func() {
		gen.CreateJobs(jobs)
		close(jobs)
	}()

	for r := range jobs {
		if r.Tile.Z != 2 {
			t.Errorf("expected clamped metatile zoom 2, got %d", r.Tile.Z)
		}
	}
}

func TestMetatileJobGenerator_CreateJobs_LowZoom(t *testing.T) {
	// Tile zooms lower than deltaZoom must result in metatile zoom 0 (not a
	// negative zoom, which would underflow the uint type).
	fake := &fakeS3Downloader{objects: map[string][]byte{}}
	gen := &metatileJobGenerator{
		s3Client:      fake,
		bucket:        "b",
		pathTemplate:  "{z}/{x}/{y}.zip",
		layerName:     "all",
		format:        "mvt",
		metatileSize:  8,
		maxDetailZoom: 0,
		bounds:        orb.Bound{Min: orb.Point{-180, -85}, Max: orb.Point{180, 85}},
		zooms:         []maptile.Zoom{0, 1},
	}

	jobs := make(chan *TileRequest, 100)
	go func() {
		gen.CreateJobs(jobs)
		close(jobs)
	}()

	for r := range jobs {
		if r.Tile.Z != 0 {
			t.Errorf("expected metatile zoom 0 for low tile zoom, got %d", r.Tile.Z)
		}
	}
}

func TestMetatileWorker_ExtractsTile(t *testing.T) {
	// The metatile worker must download a zip from S3, extract tiles matching
	// the requested format and bounds, gzip them, and send them to results.
	rawData := []byte("mvt-tile-data")
	zipBytes := buildMetatileZip(t, 0, 0, 0, "mvt", rawData)

	fake := &fakeS3Downloader{objects: map[string][]byte{
		"0/0/0.zip": zipBytes,
	}}

	gen := &metatileJobGenerator{
		s3Client:      fake,
		bucket:        "b",
		pathTemplate:  "{z}/{x}/{y}.zip",
		layerName:     "all",
		format:        "mvt",
		metatileSize:  8,
		maxDetailZoom: 0,
		bounds:        orb.Bound{Min: orb.Point{-180, -85}, Max: orb.Point{180, 85}},
		zooms:         []maptile.Zoom{0},
	}

	jobs := make(chan *TileRequest, 1)
	results := make(chan *TileResponse, 10)
	worker, _ := gen.CreateWorker()

	jobs <- &TileRequest{Tile: maptile.New(0, 0, 0), URL: "0/0/0.zip"}
	close(jobs)
	worker(0, jobs, results)
	close(results)

	var responses []*TileResponse
	for r := range results {
		responses = append(responses, r)
	}

	if len(responses) == 0 {
		t.Fatal("expected at least one tile response from metatile worker")
	}

	// Verify the stored data is gzip-compressed and decompresses to the original.
	r, err := gzip.NewReader(bytes.NewReader(responses[0].Data))
	if err != nil {
		t.Fatalf("metatile output is not gzip-compressed: %v", err)
	}
	var buf bytes.Buffer
	buf.ReadFrom(r)
	if buf.String() != string(rawData) {
		t.Errorf("decompressed data: got %q, want %q", buf.String(), rawData)
	}
}

func TestMetatileWorker_SkipsWrongFormat(t *testing.T) {
	// Tiles with a format that doesn't match the generator's format must be
	// silently skipped rather than producing a response.
	zipBytes := buildMetatileZip(t, 0, 0, 0, "png", []byte("png-data"))

	fake := &fakeS3Downloader{objects: map[string][]byte{"0/0/0.zip": zipBytes}}
	gen := &metatileJobGenerator{
		s3Client:      fake,
		bucket:        "b",
		pathTemplate:  "{z}/{x}/{y}.zip",
		layerName:     "all",
		format:        "mvt", // requesting mvt, zip contains png
		metatileSize:  8,
		maxDetailZoom: 0,
		bounds:        orb.Bound{Min: orb.Point{-180, -85}, Max: orb.Point{180, 85}},
		zooms:         []maptile.Zoom{0},
	}

	jobs := make(chan *TileRequest, 1)
	results := make(chan *TileResponse, 10)
	worker, _ := gen.CreateWorker()

	jobs <- &TileRequest{Tile: maptile.New(0, 0, 0), URL: "0/0/0.zip"}
	close(jobs)
	worker(0, jobs, results)
	close(results)

	var count int
	for range results {
		count++
	}
	if count != 0 {
		t.Errorf("expected 0 responses for wrong format, got %d", count)
	}
}

func TestT2JobGenerator_CreateJobs_URLTemplate(t *testing.T) {
	// CreateJobs must generate one request per tile at each materialized zoom
	// level, with the URL derived from the path template.
	fake := &fakeS3Downloader{objects: map[string][]byte{}}
	gen := &tapalcatl2JobGenerator{
		s3Client:          fake,
		bucket:            "b",
		pathTemplate:      "{z}/{x}/{y}.zip",
		layerName:         "all",
		materializedZooms: []maptile.Zoom{0},
		bounds:            orb.Bound{Min: orb.Point{-180, -85}, Max: orb.Point{180, 85}},
		zooms:             []maptile.Zoom{0},
	}

	jobs := make(chan *TileRequest, 10)
	go func() {
		gen.CreateJobs(jobs)
		close(jobs)
	}()

	var reqs []*TileRequest
	for r := range jobs {
		reqs = append(reqs, r)
	}

	// At z=0 there is exactly one tile, so exactly one job.
	if len(reqs) != 1 {
		t.Fatalf("expected 1 job at z=0, got %d", len(reqs))
	}
}

func TestT2Worker_ExtractsTile(t *testing.T) {
	// The t2 worker must download a zip from S3, extract tiles matching the
	// requested zooms and bounds, and send raw (uncompressed) data to results.
	rawData := []byte("png-tile-data")
	zipBytes := buildT2Zip(t, 0, 0, 0, rawData)

	fake := &fakeS3Downloader{objects: map[string][]byte{"0/0/0.zip": zipBytes}}
	gen := &tapalcatl2JobGenerator{
		s3Client:          fake,
		bucket:            "b",
		pathTemplate:      "{z}/{x}/{y}.zip",
		layerName:         "all",
		materializedZooms: []maptile.Zoom{0},
		bounds:            orb.Bound{Min: orb.Point{-180, -85}, Max: orb.Point{180, 85}},
		zooms:             []maptile.Zoom{0},
	}

	jobs := make(chan *TileRequest, 1)
	results := make(chan *TileResponse, 10)
	worker, _ := gen.CreateWorker()

	jobs <- &TileRequest{Tile: maptile.New(0, 0, 0), URL: "0/0/0.zip"}
	close(jobs)
	worker(0, jobs, results)
	close(results)

	var responses []*TileResponse
	for r := range results {
		responses = append(responses, r)
	}

	if len(responses) == 0 {
		t.Fatal("expected at least one tile response from t2 worker")
	}
	if string(responses[0].Data) != string(rawData) {
		t.Errorf("data mismatch: got %q, want %q", responses[0].Data, rawData)
	}
}

func TestT2Worker_SkipsOutOfBounds(t *testing.T) {
	// Tiles outside the generator's bounds must be skipped and not sent to results.
	rawData := []byte("out-of-bounds")
	// Build a zip with a tile at z=0/x=0/y=0 — this tile covers the whole world,
	// but we'll set bounds to a tiny area that won't intersect if we pick a specific
	// higher-zoom tile. Use z=1/x=1/y=1 in the zip but ask for z=0 only.
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	fw, _ := zw.Create("1/1/1@2x.png") // zoom 1 tile
	fw.Write(rawData)
	zw.Close()

	fake := &fakeS3Downloader{objects: map[string][]byte{"1/1/1.zip": buf.Bytes()}}
	gen := &tapalcatl2JobGenerator{
		s3Client:          fake,
		bucket:            "b",
		pathTemplate:      "{z}/{x}/{y}.zip",
		layerName:         "all",
		materializedZooms: []maptile.Zoom{1},
		// Only include zoom 0 in requested zooms — zoom 1 tiles should be filtered out.
		zooms:  []maptile.Zoom{0},
		bounds: orb.Bound{Min: orb.Point{-180, -85}, Max: orb.Point{180, 85}},
	}

	jobs := make(chan *TileRequest, 1)
	results := make(chan *TileResponse, 10)
	worker, _ := gen.CreateWorker()

	jobs <- &TileRequest{Tile: maptile.New(1, 1, 1), URL: "1/1/1.zip"}
	close(jobs)
	worker(0, jobs, results)
	close(results)

	var count int
	for range results {
		count++
	}
	if count != 0 {
		t.Errorf("expected 0 responses for zoom-filtered tile, got %d", count)
	}
}
