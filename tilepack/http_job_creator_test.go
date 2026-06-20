package tilepack

import (
	"compress/gzip"
	"bytes"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/paulmach/orb"
	"github.com/paulmach/orb/maptile"
)

// gzipBytes compresses b and returns the compressed form.
func gzipBytes(b []byte) []byte {
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	w.Write(b)
	w.Close()
	return buf.Bytes()
}

func TestHTTPError_ErrorAndString(t *testing.T) {
	// HTTPError must implement the error interface and return a human-readable status.
	e := &HTTPError{Code: 404, Status: "404 Not Found"}
	if e.Error() != "404 Not Found" {
		t.Errorf("Error(): got %q", e.Error())
	}
	if e.String() != "404 Not Found" {
		t.Errorf("String(): got %q", e.String())
	}
}

func TestDoHTTPWithRetry_Success(t *testing.T) {
	// A 200 response must be returned immediately without retrying.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		w.Write([]byte("ok"))
	}))
	defer srv.Close()

	req, _ := http.NewRequest("GET", srv.URL, nil)
	resp, err := doHTTPWithRetry(srv.Client(), req, 3)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.StatusCode != 200 {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}
	resp.Body.Close()
}

func TestDoHTTPWithRetry_ClientError_NoRetry(t *testing.T) {
	// 4xx responses are not retried — they are returned immediately as HTTPError
	// because retrying a client error will never succeed.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(404)
	}))
	defer srv.Close()

	req, _ := http.NewRequest("GET", srv.URL, nil)
	_, err := doHTTPWithRetry(srv.Client(), req, 3)
	if err == nil {
		t.Fatal("expected error for 404 response")
	}
	var httpErr *HTTPError
	if httpErr, ok := err.(*HTTPError); !ok || httpErr.Code != 404 {
		t.Errorf("expected HTTPError with code 404, got %T %v", err, err)
	}
	_ = httpErr
}

func TestDoHTTPWithRetry_ServerError_ExhaustsRetries(t *testing.T) {
	// 5xx responses are retried. When all retries are exhausted an HTTPError is returned.
	// We use nRetries=1 so the server is called once, returns 503, and the function
	// gives up after the single attempt.
	callCount := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		w.WriteHeader(503)
	}))
	defer srv.Close()

	client := &http.Client{Timeout: 2 * time.Second}
	req, _ := http.NewRequest("GET", srv.URL, nil)
	_, err := doHTTPWithRetry(client, req, 1)
	if err == nil {
		t.Fatal("expected error after exhausting retries")
	}
	if callCount != 1 {
		t.Errorf("expected 1 server call with nRetries=1, got %d", callCount)
	}
}

func TestNewFileTransportXYZJobGenerator_BadRoot(t *testing.T) {
	// NewFileTransportXYZJobGenerator must return an error if the root path
	// does not exist, preventing silent misconfiguration.
	_, err := NewFileTransportXYZJobGenerator(
		"/nonexistent/path",
		"file://{z}/{x}/{y}.pbf",
		orb.Bound{Min: orb.Point{-180, -85}, Max: orb.Point{180, 85}},
		[]maptile.Zoom{0},
		5*time.Second,
		false,
		false,
	)
	if err == nil {
		t.Fatal("expected error for non-existent root directory")
	}
}

func TestNewFileTransportXYZJobGenerator_RootIsFile(t *testing.T) {
	// NewFileTransportXYZJobGenerator must return an error when root is a file
	// rather than a directory.
	f, _ := os.CreateTemp(t.TempDir(), "notadir")
	f.Close()

	_, err := NewFileTransportXYZJobGenerator(
		f.Name(),
		"file://{z}/{x}/{y}.pbf",
		orb.Bound{Min: orb.Point{-180, -85}, Max: orb.Point{180, 85}},
		[]maptile.Zoom{0},
		5*time.Second,
		false,
		false,
	)
	if err == nil {
		t.Fatal("expected error when root is a file")
	}
}

func TestXYZJobGenerator_CreateJobs_URLTemplate(t *testing.T) {
	// CreateJobs must expand {z}, {x}, {y} placeholders in the URL template
	// and send one TileRequest per tile in the bounds.
	gen, err := NewXYZJobGenerator(
		"https://example.com/tiles/{z}/{x}/{y}.pbf",
		orb.Bound{Min: orb.Point{-180, -85}, Max: orb.Point{180, 85}},
		[]maptile.Zoom{0},
		5*time.Second,
		false,
		false,
		"pbf",
	)
	if err != nil {
		t.Fatalf("NewXYZJobGenerator: %v", err)
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

	// At z=0 there is exactly one tile.
	if len(reqs) != 1 {
		t.Fatalf("expected 1 job, got %d", len(reqs))
	}
	expected := "https://example.com/tiles/0/0/0.pbf"
	if reqs[0].URL != expected {
		t.Errorf("expected URL %q, got %q", expected, reqs[0].URL)
	}
}

func TestXYZJobGenerator_CreateJobs_InvertedY(t *testing.T) {
	// When invertedY=true, tile Y values in the jobs must use TMS numbering.
	// At z=1 the southwest quadrant in TMS has Y=0; in XYZ it has Y=1.
	gen, err := NewXYZJobGenerator(
		"{z}/{x}/{y}",
		// Strictly south of equator so only the bottom row of tiles is included.
		orb.Bound{Min: orb.Point{-180, -85}, Max: orb.Point{-90, -1}},
		[]maptile.Zoom{1},
		5*time.Second,
		true,  // invertedY
		false,
		"pbf",
	)
	if err != nil {
		t.Fatalf("NewXYZJobGenerator: %v", err)
	}

	jobs := make(chan *TileRequest, 10)
	go func() {
		gen.CreateJobs(jobs)
		close(jobs)
	}()

	for r := range jobs {
		if r.Tile.Y != 0 {
			t.Errorf("invertedY=true: expected TMS Y=0 for south tile, got Y=%d in URL %s", r.Tile.Y, r.URL)
		}
	}
}

// writeTileToServer serves tile data from a directory using an HTTP test server
// so CreateWorker can fetch it via http://.
func setupTileServer(t *testing.T, path string, data []byte, contentEncoding string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if contentEncoding != "" {
			w.Header().Set("Content-Encoding", contentEncoding)
		}
		w.WriteHeader(200)
		w.Write(data)
	}))
}

func runWorker(t *testing.T, gen JobGenerator, tileURL string, tile maptile.Tile) *TileResponse {
	t.Helper()
	jobs := make(chan *TileRequest, 1)
	results := make(chan *TileResponse, 1)

	worker, err := gen.CreateWorker()
	if err != nil {
		t.Fatalf("CreateWorker: %v", err)
	}

	jobs <- &TileRequest{Tile: tile, URL: tileURL}
	close(jobs)

	go worker(0, jobs, results)

	select {
	case r := <-results:
		return r
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for worker result")
		return nil
	}
}

func TestXYZWorker_PlainResponse_EnsureGzip(t *testing.T) {
	// When the server returns uncompressed data and ensureGzip=true, the worker
	// must gzip the data before sending it to results.  The stored bytes must
	// decompress to the original payload.
	original := []byte("raw-tile-data")
	srv := setupTileServer(t, "/0/0/0.pbf", original, "")
	defer srv.Close()

	gen, _ := NewXYZJobGenerator(srv.URL+"/{z}/{x}/{y}.pbf", orb.Bound{}, nil, 5*time.Second, false, true, "pbf")
	resp := runWorker(t, gen, srv.URL+"/0/0/0.pbf", maptile.New(0, 0, 0))

	// Decompress and verify the stored data matches the original.
	r, err := gzip.NewReader(bytes.NewReader(resp.Data))
	if err != nil {
		t.Fatalf("result is not gzip-compressed: %v", err)
	}
	var buf bytes.Buffer
	buf.ReadFrom(r)
	if buf.String() != string(original) {
		t.Errorf("decompressed data mismatch: got %q, want %q", buf.String(), original)
	}
}

func TestXYZWorker_PlainResponse_NoEnsureGzip(t *testing.T) {
	// When ensureGzip=false, uncompressed server responses must be stored as-is.
	original := []byte("raw-tile-data")
	srv := setupTileServer(t, "/0/0/0.pbf", original, "")
	defer srv.Close()

	gen, _ := NewXYZJobGenerator(srv.URL+"/{z}/{x}/{y}.pbf", orb.Bound{}, nil, 5*time.Second, false, false, "pbf")
	resp := runWorker(t, gen, srv.URL+"/0/0/0.pbf", maptile.New(0, 0, 0))

	if string(resp.Data) != string(original) {
		t.Errorf("expected raw data %q, got %q", original, resp.Data)
	}
}

func TestXYZWorker_GzipResponse_PBF_KeptCompressed(t *testing.T) {
	// When the server responds with Content-Encoding: gzip and the format is PBF,
	// the worker must store the compressed bytes as-is (MBTiles stores PBF gzipped).
	original := []byte("pbf-tile-data")
	compressed := gzipBytes(original)
	srv := setupTileServer(t, "/0/0/0.pbf", compressed, "gzip")
	defer srv.Close()

	gen, _ := NewXYZJobGenerator(srv.URL+"/{z}/{x}/{y}.pbf", orb.Bound{}, nil, 5*time.Second, false, true, "pbf")
	resp := runWorker(t, gen, srv.URL+"/0/0/0.pbf", maptile.New(0, 0, 0))

	// Stored bytes must still be compressed — they are identical to what the server sent.
	if !bytes.Equal(resp.Data, compressed) {
		t.Errorf("PBF: expected compressed bytes to be kept as-is")
	}
}

func TestXYZWorker_GzipResponse_Raster_Decompressed(t *testing.T) {
	// Regression for issue #37: when the server returns Content-Encoding: gzip
	// for a raster format (e.g. png), the worker must decompress the data before
	// storing it.  Storing compressed raster bytes produces invalid PNG/JPG files.
	original := []byte("png-tile-data")
	compressed := gzipBytes(original)
	srv := setupTileServer(t, "/0/0/0.png", compressed, "gzip")
	defer srv.Close()

	gen, _ := NewXYZJobGenerator(srv.URL+"/{z}/{x}/{y}.png", orb.Bound{}, nil, 5*time.Second, false, false, "png")
	resp := runWorker(t, gen, srv.URL+"/0/0/0.png", maptile.New(0, 0, 0))

	// Stored bytes must be the raw, uncompressed tile data.
	if string(resp.Data) != string(original) {
		t.Errorf("raster: expected decompressed data %q, got %q", original, resp.Data)
	}
}

func TestFileTransportXYZWorker_FetchesTile(t *testing.T) {
	// NewFileTransportXYZJobGenerator must serve tiles from a local directory
	// using the file:// transport, producing the expected tile data in results.
	// The URL template uses file:/// with paths relative to the registered root.
	dir := t.TempDir()
	tileData := []byte("local-tile")

	// Write a tile at {root}/0/0/0.pbf.
	tilePath := filepath.Join(dir, "0", "0")
	os.MkdirAll(tilePath, 0755)
	os.WriteFile(filepath.Join(tilePath, "0.pbf"), tileData, 0644)

	gen, err := NewFileTransportXYZJobGenerator(
		dir,
		"file:///{z}/{x}/{y}.pbf",
		orb.Bound{Min: orb.Point{-180, -85}, Max: orb.Point{180, 85}},
		[]maptile.Zoom{0},
		5*time.Second,
		false,
		false,
	)
	if err != nil {
		t.Fatalf("NewFileTransportXYZJobGenerator: %v", err)
	}

	resp := runWorker(t, gen, "file:///0/0/0.pbf", maptile.New(0, 0, 0))
	if string(resp.Data) != string(tileData) {
		t.Errorf("expected %q, got %q", tileData, resp.Data)
	}
}
