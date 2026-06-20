package http

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/paulmach/orb/maptile"
	"github.com/tilezen/go-tilepacks/tilepack"
)

// stubReader is a minimal MbtilesReader that returns canned responses for
// testing the HTTP handler without a real SQLite database.
type stubReader struct {
	data map[maptile.Tile][]byte
}

func (s *stubReader) Close() error { return nil }

func (s *stubReader) GetTile(tile maptile.Tile) (*tilepack.TileData, error) {
	d, ok := s.data[tile]
	if !ok {
		return &tilepack.TileData{Tile: tile, Data: nil}, nil
	}
	return &tilepack.TileData{Tile: tile, Data: &d}, nil
}

func (s *stubReader) VisitAllTiles(visitor func(maptile.Tile, []byte)) error { return nil }

func (s *stubReader) Metadata() (*tilepack.MbtilesMetadata, error) {
	return tilepack.NewMbtilesMetadata(map[string]string{}), nil
}

// TestParseTileFromPath verifies that the Tilezen URL pattern is parsed into
// the correct (z, x, y) tile coordinates.
func TestParseTileFromPath(t *testing.T) {
	cases := []struct {
		path    string
		z, x, y uint32
		wantErr bool
	}{
		{
			// Standard tile path must parse correctly.
			path: "/tilezen/vector/v1/512/all/5/10/12.mvt",
			z: 5, x: 10, y: 12,
		},
		{
			// Zoom 0, tile 0/0 is a valid degenerate case.
			path: "/tilezen/vector/v1/512/all/0/0/0.mvt",
			z: 0, x: 0, y: 0,
		},
		{
			// A path that doesn't match the pattern must return an error so the
			// handler can respond with 404 rather than a panic or zero tile.
			path:    "/not/a/tile/path",
			wantErr: true,
		},
		{
			// Missing the .mvt extension must not match.
			path:    "/tilezen/vector/v1/512/all/5/10/12",
			wantErr: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.path, func(t *testing.T) {
			tile, err := parseTileFromPath(tc.path)
			if tc.wantErr {
				if err == nil {
					t.Errorf("expected error for path %q, got nil", tc.path)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error for path %q: %v", tc.path, err)
			}
			if uint32(tile.Z) != tc.z || tile.X != tc.x || tile.Y != tc.y {
				t.Errorf("path %q: got z=%d x=%d y=%d, want z=%d x=%d y=%d",
					tc.path, tile.Z, tile.X, tile.Y, tc.z, tc.x, tc.y)
			}
		})
	}
}

// TestMbtilesHandler_TileFound verifies that the handler returns the tile bytes
// with the correct Content-Type when the tile exists in the reader.
func TestMbtilesHandler_TileFound(t *testing.T) {
	tile := maptile.New(10, 12, 5)
	tileData := []byte("protobuf-tile-data")

	reader := &stubReader{data: map[maptile.Tile][]byte{tile: tileData}}
	handler := MbtilesHandler(reader)

	req := httptest.NewRequest(http.MethodGet, "/tilezen/vector/v1/512/all/5/10/12.mvt", nil)
	req.Header.Set("Accept-Encoding", "gzip")
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rr.Code)
	}
	if ct := rr.Header().Get("Content-Type"); ct != "application/x-protobuf" {
		t.Errorf("expected Content-Type application/x-protobuf, got %q", ct)
	}
	if body := rr.Body.String(); body != string(tileData) {
		t.Errorf("unexpected body: %q", body)
	}
}

// TestMbtilesHandler_TileNotFound verifies that the handler returns 404 when
// the tile exists in the URL but is absent from the backing store.
func TestMbtilesHandler_TileNotFound(t *testing.T) {
	reader := &stubReader{data: map[maptile.Tile][]byte{}}
	handler := MbtilesHandler(reader)

	req := httptest.NewRequest(http.MethodGet, "/tilezen/vector/v1/512/all/5/10/12.mvt", nil)
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Errorf("expected 404, got %d", rr.Code)
	}
}

// TestMbtilesHandler_InvalidPath verifies that an unrecognized URL path gets a
// 404 rather than a 500 or panic.
func TestMbtilesHandler_InvalidPath(t *testing.T) {
	reader := &stubReader{data: map[maptile.Tile][]byte{}}
	handler := MbtilesHandler(reader)

	req := httptest.NewRequest(http.MethodGet, "/not/a/tile", nil)
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Errorf("expected 404, got %d", rr.Code)
	}
}

// TestMbtilesHandler_GzipHeader verifies that the Content-Encoding: gzip
// header is set when the client signals it accepts gzip-encoded responses.
// (Tiles in an MBTiles file are stored gzip-compressed and sent as-is.)
func TestMbtilesHandler_GzipHeader(t *testing.T) {
	tile := maptile.New(0, 0, 0)
	reader := &stubReader{data: map[maptile.Tile][]byte{tile: []byte("data")}}
	handler := MbtilesHandler(reader)

	req := httptest.NewRequest(http.MethodGet, "/tilezen/vector/v1/512/all/0/0/0.mvt", nil)
	req.Header.Set("Accept-Encoding", "gzip")
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	if ce := rr.Header().Get("Content-Encoding"); ce != "gzip" {
		t.Errorf("expected Content-Encoding: gzip, got %q", ce)
	}
}
