package main

import (
	"io"
	"testing"

	"github.com/paulmach/orb"
	"github.com/paulmach/orb/maptile"
	"github.com/schollz/progressbar/v3"
	"github.com/tilezen/go-tilepacks/tilepack"
)

// stubOutputter records Save calls so processResults tests can verify behavior
// without writing to disk or a real database.
type stubOutputter struct {
	saved []savedTile
}

type savedTile struct {
	tile maptile.Tile
	data []byte
}

func (s *stubOutputter) CreateTiles() error { return nil }
func (s *stubOutputter) Close() error       { return nil }
func (s *stubOutputter) AssignSpatialMetadata(_ orb.Bound, _, _ maptile.Zoom) error {
	return nil
}
func (s *stubOutputter) Save(tile maptile.Tile, data []byte) error {
	s.saved = append(s.saved, savedTile{tile, data})
	return nil
}

func TestProcessResults(t *testing.T) {
	// processResults must call Save on the outputter for every TileResponse
	// received before the results channel is closed, and must not skip tiles.
	out := &stubOutputter{}
	results := make(chan *tilepack.TileResponse, 3)

	results <- &tilepack.TileResponse{Tile: maptile.New(0, 0, 0), Data: []byte("a")}
	results <- &tilepack.TileResponse{Tile: maptile.New(1, 0, 1), Data: []byte("b")}
	close(results)

	bar := progressbar.NewOptions(2, progressbar.OptionSetWriter(io.Discard))
	processResults(results, out, bar)

	if len(out.saved) != 2 {
		t.Errorf("expected 2 saved tiles, got %d", len(out.saved))
	}
}
