package tilepack

import (
	"testing"

	"github.com/paulmach/orb"
	"github.com/paulmach/orb/maptile"
)

// collectTiles runs GenerateTiles and returns all tiles produced.
func collectTiles(bounds orb.Bound, zooms []maptile.Zoom, invertedY bool) []maptile.Tile {
	var tiles []maptile.Tile
	GenerateTiles(&GenerateTilesOptions{
		Bounds: bounds,
		Zooms:  zooms,
		ConsumerFunc: func(t maptile.Tile) {
			tiles = append(tiles, t)
		},
		InvertedY: invertedY,
	})
	return tiles
}

// collectRanges runs GenerateTileRanges and returns all (ll, ur, z) triples.
type tileRange struct {
	ll, ur maptile.Tile
	z      maptile.Zoom
}

func collectRanges(bounds orb.Bound, zooms []maptile.Zoom) []tileRange {
	var ranges []tileRange
	GenerateTileRanges(&GenerateRangesOptions{
		Bounds: bounds,
		Zooms:  zooms,
		ConsumerFunc: func(ll, ur maptile.Tile, z maptile.Zoom) {
			ranges = append(ranges, tileRange{ll, ur, z})
		},
	})
	return ranges
}

func TestGenerateTiles_WholeWorldZ0(t *testing.T) {
	// The whole world at zoom 0 is exactly one tile: 0/0/0.
	tiles := collectTiles(
		orb.Bound{Min: orb.Point{-180, -90}, Max: orb.Point{180, 90}},
		[]maptile.Zoom{0},
		false,
	)
	if len(tiles) != 1 {
		t.Fatalf("expected 1 tile at z0, got %d", len(tiles))
	}
	if tiles[0].X != 0 || tiles[0].Y != 0 || tiles[0].Z != 0 {
		t.Errorf("unexpected tile at z0: %+v", tiles[0])
	}
}

func TestGenerateTiles_WholeWorldZ1(t *testing.T) {
	// The whole world at zoom 1 is 4 tiles (2×2 grid).
	tiles := collectTiles(
		orb.Bound{Min: orb.Point{-180, -85.05112877980659}, Max: orb.Point{180, 85.05112877980659}},
		[]maptile.Zoom{1},
		false,
	)
	if len(tiles) != 4 {
		t.Fatalf("expected 4 tiles at z1, got %d", len(tiles))
	}
}

func TestGenerateTiles_SingleTile(t *testing.T) {
	// A tiny bounding box well inside a single tile should yield exactly one tile.
	tiles := collectTiles(
		orb.Bound{Min: orb.Point{-93.3, 44.9}, Max: orb.Point{-93.2, 45.0}},
		[]maptile.Zoom{5},
		false,
	)
	if len(tiles) != 1 {
		t.Fatalf("expected 1 tile, got %d", len(tiles))
	}
}

func TestGenerateTiles_LatitudeClamping(t *testing.T) {
	// Bounds that exceed the Web Mercator latitude limit (±85.05°) should be
	// clamped to the limit, not rejected or wrapped.  At z0 this still yields 1 tile.
	tiles := collectTiles(
		orb.Bound{Min: orb.Point{-180, -90}, Max: orb.Point{180, 90}},
		[]maptile.Zoom{0},
		false,
	)
	if len(tiles) == 0 {
		t.Fatal("expected tiles for out-of-range latitude bounds; got none")
	}
}

func TestGenerateTiles_InvertedY_False(t *testing.T) {
	// When InvertedY is false, tile Y follows the XYZ/Slippy-map convention:
	// Y=0 is the northernmost row.  A single tile strictly in the northern
	// hemisphere (lat > 0, above the equator) at z=1 must have Y=0.
	tiles := collectTiles(
		// Keep strictly north of equator to avoid straddling the Y=0/Y=1 boundary.
		orb.Bound{Min: orb.Point{-180, 1}, Max: orb.Point{-90, 85}},
		[]maptile.Zoom{1},
		false,
	)
	if len(tiles) == 0 {
		t.Fatal("expected at least one tile")
	}
	for _, tile := range tiles {
		if tile.Y != 0 {
			t.Errorf("north hemisphere (invertedY=false): expected Y=0, got Y=%d for tile %+v", tile.Y, tile)
		}
	}
}

func TestGenerateTiles_InvertedY_True(t *testing.T) {
	// When InvertedY is true the input Y is in TMS/GDAL convention (Y=0 is
	// southernmost). The south-hemisphere tile at z=1 in TMS is Y=0, so a
	// bounds strictly below the equator must yield only tiles with Y=0.
	tiles := collectTiles(
		// Strictly south of equator.
		orb.Bound{Min: orb.Point{-180, -85}, Max: orb.Point{-90, -1}},
		[]maptile.Zoom{1},
		true,
	)
	if len(tiles) == 0 {
		t.Fatal("expected at least one tile")
	}
	for _, tile := range tiles {
		if tile.Y != 0 {
			t.Errorf("south hemisphere (invertedY=true): expected Y=0, got Y=%d for tile %+v", tile.Y, tile)
		}
	}
}

func TestGenerateTileRanges_Antimeridian(t *testing.T) {
	// A bounding box that crosses the antimeridian (Min.X > Max.X) must be
	// split into two sub-boxes: one on the western side and one on the eastern
	// side, so tiles from both halves are covered.
	ranges := collectRanges(
		// Bounds from eastern Russia across the antimeridian to Alaska
		orb.Bound{Min: orb.Point{170, 50}, Max: orb.Point{-170, 72}},
		[]maptile.Zoom{2},
	)
	// Splitting across the antimeridian yields two ranges for a single zoom level.
	if len(ranges) != 2 {
		t.Fatalf("expected 2 ranges for antimeridian-crossing bounds at z2, got %d", len(ranges))
	}
}

func TestGenerateTiles_Antimeridian_NoDuplicates(t *testing.T) {
	// Tiles generated from antimeridian-crossing bounds must not duplicate any
	// tile coordinate; each (z, x, y) should appear exactly once.
	seen := make(map[maptile.Tile]int)
	GenerateTiles(&GenerateTilesOptions{
		Bounds: orb.Bound{Min: orb.Point{170, 50}, Max: orb.Point{-170, 72}},
		Zooms:  []maptile.Zoom{2},
		ConsumerFunc: func(t maptile.Tile) {
			seen[t]++
		},
		InvertedY: false,
	})
	for tile, count := range seen {
		if count > 1 {
			t.Errorf("tile %+v appeared %d times", tile, count)
		}
	}
}
