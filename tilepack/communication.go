package tilepack

import (
	"github.com/paulmach/orb/maptile"
)

type TileRequest struct {
	Tile maptile.Tile
	URL  string
}

type TileResponse struct {
	Tile    maptile.Tile
	Data    []byte
	Elapsed float64
}
