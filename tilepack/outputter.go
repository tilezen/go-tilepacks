package tilepack

import (
	"github.com/paulmach/orb/maptile"
)

type TileOutputter interface {
	CreateTiles() error
	Save(tile maptile.Tile, data []byte) error
	Close() error
}
