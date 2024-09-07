package tilepack

import (
	"github.com/paulmach/orb"
	"github.com/paulmach/orb/maptile"
)

type TileOutputter interface {
	CreateTiles() error
	Save(tile maptile.Tile, data []byte) error
	AssignSpatialMetadata(orb.Bound, maptile.Zoom, maptile.Zoom) error
	Close() error
}
