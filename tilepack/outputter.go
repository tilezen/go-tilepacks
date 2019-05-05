package tilepack

type TileOutputter interface {
	Save(tile *Tile, data []byte) error
	Close() error
}
