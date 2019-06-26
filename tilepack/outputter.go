package tilepack

type TileOutputter interface {
	CreateTiles() error
	Save(tile *Tile, data []byte) error
	Close() error
}
