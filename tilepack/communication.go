package tilepack

type TileRequest struct {
	Tile *Tile
	URL  string
}

type TileResponse struct {
	Tile    *Tile
	Data    []byte
	Elapsed float64
}
