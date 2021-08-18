package tilepack

import (
	"database/sql"
	"log"

	_ "github.com/mattn/go-sqlite3" // Register sqlite3 database driver
)

type TileData struct {
	Tile *Tile
	Data *[]byte
}

type MbtilesReader interface {
	Close() error
	GetTile(tile *Tile) (*TileData, error)
	VisitAllTiles(visitor func(*Tile, []byte)) error
}

type tileDataFromDatabase struct {
	Data *[]byte
}

func NewMbtilesReader(dsn string) (MbtilesReader, error) {
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, err
	}
	return NewMbtilesReaderWithDatabase(db)
}

func NewMbtilesReaderWithDatabase(db *sql.DB) (MbtilesReader, error) {
	return &mbtilesReader{db: db}, nil
}

type mbtilesReader struct {
	MbtilesReader
	db *sql.DB
}

// Close gracefully tears down the mbtiles connection.
func (o *mbtilesReader) Close() error {
	var err error

	if o.db != nil {
		if err2 := o.db.Close(); err2 != nil {
			err = err2
		}
	}

	return err
}

// GetTile returns data for the given tile.
func (o *mbtilesReader) GetTile(tile *Tile) (*TileData, error) {
	var data []byte

	result := o.db.QueryRow("SELECT tile_data FROM tiles WHERE zoom_level=? AND tile_column=? AND tile_row=? LIMIT 1", tile.Z, tile.X, tile.Y)
	err := result.Scan(&data)

	if err != nil {
		if err == sql.ErrNoRows {
			blankTile := &TileData{Tile: tile, Data: nil}
			return blankTile, nil
		}
		return nil, err
	}

	tileData := &TileData{
		Tile: tile,
		Data: &data,
	}

	return tileData, nil
}

// VisitAllTiles runs the given function on all tiles in this mbtiles archive.
func (o *mbtilesReader) VisitAllTiles(visitor func(*Tile, []byte)) error {
	rows, err := o.db.Query("SELECT zoom_level, tile_column, tile_row, tile_data FROM tiles")
	if err != nil {
		return err
	}

	var z, x, y uint
	for rows.Next() {
		data := []byte{}
		err := rows.Scan(&z, &x, &y, &data)
		if err != nil {
			log.Printf("Couldn't scan row: %+v", err)
		}

		t := &Tile{Z: z, X: x, Y: y}
		visitor(t, data)
	}
	return nil
}
