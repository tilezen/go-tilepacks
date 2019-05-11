package tilepack

import (
	"database/sql"

	_ "github.com/mattn/go-sqlite3" // Register sqlite3 database driver
)

type TileData struct {
	Data *[]byte
}

func NewMbtilesReader(dsn string) (*MbtilesReader, error) {
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, err
	}

	return &MbtilesReader{db: db}, nil
}

type MbtilesReader struct {
	db *sql.DB
}

func (o *MbtilesReader) Close() error {
	var err error

	if o.db != nil {
		if err2 := o.db.Close(); err2 != nil {
			err = err2
		}
	}

	return err
}

func (o *MbtilesReader) GetTile(tile *Tile) (*TileData, error) {
	var data []byte

	result := o.db.QueryRow("SELECT tile_data FROM tiles WHERE zoom_level=? AND tile_column=? AND tile_row=? LIMIT 1", tile.Z, tile.X, tile.Y)
	err := result.Scan(&data)

	if err != nil {
		if err == sql.ErrNoRows {
			return &TileData{Data: nil}, nil
		}
		return nil, err
	}

	return &TileData{
		Data: &data,
	}, nil
}
