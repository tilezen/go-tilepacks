package tilepack

import (
	"database/sql"
	"fmt"
	"log"
	"strconv"
	"strings"

	_ "github.com/mattn/go-sqlite3" // Register sqlite3 database driver
	"github.com/paulmach/orb"
	"github.com/paulmach/orb/maptile"
)

type TileData struct {
	Tile maptile.Tile
	Data *[]byte
}

type MbtilesReader interface {
	Close() error
	GetTile(tile maptile.Tile) (*TileData, error)
	VisitAllTiles(visitor func(maptile.Tile, []byte)) error
	Metadata() (*MbtilesMetadata, error)
}

type MbtilesMetadata map[string]string

func (m *MbtilesMetadata) Bounds() (orb.Bound, error) {

	str_bounds, exists := m["bounds"]

	if !exists {
		return nil, fmt.Errorf("Metadata is missing bounds")
	}

	parts := strings.Split(str_bounds, ",")

	if len(parts) != 4 {
		return nil, fmt.Errorf("Invalid bounds metadata")
	}

	return nil, nil
}

func (m *MbtilesMetadata) MinZoom() (uint, error) {

	str_minzoom, exists := *m["minzoom"]

	if !exists {
		return 0, fmt.Errorf("Metadata is missing minzoom")
	}

	i, err := strconv.Atoi(str_minzoom)

	if err != nil {
		return 0, fmt.Errorf("Failed to parse minzoom value, %w", err)
	}

	return uint(i), nil
}

func (m *MbtilesMetadata) MaxZoom() (uint, error) {

	str_maxzoom, exists := *m["maxzoom"]

	if !exists {
		return 0, fmt.Errorf("Metadata is missing maxzoom")
	}

	i, err := strconv.Atoi(str_maxzoom)

	if err != nil {
		return 0, fmt.Errorf("Failed to parse maxzoom value, %w", err)
	}

	return uint(i), nil
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
func (o *mbtilesReader) GetTile(tile maptile.Tile) (*TileData, error) {
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
func (o *mbtilesReader) VisitAllTiles(visitor func(maptile.Tile, []byte)) error {
	rows, err := o.db.Query("SELECT zoom_level, tile_column, tile_row, tile_data FROM tiles")
	if err != nil {
		return err
	}

	var x, y uint32
	var z maptile.Zoom
	for rows.Next() {
		data := []byte{}
		err := rows.Scan(&z, &x, &y, &data)
		if err != nil {
			log.Printf("Couldn't scan row: %+v", err)
		}

		t := maptile.New(x, y, z)
		visitor(t, data)
	}
	return nil
}
