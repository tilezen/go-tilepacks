package tilepack

import (
	"crypto/md5"
	"database/sql"
	"encoding/hex"
	"fmt"
	"math"
	"strconv"

	_ "github.com/mattn/go-sqlite3" // Register sqlite3 database driver
	"github.com/paulmach/orb"
	"github.com/paulmach/orb/maptile"
)

func NewMbtilesOutputter(dsn string, batchSize int, metadata *MbtilesMetadata) (*mbtilesOutputter, error) {
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, err
	}

	return &mbtilesOutputter{db: db, batchSize: batchSize, metadata: metadata}, nil
}

type mbtilesOutputter struct {
	TileOutputter
	db         *sql.DB
	txn        *sql.Tx
	hasTiles   bool
	batchCount int
	batchSize  int
	metadata   *MbtilesMetadata
}

func (o *mbtilesOutputter) Close() error {
	var err error

	// Save the metadata
	for name, value := range o.metadata.metadata {
		q := "INSERT OR REPLACE INTO metadata (name, value) VALUES(?, ?)"
		_, err = o.txn.Exec(q, name, value)

		if err != nil {
			return fmt.Errorf("failed to add %s metadata key: %w", name, err)
		}
	}

	// Commit the transaction
	if o.txn != nil {
		err = o.txn.Commit()
	}

	// Close the database
	if o.db != nil {
		if err2 := o.db.Close(); err2 != nil {
			err = err2
		}
	}

	return err
}

func (o *mbtilesOutputter) CreateTiles() error {
	if o.hasTiles {
		return nil
	}
	if _, err := o.db.Exec(`
		BEGIN TRANSACTION;
		CREATE TABLE IF NOT EXISTS map (
			zoom_level INTEGER NOT NULL,
			tile_column INTEGER NOT NULL,
			tile_row INTEGER NOT NULL,
			tile_id TEXT NOT NULL
		);
		CREATE UNIQUE INDEX IF NOT EXISTS map_index ON map (zoom_level, tile_column, tile_row);
		CREATE TABLE IF NOT EXISTS images (
			tile_data BLOB NOT NULL,
			tile_id TEXT NOT NULL
		);
		CREATE UNIQUE INDEX IF NOT EXISTS images_id ON images (tile_id);
		CREATE TABLE IF NOT EXISTS metadata (
			name TEXT,
			value TEXT
		);
		CREATE UNIQUE INDEX IF NOT EXISTS name ON metadata (name);
		CREATE VIEW IF NOT EXISTS tiles AS
		SELECT
			map.zoom_level AS zoom_level,
			map.tile_column AS tile_column,
			map.tile_row AS tile_row,
			images.tile_data AS tile_data
		FROM map
		JOIN images ON images.tile_id = map.tile_id;
		COMMIT;
	    PRAGMA synchronous=OFF;
	`); err != nil {
		return err
	}
	o.hasTiles = true
	return nil
}

func (o *mbtilesOutputter) AssignSpatialMetadata(bounds orb.Bound, minZoom maptile.Zoom, maxZoom maptile.Zoom) error {

	// https://github.com/mapbox/mbtiles-spec/blob/master/1.3/spec.md

	center := bounds.Center()

	strBounds := fmt.Sprintf("%f,%f,%f,%f", bounds.Min[0], bounds.Min[1], bounds.Max[0], bounds.Max[1])
	strCenter := fmt.Sprintf("%f,%f", center[0], center[1])

	strMinzoom := strconv.Itoa(int(minZoom))
	strMaxzoom := strconv.Itoa(int(maxZoom))

	metadata := map[string]string{
		"bounds":  strBounds,
		"center":  strCenter,
		"minzoom": strMinzoom,
		"maxzoom": strMaxzoom,
	}

	for name, value := range metadata {
		o.metadata.Set(name, value)
	}

	return nil
}

func (o *mbtilesOutputter) Save(tile maptile.Tile, data []byte) error {
	if err := o.CreateTiles(); err != nil {
		return err
	}

	if o.txn == nil {
		tx, err := o.db.Begin()
		if err != nil {
			return err
		}
		o.txn = tx
	}

	hash := md5.Sum(data)
	tileID := hex.EncodeToString(hash[:])

	invertedY := uint32(math.Pow(2.0, float64(tile.Z))) - 1 - tile.Y

	_, err := o.txn.Exec("INSERT OR REPLACE INTO images (tile_id, tile_data) VALUES (?, ?);", tileID, data)
	if err != nil {
		return err
	}

	_, err = o.txn.Exec("INSERT OR REPLACE INTO map (zoom_level, tile_column, tile_row, tile_id) VALUES (?, ?, ?, ?);", tile.Z, tile.X, invertedY, tileID)
	if err != nil {
		return err
	}

	o.batchCount++

	if o.batchCount%o.batchSize == 0 {
		err := o.txn.Commit()
		if err != nil {
			return err
		}
		o.batchCount = 0
		o.txn = nil
	}

	return err
}
