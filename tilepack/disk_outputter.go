package tilepack

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

type diskOutputter struct {
	TileOutputter
	root     string
	format   string
	hasTiles bool
}

func NewDiskOutputter(dsn string) (*diskOutputter, error) {

	format := "svg" // FIX ME

	root, err := filepath.Abs(dsn)

	if err != nil {
		return nil, err
	}

	o := diskOutputter{
		root:   root,
		format: format,
	}

	return &o, nil
}

func (o *diskOutputter) Close() error {
	return nil
}

func (o *diskOutputter) CreateTiles() error {
	if o.hasTiles {
		return nil
	}

	info, err := os.Stat(o.root)

	if err != nil {

		if os.IsNotExist(err) {

			err := os.MkdirAll(o.root, 0755)

			if err != nil {
				return err
			}
		} else {
			return err
		}

	} else {

		if !info.IsDir() {
			return errors.New("Root is already a file")
		}
	}

	o.hasTiles = true
	return nil
}

func (o *diskOutputter) Save(tile *Tile, data []byte) error {

	rel_path := fmt.Sprintf("%d/%d/%d.%s", tile.Z, tile.X, tile.Y, o.format)
	abs_path := filepath.Join(o.root, rel_path)

	root := filepath.Dir(abs_path)

	_, err := os.Stat(root)

	if os.IsNotExist(err) {
		err = os.MkdirAll(root, 0755)
	}

	if err != nil {
		return err
	}

	fh, err := os.OpenFile(abs_path, os.O_RDWR|os.O_CREATE, 0644)

	if err != nil {
		return err
	}

	defer fh.Close()

	_, err = fh.Write(data)

	if err != nil {
		return err
	}

	return fh.Close()
}
