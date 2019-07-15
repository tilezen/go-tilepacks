package tilepack

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/aaronland/go-string/dsn"
)

type diskOutputter struct {
	TileOutputter
	root     string
	format   string
	hasTiles bool
}

func NewDiskOutputter(dsnStr string) (*diskOutputter, error) {

	dsnMap, err := dsn.StringToDSNWithKeys(dsnStr, "root", "format")

	if err != nil {
		return nil, err
	}

	abs_root, err := filepath.Abs(dsnMap["root"])

	if err != nil {
		return nil, err
	}

	o := diskOutputter{
		root:   abs_root,
		format: dsnMap["format"],
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

	relPath := fmt.Sprintf("%d/%d/%d.%s", tile.Z, tile.X, tile.Y, o.format)
	absPath := filepath.Join(o.root, relPath)

	root := filepath.Dir(absPath)

	_, err := os.Stat(root)

	if os.IsNotExist(err) {
		err = os.MkdirAll(root, 0755)
	}

	if err != nil {
		return err
	}

	fh, err := os.OpenFile(absPath, os.O_RDWR|os.O_CREATE, 0644)

	if err != nil {
		return err
	}

	_, err = fh.Write(data)

	if err != nil {
		fh.Close()
		return err
	}

	return fh.Close()
}
