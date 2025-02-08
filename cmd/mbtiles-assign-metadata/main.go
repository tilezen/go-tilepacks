package main

import (
	"flag"
	"log"

	"github.com/paulmach/orb"
	"github.com/paulmach/orb/maptile"
	"github.com/tilezen/go-tilepacks/tilepack"
)

func main() {

	var verify bool

	flag.BoolVar(&verify, "verify", false, "Verify that spatial metadata was written to each database")

	flag.Parse()

	for _, path := range flag.Args() {

		mbtilesReader, err := tilepack.NewMbtilesReader(path)

		if err != nil {
			log.Fatalf("Couldn't read input mbtiles %s: %+v", path, err)
		}

		var bounds *orb.Bound
		minZoom := uint(20)
		maxZoom := uint(0)

		err = mbtilesReader.VisitAllTiles(func(t maptile.Tile, data []byte) {

			tb := t.Bound()

			if bounds == nil {
				bounds = &tb
			} else {
				tb = bounds.Union(tb)
				bounds = &tb
			}

			minZoom = min(minZoom, uint(t.Z))
			maxZoom = max(maxZoom, uint(t.Z))
		})

		if err != nil {
			log.Fatalf("Couldn't read tiles from %s: %+v", path, err)
		}

		mbtilesReader.Close()

		metadata := tilepack.NewMbtilesMetadata(map[string]string{})

		mbtilesWriter, err := tilepack.NewMbtilesOutputter(path, 0, metadata)

		if err != nil {
			log.Fatalf("Couldn't read input mbtiles %s: %+v", path, err)
		}

		err = mbtilesWriter.AssignSpatialMetadata(*bounds, maptile.Zoom(minZoom), maptile.Zoom(maxZoom))

		if err != nil {
			log.Fatalf("Failed to assign spatial metadata to %s: %+v", path, err)
		}

		mbtilesWriter.Close()

		if verify {

			mbtilesReader, err := tilepack.NewMbtilesReader(path)

			if err != nil {
				log.Fatalf("Couldn't read input mbtiles %s: %+v", path, err)
			}

			metadata, err := mbtilesReader.Metadata()

			if err != nil {
				log.Fatalf("Unable to read metadata for %s, %v", path, err)
			}

			bounds, err := metadata.Bounds()

			if err != nil {
				log.Fatalf("Failed to derive bounds metadata after update")
			}

			center, zoom, err := metadata.Center()

			if err != nil {
				log.Fatalf("Failed to derive bounds metadata after update")
			}

			minZoom, err := metadata.MinZoom()

			if err != nil {
				log.Fatalf("Failed to derive min zoom metadata after update")
			}

			maxZoom, err := metadata.MaxZoom()

			if err != nil {
				log.Fatalf("Failed to derive max zoom metadata after update")
			}

			log.Printf("[%s] bounds: %v center: %v@%d zoom: %d-%d\n", path, bounds, center, zoom, minZoom, maxZoom)
		}
	}
}
