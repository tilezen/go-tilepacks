package main

import (
	"flag"
	"log"
	"os"
	"strings"

	"github.com/tilezen/go-tilepacks/tilepack"
)

func pathExists(path string) bool {
	_, err := os.Stat(path)
	if os.IsNotExist(err) {
		return false
	}
	return true
}

func main() {
	outputFilename := flag.String("output", "", "The output mbtiles to write to")
	flag.Parse()
	inputFilenames := flag.Args()

	if *outputFilename == "" {
		log.Fatalf("Must specify --output path")
	}

	if len(inputFilenames) == 0 {
		log.Fatalf("Must specify at least one input path")
	}

	log.Printf("Reading %s and writing them to %s", strings.Join(inputFilenames, ", "), *outputFilename)

	// If the output file exists already we shouldn't overwrite it
	if pathExists(*outputFilename) {
		log.Fatalf("Output path %s already exists and cannot be overwritten", *outputFilename)
	}

	// Create the output mbtiles
	outputMbtiles, err := tilepack.NewMbtilesOutputter(*outputFilename)
	if err != nil {
		log.Fatalf("Couldn't create output mbtiles: %+v", err)
	}

	err = outputMbtiles.CreateTiles()
	if err != nil {
		log.Fatalf("Couldn't create output mbtiles: %+v", err)
	}

	for _, inputFilename := range inputFilenames {
		mbtilesReader, err := tilepack.NewMbtilesReader(inputFilename)
		if err != nil {
			log.Fatalf("Couldn't read input mbtiles %s: %+v", inputFilename, err)
		}

		err = mbtilesReader.VisitAllTiles(func(t *tilepack.Tile, data []byte) {
			outputMbtiles.Save(t, data)
		})
		if err != nil {
			log.Fatalf("Couldn't read tiles from %s: %+v", inputFilename, err)
		}
		mbtilesReader.Close()
	}

	outputMbtiles.Close()
}
