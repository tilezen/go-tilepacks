package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"regexp"
	"strconv"

	"github.com/tilezen/go-tilepacks/tilepack"
)

type MbtilesHandler struct {
	mbtiles *tilepack.MbtilesReader
}

var (
	tilezenRegex = regexp.MustCompile(`^\/tilezen\/vector\/v1\/512\/all\/(\d+)\/(\d+)\/(\d+)\.mvt$`)
)

func NewMbtilesHandler(filename string) (*MbtilesHandler, error) {
	reader, err := tilepack.NewMbtilesReader(filename)
	if err != nil {
		return nil, err
	}

	return &MbtilesHandler{
		mbtiles: reader,
	}, nil
}

func parseTileFromPath(url string) (*tilepack.Tile, error) {
	match := tilezenRegex.FindStringSubmatch(url)
	if match == nil {
		return nil, fmt.Errorf("invalid tile path")
	}

	z, _ := strconv.ParseUint(match[1], 10, 32)
	x, _ := strconv.ParseUint(match[2], 10, 32)
	y, _ := strconv.ParseUint(match[3], 10, 32)

	return &tilepack.Tile{Z: uint(z), X: uint(x), Y: uint(y)}, nil
}

func (o *MbtilesHandler) TilesHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		requestedTile, err := parseTileFromPath(r.URL.Path)
		if err != nil {
			http.NotFound(w, r)
			return
		}

		result, err := o.mbtiles.GetTile(requestedTile)
		if err != nil {
			log.Printf("Error getting tile: %+v", err)
			http.NotFound(w, r)
			return
		}

		w.WriteHeader(http.StatusOK)
		w.Write(*result.Data)
	}
}

func main() {
	mbtilesFile := flag.String("input", "", "The name of the mbtiles file to serve from.")
	flag.Parse()

	if *mbtilesFile == "" {
		log.Fatal("Need to provide --input parameter")
	}

	mbtilesHandler, err := NewMbtilesHandler(*mbtilesFile)
	if err != nil {
		log.Fatalf("Couldn't create mbtiles handler: %+v", err)
	}

	http.HandleFunc("/preview.html", previewHTMLHandler)
	http.Handle("/tilezen/", mbtilesHandler.TilesHandler())
	http.HandleFunc("/", defaultHandler)

	err = http.ListenAndServe(":8080", nil)
	if err != nil {
		log.Fatalf("Problem serving: %+v", err)
	}
}

func previewHTMLHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "preview html")
}

func defaultHandler(w http.ResponseWriter, r *http.Request) {
	http.NotFound(w, r)
}
