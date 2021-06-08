package http

import (
	"fmt"
	"github.com/tilezen/go-tilepacks/tilepack"
	"log"
	gohttp "net/http"
	"regexp"
	"strconv"
	"strings"
)

var (
	tilezenRegex = regexp.MustCompile(`\/tilezen\/vector\/v1\/512\/all\/(\d+)\/(\d+)\/(\d+)\.mvt$`)
)

func MbtilesHandler(reader tilepack.MbtilesReader) gohttp.HandlerFunc {

	return func(w gohttp.ResponseWriter, r *gohttp.Request) {
		requestedTile, err := parseTileFromPath(r.URL.Path)
		if err != nil {
			gohttp.NotFound(w, r)
			return
		}

		result, err := reader.GetTile(requestedTile)
		if err != nil {
			log.Printf("Error getting tile: %+v", err)
			gohttp.NotFound(w, r)
			return
		}

		if result.Data == nil {
			gohttp.NotFound(w, r)
			return
		}

		acceptEncoding := r.Header.Get("Accept-Encoding")
		if strings.Contains(acceptEncoding, "gzip") {
			w.Header().Set("Content-Encoding", "gzip")
		} else {
			log.Printf("Requester doesn't accept gzip but our mbtiles have gzip in them")
		}

		w.Header().Set("Content-Type", "application/x-protobuf")
		w.Write(*result.Data)
	}
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
