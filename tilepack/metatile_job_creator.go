package tilepack

import (
	"archive/zip"
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

const (
	tileScale = 2
)

func log2Uint(size uint) uint {
	return uint(math.Log2(float64(size)))
}

func NewMetatileJobGenerator(bucket string, pathTemplate string, layerName string, metatileSize uint, zooms []uint, bounds *LngLatBbox) (JobGenerator, error) {
	sess, err := session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	})
	if err != nil {
		return nil, err
	}

	downloader := s3manager.NewDownloader(sess)

	return &metatileJobGenerator{
		s3Client:     downloader,
		bucket:       bucket,
		pathTemplate: pathTemplate,
		layerName:    layerName,
		metatileSize: metatileSize,
		bounds:       bounds,
		zooms:        zooms,
	}, nil
}

type metatileJobGenerator struct {
	s3Client     *s3manager.Downloader
	bucket       string
	pathTemplate string
	layerName    string
	metatileSize uint
	bounds       *LngLatBbox
	zooms        []uint
}

func (x *metatileJobGenerator) CreateWorker() (func(id int, jobs chan *TileRequest, results chan *TileResponse), error) {
	f := func(id int, jobs chan *TileRequest, results chan *TileResponse) {
		for request := range jobs {
			// Download the metatile archive zip to a byte buffer
			compressedBytes := &aws.WriteAtBuffer{}
			numBytes, err := x.s3Client.Download(compressedBytes, &s3.GetObjectInput{
				Bucket: aws.String(x.bucket),
				Key:    aws.String(request.URL),
			})
			if err != nil {
				log.Fatalf("Unable to download item %s: %+v", request.URL, err)
			}

			// Uncompress the archive
			readBytes := compressedBytes.Bytes()
			readBytesReader := bytes.NewReader(readBytes)
			zippedReader, err := zip.NewReader(readBytesReader, numBytes)
			if err != nil {
				log.Fatalf("Unable to unzip metatile archive %s: %+v", request.URL, err)
			}

			// Iterate over the contents of the zip and add them as TileResponses
			for _, zf := range zippedReader.File {
				var offsetZ, offsetX, offsetY uint
				// TODO Pull in the format too?
				if n, err := fmt.Sscanf(zf.Name, "%d/%d/%d.mvt", &offsetZ, &offsetX, &offsetY); err != nil || n != 3 {
					log.Fatalf("Couldn't scan metatile name")
				}

				// Add the offset to metatile to get the actual tile
				t := &Tile{
					Z: request.Tile.Z + offsetZ,
					X: request.Tile.X + offsetX,
					Y: request.Tile.Y + offsetY,
				}

				if !zoomIsWanted(t.Z, x.zooms) {
					continue
				}

				if !x.bounds.Intersects(t.Bounds()) {
					continue
				}

				// Read the data for the tile
				zfReader, err := zf.Open()
				if err != nil {
					log.Fatalf("Couldn't read zf %s: %+v", zf.Name, err)
				}

				b, err := ioutil.ReadAll(zfReader)
				if err != nil {
					log.Fatalf("Couldn't read zf %s: %+v", zf.Name, err)
				}

				results <- &TileResponse{
					Data: b,
					Tile: t,
				}
			}
		}
	}

	return f, nil
}

func (x *metatileJobGenerator) CreateJobs(jobs chan *TileRequest) error {
	metaZoom := log2Uint(x.metatileSize)
	tileZoom := log2Uint(tileScale)
	deltaZoom := int(metaZoom) - int(tileZoom)

	// Iterate over the list of requested zooms
	for _, z := range x.zooms {
		// Calculate the metatile zoom for this requested zoom
		var metatileZoom uint
		if int(z) < deltaZoom {
			metatileZoom = 0
		} else {
			metatileZoom = z - uint(deltaZoom)
		}

		// Generate requests for tiles in the bounding box at this metatile zoom
		GenerateTiles(&GenerateTilesOptions{
			Bounds:    x.bounds,
			InvertedY: false,
			Zooms:     []uint{metatileZoom},
			ConsumerFunc: func(t *Tile) {
				hash := md5.Sum([]byte(fmt.Sprintf("%d/%d/%d.zip", t.Z, t.X, t.Y)))
				hashHex := hex.EncodeToString(hash[:])

				path := strings.NewReplacer(
					"{x}", fmt.Sprintf("%d", t.X),
					"{y}", fmt.Sprintf("%d", t.Y),
					"{z}", fmt.Sprintf("%d", t.Z),
					"{l}", x.layerName,
					"{h}", hashHex[:5]).Replace(x.pathTemplate)

				jobs <- &TileRequest{
					Tile: t,
					URL:  path,
				}
			},
		})
	}

	return nil
}
