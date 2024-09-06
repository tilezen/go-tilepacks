package tilepack

import (
	"archive/zip"
	"bytes"
	"compress/gzip"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"math"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/paulmach/orb"
	"github.com/paulmach/orb/maptile"
)

const (
	tileScale = 2
)

func log2Uint(size uint) uint {
	return uint(math.Log2(float64(size)))
}

func NewMetatileJobGenerator(bucket string, requesterPays bool, pathTemplate string, layerName string, format string, metatileSize uint, maxDetailZoom maptile.Zoom, zooms []maptile.Zoom, bounds orb.Bound) (JobGenerator, error) {
	sess, err := session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	})
	if err != nil {
		return nil, err
	}

	downloader := s3manager.NewDownloader(
		sess,
		func(downloader *s3manager.Downloader) {
			// See https://levyeran.medium.com/high-memory-allocations-and-gc-cycles-while-downloading-large-s3-objects-using-the-aws-sdk-for-go-e776a136c5d0
			downloader.BufferProvider = s3manager.NewPooledBufferedWriterReadFromProvider(15 * 1024 * 1024)
		},
	)

	return &metatileJobGenerator{
		s3Client:      downloader,
		bucket:        bucket,
		requesterPays: requesterPays,
		pathTemplate:  pathTemplate,
		layerName:     layerName,
		metatileSize:  metatileSize,
		maxDetailZoom: maxDetailZoom,
		bounds:        bounds,
		zooms:         zooms,
		format:        format,
	}, nil
}

type metatileJobGenerator struct {
	s3Client      *s3manager.Downloader
	bucket        string
	requesterPays bool
	pathTemplate  string
	layerName     string
	metatileSize  uint
	maxDetailZoom maptile.Zoom
	bounds        orb.Bound
	zooms         []maptile.Zoom
	format        string
}

func (x *metatileJobGenerator) CreateWorker() (func(id int, jobs chan *TileRequest, results chan *TileResponse), error) {
	f := func(id int, jobs chan *TileRequest, results chan *TileResponse) {
		// Instantiate the gzip support stuff once instead on every iteration
		bodyBuffer := bytes.NewBuffer(nil)
		bodyGzipper := gzip.NewWriter(bodyBuffer)

		for metaTileRequest := range jobs {
			// Download the metatile archive zip to a byte buffer
			compressedBytes := &aws.WriteAtBuffer{}
			input := &s3.GetObjectInput{
				Bucket: aws.String(x.bucket),
				Key:    aws.String(metaTileRequest.URL),
			}

			if x.requesterPays {
				input.RequestPayer = aws.String("requester")
			}

			numBytes, err := x.s3Client.Download(compressedBytes, input)
			if err != nil {
				log.Printf("Unable to download item s3://%s/%s: %+v", x.bucket, metaTileRequest.URL, err)
				continue
			}

			// Uncompress the archive
			readBytes := compressedBytes.Bytes()
			readBytesReader := bytes.NewReader(readBytes)
			zippedReader, err := zip.NewReader(readBytesReader, numBytes)
			if err != nil {
				log.Printf("Unable to unzip metatile archive %s: %+v", metaTileRequest.URL, err)
				continue
			}

			// Iterate over the contents of the zip and add them as TileResponses
			for _, zf := range zippedReader.File {
				var offsetZ, offsetX, offsetY uint32
				var format string
				if n, err := fmt.Sscanf(zf.Name, "%d/%d/%d.%s", &offsetZ, &offsetX, &offsetY, &format); err != nil || n != 4 {
					log.Fatalf("Couldn't scan metatile name")
				}

				// Skip formats we don't care about
				if format != x.format {
					continue
				}

				// Add the offset to metatile to get the actual tile
				t := maptile.New(
					(metaTileRequest.Tile.X<<offsetZ)+offsetX,
					(metaTileRequest.Tile.Y<<offsetZ)+offsetY,
					metaTileRequest.Tile.Z+maptile.Zoom(offsetZ),
				)

				if !arrayContains(t.Z, x.zooms) {
					continue
				}

				if !x.bounds.Intersects(t.Bound()) {
					continue
				}

				// Read the data for the tile
				zfReader, err := zf.Open()
				if err != nil {
					log.Fatalf("Couldn't read zf %s: %+v", zf.Name, err)
				}

				b, err := io.ReadAll(zfReader)
				if err != nil {
					log.Fatalf("Couldn't read zf %s: %+v", zf.Name, err)
				}

				// Gzip the data
				bodyBuffer.Reset()
				bodyGzipper.Reset(bodyBuffer)

				_, err = bodyGzipper.Write(b)
				if err != nil {
					log.Printf("Couldn't write to gzipper: %+v", err)
					continue
				}

				err = bodyGzipper.Close()
				if err != nil {
					log.Printf("Couldn't flush gzipper: %+v", err)
					continue
				}

				bodyData, err := io.ReadAll(bodyBuffer)
				if err != nil {
					log.Printf("Couldn't read bytes into byte array: %+v", err)
					continue
				}

				results <- &TileResponse{
					Data: bodyData,
					Tile: t,
				}
			}
		}
	}

	return f, nil
}

func (x *metatileJobGenerator) CreateJobs(jobs chan *TileRequest) error {
	// Convert the list of requested zooms into a list of zooms where metatiles are
	metatileZooms := []maptile.Zoom{}

	metaZoom := maptile.Zoom(log2Uint(x.metatileSize))
	tileZoom := maptile.Zoom(log2Uint(tileScale))
	deltaZoom := metaZoom - tileZoom

	for _, z := range x.zooms {
		var metatileZoom maptile.Zoom
		if z < deltaZoom {
			metatileZoom = 0
		} else {
			metatileZoom = z - deltaZoom
		}

		// Beyond the "max detail zoom", all tiles are in the metatile
		if x.maxDetailZoom > 0 && metatileZoom > x.maxDetailZoom {
			metatileZoom = x.maxDetailZoom
		}

		if !arrayContains(metatileZoom, metatileZooms) {
			metatileZooms = append(metatileZooms, metatileZoom)
		}
	}

	// Generate requests for metatiles in the bounding box
	GenerateTiles(&GenerateTilesOptions{
		Bounds:    x.bounds,
		InvertedY: false,
		Zooms:     metatileZooms,
		ConsumerFunc: func(t maptile.Tile) {
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

	return nil
}
