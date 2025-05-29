package main

import (
	"flag"
	"log"
	"os"
	"regexp"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/paulmach/orb"
	"github.com/paulmach/orb/maptile"
	"github.com/schollz/progressbar/v3"

	"github.com/tilezen/go-tilepacks/tilepack"
)

func processResults(results chan *tilepack.TileResponse, processor tilepack.TileOutputter, progress *progressbar.ProgressBar) {
	tileCount := 0
	for result := range results {
		err := processor.Save(result.Tile, result.Data)
		if err != nil {
			log.Printf("Couldn't save tile %+v", err)
		}

		tileCount += 1
		progress.Add(1)
	}

	progress.Finish()
	log.Printf("Processed %d tiles", tileCount)
}

func main() {
	generatorStr := flag.String("generator", "xyz", "Which tile fetcher to use. Options are xyz, metatile, tapalcatl2.")
	fileTransportRoot := flag.String("file-transport-root", "", "The root directory for tiles if -url-template defines a file:// URL scheme")
	outputMode := flag.String("output-mode", "mbtiles", "Valid modes are: disk, mbtiles, pmtiles.")
	outputDSN := flag.String("dsn", "", "Path, or DSN string, to output files.")
	boundingBoxStr := flag.String("bounds", "-90.0,-180.0,90.0,180.0", "Comma-separated bounding box in south,west,north,east format. Defaults to the whole world.")
	zoomsStr := flag.String("zooms", "0,1,2,3,4,5,6,7,8,9,10", "Comma-separated list of zoom levels or a '{MIN_ZOOM}-{MAX_ZOOM}' range string.")
	numTileFetchWorkers := flag.Int("workers", 25, "Number of tile fetch workers to use.")
	mbtilesBatchSize := flag.Int("batch-size", 50, "(For mbtiles outputter) Number of tiles to batch together before writing to mbtiles")
	mbtilesTilesetName := flag.String("tileset-name", "tileset", "(For mbtiles outputter) Name of the tileset to write to the mbtiles file metadata.")
	mbtilesFormat := flag.String("mbtiles-format", "pbf", "(Deprecated. Use --output-format instead)")
	outputFormat := flag.String("output-format", "pbf", "(For mbtiles and pmtiles outputter) Format of the tiles in the mbtiles or pmtiles file metadata.")
	requestTimeout := flag.Int("timeout", 60, "HTTP client timeout for tile requests.")
	cpuProfile := flag.String("cpuprofile", "", "Enables CPU profiling. Saves the dump to the given path.")
	invertedY := flag.Bool("inverted-y", false, "Invert the Y-value of tiles to match the TMS (as opposed to ZXY) tile format.")
	ensureGzip := flag.Bool("ensure-gzip", true, "Ensure tile data is gzipped. Only applies to XYZ tiles.")
	urlTemplateStr := flag.String("url-template", "", "(For xyz generator) URL template to make tile requests with. If URL template begins with file:// you must pass the -file-transport-root flag.")
	layerNameStr := flag.String("layer-name", "", "(For metatile, tapalcatl2 generator) The layer name to use for hash building.")
	formatStr := flag.String("format", "mvt", "(For metatile generator) The format of the tile inside the metatile to extract.")
	pathTemplateStr := flag.String("path-template", "", "(For metatile, tapalcatl2 generator) The template to use for the path part of the S3 path to the t2 archive.")
	bucketStr := flag.String("bucket", "", "(For metatile, tapalcatl2 generator) The name of the S3 bucket to request t2 archives from.")
	requesterPays := flag.Bool("requester-pays", false, "(For metatile, tapalcatl2 generator) Whether to make S3 requests with requester pays enabled.")
	materializedZoomsStr := flag.String("materialized-zooms", "", "(For tapalcatl2 generator) Specifies the materialized zooms for t2 archives.")
	flag.Parse()

	if *cpuProfile != "" {
		f, err := os.Create(*cpuProfile)
		if err != nil {
			log.Fatal("could not create CPU profile: ", err)
		}
		defer f.Close()
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatal("could not start CPU profile: ", err)
		}
		defer pprof.StopCPUProfile()
	}

	if *outputDSN == "" {
		log.Fatalf("Output DSN (-dsn) is required")
	}

	boundingBoxStrSplit := strings.Split(*boundingBoxStr, ",")
	if len(boundingBoxStrSplit) != 4 {
		log.Fatalf("Bounding box string must be a comma-separated list of 4 numbers")
	}

	boundingBoxFloats := make([]float64, 4)
	for i, bboxStr := range boundingBoxStrSplit {
		bboxStr = strings.TrimSpace(bboxStr)
		bboxFloat, err := strconv.ParseFloat(bboxStr, 64)
		if err != nil {
			log.Fatalf("Bounding box string could not be parsed as numbers")
		}

		boundingBoxFloats[i] = bboxFloat
	}

	bounds := orb.MultiPoint{
		orb.Point{boundingBoxFloats[1], boundingBoxFloats[0]},
		orb.Point{boundingBoxFloats[3], boundingBoxFloats[2]},
	}.Bound()

	var zooms []maptile.Zoom

	reZoom := regexp.MustCompile(`^\d+-\d+$`)

	if reZoom.MatchString(*zoomsStr) {

		zoomRange := strings.Split(*zoomsStr, "-")

		minZoom, err := strconv.ParseUint(zoomRange[0], 10, 32)

		if err != nil {
			log.Fatalf("Failed to parse min zoom (%s), %s\n", zoomRange[0], err)
		}

		maxZoom, err := strconv.ParseUint(zoomRange[1], 10, 32)

		if err != nil {
			log.Fatalf("Failed to parse max zoom (%s), %s\n", zoomRange[1], err)
		}

		if minZoom > maxZoom {
			log.Fatal("Invalid zoom range")
		}

		zooms = make([]maptile.Zoom, 0)

		for z := maptile.Zoom(minZoom); z <= maptile.Zoom(maxZoom); z++ {
			zooms = append(zooms, z)
		}

	} else {

		zoomsStrSplit := strings.Split(*zoomsStr, ",")
		zooms = make([]maptile.Zoom, len(zoomsStrSplit))
		for i, zoomStr := range zoomsStrSplit {
			z, err := strconv.ParseUint(zoomStr, 10, 32)
			if err != nil {
				log.Fatalf("Zoom list could not be parsed: %+v", err)
			}

			zooms[i] = maptile.Zoom(z)
		}
	}

	var jobCreator tilepack.JobGenerator
	var err error
	switch *generatorStr {
	case "xyz":
		if *urlTemplateStr == "" {
			log.Fatalf("URL template is required")
		}

		if strings.HasPrefix(*urlTemplateStr, "file://") {

			if *fileTransportRoot == "" {
				log.Fatalf("-file-transport-root flag is required when URL template uses file://")
			}

			jobCreator, err = tilepack.NewFileTransportXYZJobGenerator(*fileTransportRoot, *urlTemplateStr, bounds, zooms, time.Duration(*requestTimeout)*time.Second, *invertedY, *ensureGzip)
		} else {
			jobCreator, err = tilepack.NewXYZJobGenerator(*urlTemplateStr, bounds, zooms, time.Duration(*requestTimeout)*time.Second, *invertedY, *ensureGzip, *mbtilesFormat)
		}

	case "metatile":
		if *bucketStr == "" {
			log.Fatalf("Bucket name is required")
		}

		if *pathTemplateStr == "" {
			log.Fatalf("Path template is required")
		}

		if *layerNameStr == "" {
			log.Fatalf("Layer name is required")
		}

		if *formatStr == "" {
			log.Fatalf("Format is required")
		}

		// TODO These should probably be configurable
		metatileSize := uint(8)
		maxDetailZoom := maptile.Zoom(13)

		jobCreator, err = tilepack.NewMetatileJobGenerator(*bucketStr, *requesterPays, *pathTemplateStr, *layerNameStr, *formatStr, metatileSize, maxDetailZoom, zooms, bounds)
	case "tapalcatl2":
		if *bucketStr == "" {
			log.Fatalf("Bucket name is required")
		}

		if *pathTemplateStr == "" {
			log.Fatalf("Path template is required")
		}

		if *materializedZoomsStr == "" {
			log.Fatalf("Materialized zoom list is required")
		}

		if *layerNameStr == "" {
			log.Fatalf("layerNameStr is required")
		}

		materializedZoomsStrSplit := strings.Split(*materializedZoomsStr, ",")
		materializedZooms := make([]maptile.Zoom, len(materializedZoomsStrSplit))
		for i, materializedZoomStr := range materializedZoomsStrSplit {
			z, err := strconv.ParseUint(materializedZoomStr, 10, 32)
			if err != nil {
				log.Fatalf("Materialized zoom list could not be parsed: %+v", err)
			}
			materializedZooms[i] = maptile.Zoom(z)
		}

		jobCreator, err = tilepack.NewTapalcatl2JobGenerator(*bucketStr, *requesterPays, *pathTemplateStr, *layerNameStr, materializedZooms, zooms, bounds)
	default:
		log.Fatalf("Unknown job generator type %s", *generatorStr)
	}

	if err != nil {
		log.Fatalf("Failed to create jobCreator: %s", err)
	}

	expectedTileCount := calculateExpectedTiles(bounds, zooms)
	progress := progressbar.NewOptions(
		int(expectedTileCount),
		progressbar.OptionSetItsString("tile"),
		progressbar.OptionShowIts(),
		progressbar.OptionFullWidth(),
		progressbar.OptionThrottle(100*time.Millisecond),
	)
	log.Printf("Expecting to fetch %d tiles", expectedTileCount)

	var outputter tilepack.TileOutputter
	var outputterErr error

	switch *outputMode {
	case "disk":
		outputter, outputterErr = tilepack.NewDiskOutputter(*outputDSN)
	case "mbtiles":
		metadata := tilepack.NewMbtilesMetadata(map[string]string{})

		// mbtilesFormat is deprecated, use outputFormat instead
		if *mbtilesFormat != "" {
			log.Printf("Warning: --mbtiles-format is deprecated, use --output-format instead")
			*outputFormat = *mbtilesFormat
		}

		if *outputFormat == "" {
			log.Fatalf("--output-format is required for mbtiles output")
		}
		metadata.Set("format", *outputFormat)

		if *outputFormat != "pbf" && *ensureGzip {
			log.Printf("Warning: gzipping is only required for PBF tiles. You may want to disable it for other formats with --ensure-gzip=false")
		}

		if *mbtilesTilesetName == "" {
			log.Fatalf("--tileset-name is required for mbtiles output")
		}
		metadata.Set("name", *mbtilesTilesetName)

		outputter, outputterErr = tilepack.NewMbtilesOutputter(*outputDSN, *mbtilesBatchSize, metadata)
	case "pmtiles":
		metadata := tilepack.NewMbtilesMetadata(map[string]string{})

		if *outputFormat == "" {
			log.Fatalf("--output-format is required for pmtiles output")
		}
		metadata.Set("format", *outputFormat)

		if *outputFormat != "pbf" && *ensureGzip {
			log.Printf("Warning: gzipping is only required for PBF tiles. You may want to disable it for other formats with --ensure-gzip=false")
		}

		if *mbtilesTilesetName == "" {
			log.Fatalf("--tileset-name is required for pmtiles output")
		}
		metadata.Set("name", *mbtilesTilesetName)

		outputter, outputterErr = tilepack.NewPmtilesOutputter(*outputDSN, *outputFormat, metadata)
	default:
		log.Fatalf("Unknown outputter: %s", *outputMode)
	}

	if outputterErr != nil {
		log.Fatalf("Couldn't create %s output: %+v", *outputMode, outputterErr)
	}

	err = outputter.CreateTiles()

	if err != nil {
		log.Fatalf("Failed to create %s output: %+v", *outputMode, err)
	}

	log.Printf("Created %s output\n", *outputMode)

	jobs := make(chan *tilepack.TileRequest, 2000)
	results := make(chan *tilepack.TileResponse, 2000)

	// Start up the HTTP workers that will fetch tiles
	workerWG := &sync.WaitGroup{}
	for w := 0; w < *numTileFetchWorkers; w++ {
		worker, err := jobCreator.CreateWorker()
		if err != nil {
			log.Fatalf("Couldn't create %s worker: %+v", *generatorStr, err)
		}

		workerN := w
		workerWG.Add(1)
		go func() {
			defer workerWG.Done()
			worker(workerN, jobs, results)
		}()
	}

	// Start the worker that receives data from HTTP workers
	resultWG := &sync.WaitGroup{}
	resultWG.Add(1)
	go func() {
		defer resultWG.Done()
		processResults(results, outputter, progress)
	}()

	jobCreator.CreateJobs(jobs)

	// Add tile request jobs
	close(jobs)
	log.Print("Job queue closed")

	// When the workers are done, close the results channel
	workerWG.Wait()
	close(results)
	log.Print("Finished making tile requests")

	// Wait for the results to be written out
	resultWG.Wait()
	log.Print("Finished processing tiles")

	err = outputter.AssignSpatialMetadata(bounds, zooms[0], zooms[len(zooms)-1])
	if err != nil {
		log.Printf("Wrote tiles but failed to assign spatial metadata, %v", err)
	}

	err = outputter.Close()
	if err != nil {
		log.Printf("Error closing processor: %+v", err)
	}
}

func calculateExpectedTiles(bounds orb.Bound, zooms []maptile.Zoom) uint32 {
	totalTiles := uint32(0)

	opts := &tilepack.GenerateRangesOptions{
		Bounds: bounds,
		Zooms:  zooms,
		ConsumerFunc: func(minTile maptile.Tile, maxTile maptile.Tile, z maptile.Zoom) {
			tilesAtZoom := (maxTile.X + 1 - minTile.X) * (maxTile.Y + 1 - minTile.Y)
			totalTiles += tilesAtZoom
		},
	}
	tilepack.GenerateTileRanges(opts)

	return totalTiles
}
