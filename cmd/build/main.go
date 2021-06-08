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

	"github.com/tilezen/go-tilepacks/tilepack"
)

const (
	saveLogInterval = 10000
)

func processResults(waitGroup *sync.WaitGroup, results chan *tilepack.TileResponse, processor tilepack.TileOutputter) {
	defer waitGroup.Done()

	start := time.Now()

	counter := 0
	for result := range results {
		err := processor.Save(result.Tile, result.Data)
		if err != nil {
			log.Printf("Couldn't save tile %+v", err)
		}

		counter++

		if counter%saveLogInterval == 0 {
			duration := time.Since(start)
			start = time.Now()
			log.Printf("Saved %dk tiles (%0.1f tiles per second)", counter/1000, saveLogInterval/duration.Seconds())
		}
	}
	log.Printf("Saved %d tiles", counter)

	err := processor.Close()
	if err != nil {
		log.Printf("Error closing processor: %+v", err)
	}
}

func main() {
	generatorStr := flag.String("generator", "xyz", "Which tile fetcher to use. Options are xyz, metatile, tapalcatl2.")
	fileTransportRoot := flag.String("file-transport-root", "", "The root directory for tiles if -url-template defines a file:// URL scheme")
	outputMode := flag.String("output-mode", "mbtiles", "Valid modes are: disk, mbtiles.")
	outputDSN := flag.String("dsn", "", "Path, or DSN string, to output files.")
	boundingBoxStr := flag.String("bounds", "-90.0,-180.0,90.0,180.0", "Comma-separated bounding box in south,west,north,east format. Defaults to the whole world.")
	zoomsStr := flag.String("zooms", "0,1,2,3,4,5,6,7,8,9,10", "Comma-separated list of zoom levels or a '{MIN_ZOOM}-{MAX_ZOOM}' range string.")
	numTileFetchWorkers := flag.Int("workers", 25, "Number of tile fetch workers to use.")
	requestTimeout := flag.Int("timeout", 60, "HTTP client timeout for tile requests.")
	cpuProfile := flag.String("cpuprofile", "", "Enables CPU profiling. Saves the dump to the given path.")
	invertedY := flag.Bool("inverted-y", false, "Invert the Y-value of tiles to match the TMS (as opposed to ZXY) tile format.")
	urlTemplateStr := flag.String("url-template", "", "(For xyz generator) URL template to make tile requests with. If URL template begins with file:// you must pass the -file-transport-root flag.")
	layerNameStr := flag.String("layer-name", "", "(For metatile, tapalcatl2 generator) The layer name to use for hash building.")
	pathTemplateStr := flag.String("path-template", "", "(For metatile, tapalcatl2 generator) The template to use for the path part of the S3 path to the t2 archive.")
	bucketStr := flag.String("bucket", "", "(For metatile, tapalcatl2 generator) The name of the S3 bucket to request t2 archives from.")
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

	bounds := &tilepack.LngLatBbox{
		South: boundingBoxFloats[0],
		West:  boundingBoxFloats[1],
		North: boundingBoxFloats[2],
		East:  boundingBoxFloats[3],
	}

	var zooms []uint

	re_zoom, re_err := regexp.Compile(`^\d+\-\d+$`)

	if re_err != nil {
		log.Fatal("Failed to compile zoom range regular expression")
	}

	if re_zoom.MatchString(*zoomsStr) {

		zoom_range := strings.Split(*zoomsStr, "-")

		min_zoom, err := strconv.ParseUint(zoom_range[0], 10, 32)

		if err != nil {
			log.Fatalf("Failed to parse min zoom (%s), %s\n", zoom_range[0], err)
		}

		max_zoom, err := strconv.ParseUint(zoom_range[1], 10, 32)

		if err != nil {
			log.Fatalf("Failed to parse max zoom (%s), %s\n", zoom_range[1], err)
		}

		if min_zoom > max_zoom {
			log.Fatal("Invalid zoom range")
		}

		zooms = make([]uint, 0)

		for z := min_zoom; z <= max_zoom; z++ {
			zooms = append(zooms, uint(z))
		}

	} else {

		zoomsStrSplit := strings.Split(*zoomsStr, ",")
		zooms = make([]uint, len(zoomsStrSplit))
		for i, zoomStr := range zoomsStrSplit {
			z, err := strconv.ParseUint(zoomStr, 10, 32)
			if err != nil {
				log.Fatalf("Zoom list could not be parsed: %+v", err)
			}

			zooms[i] = uint(z)
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

			jobCreator, err = tilepack.NewFileTransportXYZJobGenerator(*fileTransportRoot, *urlTemplateStr, bounds, zooms, time.Duration(*requestTimeout)*time.Second, *invertedY)
		} else {
			jobCreator, err = tilepack.NewXYZJobGenerator(*urlTemplateStr, bounds, zooms, time.Duration(*requestTimeout)*time.Second, *invertedY)
		}

	case "metatile":
		if *bucketStr == "" {
			log.Fatalf("Bucket name is required")
		}

		if *pathTemplateStr == "" {
			log.Fatalf("Path template is required")
		}

		if *layerNameStr == "" {
			log.Fatalf("layerNameStr is required")
		}

		// TODO These should probably be configurable
		metatileSize := uint(8)
		maxDetailZoom := uint(13)

		jobCreator, err = tilepack.NewMetatileJobGenerator(*bucketStr, *pathTemplateStr, *layerNameStr, metatileSize, maxDetailZoom, zooms, bounds)
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
		materializedZooms := make([]uint, len(materializedZoomsStrSplit))
		for i, materializedZoomStr := range materializedZoomsStrSplit {
			z, err := strconv.ParseUint(materializedZoomStr, 10, 32)
			if err != nil {
				log.Fatalf("Materialized zoom list could not be parsed: %+v", err)
			}
			materializedZooms[i] = uint(z)
		}

		jobCreator, err = tilepack.NewTapalcatl2JobGenerator(*bucketStr, *pathTemplateStr, *layerNameStr, materializedZooms, zooms, bounds)
	default:
		log.Fatalf("Unknown job generator type %s", *generatorStr)
	}

	if err != nil {
		log.Fatalf("Failed to create jobCreator: %s", err)
	}

	var outputter tilepack.TileOutputter
	var outputter_err error

	switch *outputMode {
	case "disk":
		outputter, outputter_err = tilepack.NewDiskOutputter(*outputDSN)
	case "mbtiles":
		outputter, outputter_err = tilepack.NewMbtilesOutputter(*outputDSN)
	default:
		log.Fatalf("Unknown outputter: %s", *outputMode)
	}

	if outputter_err != nil {
		log.Fatalf("Couldn't create %s output: %+v", *outputMode, outputter_err)
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

		go func() {
			workerWG.Add(1)
			defer workerWG.Done()
			worker(w, jobs, results)
		}()
	}

	// Start the worker that receives data from HTTP workers
	resultWG := &sync.WaitGroup{}
	resultWG.Add(1)
	go processResults(resultWG, results, outputter)

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
}
