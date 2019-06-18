package main

import (
	"flag"
	"log"
	"os"
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
	urlTemplateStr := flag.String("url", "", "URL template to make tile requests with.")
	outputStr := flag.String("output", "", "Path to output mbtiles file.")
	boundingBoxStr := flag.String("bounds", "-90.0,-180.0,90.0,180.0", "Comma-separated bounding box in south,west,north,east format. Defaults to the whole world.")
	zoomsStr := flag.String("zooms", "0,1,2,3,4,5,6,7,8,9,10", "Comma-separated list of zoom levels.")
	numTileFetchWorkers := flag.Int("workers", 25, "Number of tile fetch workers to use.")
	requestTimeout := flag.Int("timeout", 60, "HTTP client timeout for tile requests.")
	cpuProfile := flag.String("cpuprofile", "", "Enables CPU profiling. Saves the dump to the given path.")
	invertedY := flag.Bool("inverted-y", false, "Invert the Y-value of tiles to match the TMS (as opposed to ZXY) tile format.")
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

	if *outputStr == "" {
		log.Fatalf("Output path is required")
	}

	if *urlTemplateStr == "" {
		log.Fatalf("URL template is required")
	}

	boundingBoxStrSplit := strings.Split(*boundingBoxStr, ",")
	if len(boundingBoxStrSplit) != 4 {
		log.Fatalf("Bounding box string must be a comma-separated list of 4 numbers")
	}

	boundingBoxFloats := make([]float64, 4)
	for i, bboxStr := range boundingBoxStrSplit {
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

	zoomsStrSplit := strings.Split(*zoomsStr, ",")
	zooms := make([]uint, len(zoomsStrSplit))
	for i, zoomStr := range zoomsStrSplit {
		z, err := strconv.ParseUint(zoomStr, 10, 32)
		if err != nil {
			log.Fatalf("Zoom list could not be parsed: %+v", err)
		}

		zooms[i] = uint(z)
	}

	var jobCreator tilepack.JobGenerator
	var err error
	switch *generatorStr {
	case "xyz":
		jobCreator, err = tilepack.NewXYZJobGenerator(*urlTemplateStr, bounds, zooms, time.Duration(*requestTimeout)*time.Second, *invertedY)
	// case "metatile":
	// 	jobCreator, err = tilepack.NewMetatileJobGenerator(*urlTemplateStr, bounds, zooms)
	case "tapalcatl2":
		materializedZoomsStrSplit := strings.Split(*materializedZoomsStr, ",")
		materializedZooms := make([]uint, len(materializedZoomsStrSplit))
		for i, materializedZoomStr := range materializedZoomsStrSplit {
			z, err := strconv.ParseUint(materializedZoomStr, 10, 32)
			if err != nil {
				log.Fatalf("Materialized zoom list could not be parsed: %+v", err)
			}
			materializedZooms[i] = uint(z)
		}

		jobCreator, err = tilepack.NewTapalcatl2JobGenerator(*urlTemplateStr, bounds, zooms, materializedZooms)
	default:
		log.Fatalf("Unknown job generator type %s", *generatorStr)
	}

	mbtilesOutputter, err := tilepack.NewMbtilesOutputter(*outputStr)
	if err != nil {
		log.Fatalf("Couldn't create mbtiles output: %+v", err)
	}

	mbtilesOutputter.CreateTiles()
	log.Println("Created mbtiles output")

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
	go processResults(resultWG, results, mbtilesOutputter)

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
