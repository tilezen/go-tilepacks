package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/iandees/go-tilepacks/tilepack"
)

type TileRequest struct {
	Tile *tilepack.Tile
	URL  string
}

type TileResponse struct {
	Tile    *tilepack.Tile
	Data    []byte
	Elapsed float64
}

func httpWorker(wg *sync.WaitGroup, id int, client *http.Client, jobs chan *TileRequest, results chan *TileResponse) {
	defer wg.Done()

	for request := range jobs {
		start := time.Now()
		resp, err := client.Get(request.URL)
		if err != nil {
			log.Printf("Error on HTTP request: %+v", err)
		}

		secs := time.Since(start).Seconds()
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			log.Printf("Error copying bytes from HTTP response: %+v", err)
		}

		results <- &TileResponse{
			Tile:    request.Tile,
			Data:    body,
			Elapsed: secs,
		}
	}
}

func processResults(waitGroup *sync.WaitGroup, results chan *TileResponse, processor tilepack.TileOutputter) {
	defer waitGroup.Done()

	counter := 0
	for result := range results {
		err := processor.Save(result.Tile, result.Data)
		if err != nil {
			log.Printf("Couldn't save tile %+v", err)
		}

		counter++

		if counter%10000 == 0 {
			log.Printf("Saved %dk tiles", counter/1000)
		}
	}
	log.Printf("Saved %d tiles", counter)

	err := processor.Close()
	if err != nil {
		log.Printf("Error closing processor: %+v", err)
	}
}

func main() {

	urlTemplateStr := flag.String("url", "", "URL template to make tile requests with.")
	outputStr := flag.String("output", "", "Path to output mbtiles file.")
	boundingBoxStr := flag.String("bounds", "-90.0,-180.0,90.0,180.0", "Comma-separated bounding box in south,west,north,east format. Defaults to the whole world.")
	zoomsStr := flag.String("zooms", "0,1,2,3,4,5,6,7,8,9,10", "Comma-separated list of zoom levels.")
	numHTTPWorkers := flag.Int("workers", 25, "Number of HTTP client workers to use.")
	flag.Parse()

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

	// Configure the HTTP client with a timeout and connection pools
	httpClient := &http.Client{}
	httpClient.Timeout = 30 * time.Second
	httpTransport := &http.Transport{
		MaxIdleConnsPerHost: 500,
	}
	httpClient.Transport = httpTransport

	mbtilesOutputter, err := tilepack.NewMbtilesOutputter(*outputStr)
	if err != nil {
		log.Fatalf("Couldn't create mbtiles output: %+v", err)
	}

	mbtilesOutputter.CreateTiles()
	log.Println("Created mbtiles output")

	jobs := make(chan *TileRequest, 1000)
	results := make(chan *TileResponse, 1000)

	// Start up the HTTP workers that will fetch tiles
	workerWG := &sync.WaitGroup{}
	for w := 0; w < *numHTTPWorkers; w++ {
		workerWG.Add(1)
		go httpWorker(workerWG, w, httpClient, jobs, results)
	}

	// Start the worker that receives data from HTTP workers
	resultWG := &sync.WaitGroup{}
	resultWG.Add(1)
	go processResults(resultWG, results, mbtilesOutputter)

	// Add tile request jobs
	tilepack.GenerateTiles(bounds, zooms, func(tile *tilepack.Tile) {
		url := strings.NewReplacer(
			"{x}", fmt.Sprintf("%d", tile.X),
			"{y}", fmt.Sprintf("%d", tile.Y),
			"{z}", fmt.Sprintf("%d", tile.Z)).Replace(*urlTemplateStr)

		jobs <- &TileRequest{
			URL:  url,
			Tile: tile,
		}
	})
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
