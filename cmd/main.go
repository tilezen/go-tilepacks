package main

import (
	"bytes"
	"compress/gzip"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/tilezen/go-tilepacks/tilepack"
)

type TileRequest struct {
	Tile *tilepack.Tile
	URL  string
	Gzip bool
}

type TileResponse struct {
	Tile    *tilepack.Tile
	Data    []byte
	Elapsed float64
}

const (
	httpUserAgent   = "go-tilepacks/1.0"
	saveLogInterval = 10000
)

func httpWorker(wg *sync.WaitGroup, id int, client *http.Client, jobs chan *TileRequest, results chan *TileResponse) {
	defer wg.Done()

	for request := range jobs {
		start := time.Now()

		httpReq, err := http.NewRequest("GET", request.URL, nil)
		if err != nil {
			log.Printf("Unable to create HTTP request: %+v", err)
			continue
		}

		httpReq.Header.Add("User-Agent", httpUserAgent)
		if request.Gzip {
			httpReq.Header.Add("Accept-Encoding", "gzip")
		}

		resp, err := client.Do(httpReq)
		if err != nil {
			log.Printf("Error on HTTP request: %+v", err)
			continue
		}

		if resp.StatusCode != 200 {
			log.Printf("Failed to GET %+v: %+v", request.URL, resp.Status)
			continue
		}

		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			log.Printf("Error copying bytes from HTTP response: %+v", err)
			continue
		}
		resp.Body.Close()

		secs := time.Since(start).Seconds()

		if request.Gzip {
			contentEncoding := resp.Header.Get("Content-Encoding")
			if contentEncoding != "gzip" {
				var b bytes.Buffer
				w := gzip.NewWriter(&b)
				w.Write(body)
				w.Close()
				body = b.Bytes()
			}
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

	urlTemplateStr := flag.String("url", "", "URL template to make tile requests with.")
	outputStr := flag.String("output", "", "Path to output mbtiles file.")
	boundingBoxStr := flag.String("bounds", "-90.0,-180.0,90.0,180.0", "Comma-separated bounding box in south,west,north,east format. Defaults to the whole world.")
	zoomsStr := flag.String("zooms", "0,1,2,3,4,5,6,7,8,9,10", "Comma-separated list of zoom levels.")
	numHTTPWorkers := flag.Int("workers", 25, "Number of HTTP client workers to use.")
	gzipEnabled := flag.Bool("gzip", false, "Request gzip encoding from server and store gzipped contents in mbtiles. Will gzip locally if server doesn't do it.")
	requestTimeout := flag.Int("timeout", 60, "HTTP client timeout for tile requests.")
	cpuProfile := flag.String("cpuprofile", "", "Enables CPU profiling. Saves the dump to the given path.")
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

	// Configure the HTTP client with a timeout and connection pools
	httpClient := &http.Client{}
	httpClient.Timeout = time.Duration(*requestTimeout) * time.Second
	httpTransport := &http.Transport{
		MaxIdleConnsPerHost: 500,
		DisableCompression:  true,
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
			Gzip: *gzipEnabled,
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
