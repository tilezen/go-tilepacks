package tilepack

import (
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strings"
	"time"
)

type HTTPError struct {
	Code   int
	Status string
}

func (e *HTTPError) Error() string {
	return e.String()
}

func (e *HTTPError) String() string {
	return e.Status
}

const (
	httpUserAgent = "go-tilepacks/1.0"
)

func NewXYZJobGenerator(urlTemplate string, bounds *LngLatBbox, zooms []uint, httpTimeout time.Duration, invertedY bool, ensureGzip bool) (JobGenerator, error) {
	// Configure the HTTP client with a timeout and connection pools
	httpClient := &http.Client{}
	httpClient.Timeout = httpTimeout
	httpTransport := &http.Transport{
		MaxIdleConnsPerHost: 500,
		DisableCompression:  true,
	}
	httpClient.Transport = httpTransport

	return &xyzJobGenerator{
		httpClient:  httpClient,
		urlTemplate: urlTemplate,
		bounds:      bounds,
		zooms:       zooms,
		invertedY:   invertedY,
		ensureGzip:  ensureGzip,
	}, nil
}

func NewFileTransportXYZJobGenerator(root string, urlTemplate string, bounds *LngLatBbox, zooms []uint, httpTimeout time.Duration, invertedY bool, ensureGzip bool) (JobGenerator, error) {

	info, err := os.Stat(root)

	if err != nil {
		return nil, err
	}

	if !info.IsDir() {
		return nil, errors.New("Invalid root directory")
	}

	httpClient := &http.Client{}
	httpClient.Timeout = httpTimeout

	httpTransport := &http.Transport{}
	httpTransport.RegisterProtocol("file", http.NewFileTransport(http.Dir(root)))
	httpClient.Transport = httpTransport

	return &xyzJobGenerator{
		httpClient:  httpClient,
		urlTemplate: urlTemplate,
		bounds:      bounds,
		zooms:       zooms,
		invertedY:   invertedY,
		ensureGzip:  ensureGzip,
	}, nil
}

type xyzJobGenerator struct {
	httpClient  *http.Client
	urlTemplate string
	bounds      *LngLatBbox
	zooms       []uint
	invertedY   bool
	ensureGzip  bool
}

func doHTTPWithRetry(client *http.Client, request *http.Request, nRetries int) (*http.Response, error) {
	sleep := 500 * time.Millisecond

	for i := 0; i < nRetries; i++ {
		resp, err := client.Do(request)
		if err != nil {
			return nil, err
		}

		if resp.StatusCode == 200 {
			return resp, nil
		}

		resp.Body.Close()

		// log.Printf("Failed to GET (try %d) %+v: %+v", i, request.URL, resp.Status)

		// was previously
		// if resp.StatusCode > 500 && resp.StatusCode < 600 { sleep... }

		if resp.StatusCode <= 500 || resp.StatusCode >= 600 {
			return nil, &HTTPError{Code: resp.StatusCode, Status: resp.Status}
		}

		time.Sleep(sleep)
		sleep *= 2.0
		if sleep > 30.0 {
			sleep = 30 * time.Second
		}
	}

	return nil, fmt.Errorf("ran out of HTTP GET retries for %s", request.URL)
}

func (x *xyzJobGenerator) CreateWorker() (func(id int, jobs chan *TileRequest, results chan *TileResponse), error) {
	f := func(id int, jobs chan *TileRequest, results chan *TileResponse) {

		// Instantiate the gzip support stuff once instead on every iteration
		bodyBuffer := bytes.NewBuffer(nil)
		bodyGzipper := gzip.NewWriter(bodyBuffer)

		for request := range jobs {
			start := time.Now()

			httpReq, err := http.NewRequest("GET", request.URL, nil)
			if err != nil {
				log.Printf("Unable to create HTTP request: %+v", err)
				continue
			}

			httpReq.Header.Add("User-Agent", httpUserAgent)
			httpReq.Header.Add("Accept-Encoding", "gzip")

			resp, err := doHTTPWithRetry(x.httpClient, httpReq, 30)
			if err != nil {
				log.Printf("Skipping %+v: %+v", request, err)
				continue
			}

			var bodyData []byte
			contentEncoding := resp.Header.Get("Content-Encoding")

			switch contentEncoding {
			case "gzip":
				// If the server reports content encoding of gzip, we can just copy the bytes as-is
				bodyData, err = ioutil.ReadAll(resp.Body)
			default:

				if !x.ensureGzip {
					bodyData, err = ioutil.ReadAll(resp.Body)
				} else {

					// Otherwise we'll gzip the data, so we should
					// reset at the top in case we ran into a continue below
					bodyBuffer.Reset()
					bodyGzipper.Reset(bodyBuffer)

					_, err = io.Copy(bodyGzipper, resp.Body)
					if err != nil {
						log.Printf("Couldn't copy to gzipper: %+v", err)
						continue
					}

					err = bodyGzipper.Flush()
					if err != nil {
						log.Printf("Couldn't flush gzipper: %+v", err)
						continue
					}

					bodyData, err = ioutil.ReadAll(bodyBuffer)
				}

				if err != nil {
					log.Printf("Couldn't read bytes into byte array: %+v", err)
					continue
				}
			}
			resp.Body.Close()

			if err != nil {
				log.Printf("Error copying bytes from HTTP response: %+v", err)
				continue
			}

			secs := time.Since(start).Seconds()

			results <- &TileResponse{
				Tile:    request.Tile,
				Data:    bodyData,
				Elapsed: secs,
			}

			// Sleep a tiny bit to try to prevent thundering herd
			time.Sleep(time.Duration(rand.Intn(50)) * time.Millisecond)
		}
	}

	return f, nil
}

func (x *xyzJobGenerator) CreateJobs(jobs chan *TileRequest) error {
	consumer := func(tile *Tile) {
		url := strings.NewReplacer(
			"{x}", fmt.Sprintf("%d", tile.X),
			"{y}", fmt.Sprintf("%d", tile.Y),
			"{z}", fmt.Sprintf("%d", tile.Z)).Replace(x.urlTemplate)

		jobs <- &TileRequest{
			URL:  url,
			Tile: tile,
		}
	}

	opts := &GenerateTilesOptions{
		Bounds:       x.bounds,
		Zooms:        x.zooms,
		ConsumerFunc: consumer,
		InvertedY:    x.invertedY,
	}

	GenerateTiles(opts)

	return nil
}
