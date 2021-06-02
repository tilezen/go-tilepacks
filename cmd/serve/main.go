package main

import (
	"flag"
	"log"
	gohttp "net/http"
	"os"
	"time"

	"github.com/tilezen/go-tilepacks/http"
	"github.com/tilezen/go-tilepacks/tilepack"
)

func loggingMiddleware(logger *log.Logger) func(gohttp.Handler) gohttp.Handler {
	return func(next gohttp.Handler) gohttp.Handler {
		return gohttp.HandlerFunc(func(w gohttp.ResponseWriter, r *gohttp.Request) {
			defer func() {
				logger.Println(r.Method, r.URL.Path, r.RemoteAddr, r.UserAgent())
			}()
			next.ServeHTTP(w, r)
		})
	}
}

func main() {
	mbtilesFile := flag.String("input", "", "The name of the mbtiles file to serve from.")
	addr := flag.String("listen", ":8080", "The address and port to listen on")
	flag.Parse()

	logger := log.New(os.Stdout, "http: ", log.LstdFlags)

	if *mbtilesFile == "" {
		logger.Fatal("Need to provide --input parameter")
	}

	reader, err := tilepack.NewMbtilesReader(*mbtilesFile)
	if err != nil {
		logger.Fatalf("Couldn't create MBtilesReader, %v", err)
	}

	mbtilesHandler := http.MbtilesHandler(reader)

	router := gohttp.NewServeMux()
	router.HandleFunc("/preview.html", previewHTMLHandler)
	router.Handle("/tilezen/", mbtilesHandler)
	router.HandleFunc("/", defaultHandler)

	server := &gohttp.Server{
		Addr:         *addr,
		Handler:      loggingMiddleware(logger)(router),
		ErrorLog:     logger,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
		IdleTimeout:  30 * time.Second,
	}

	if err := server.ListenAndServe(); err != nil && err != gohttp.ErrServerClosed {
		logger.Fatalf("Could not listen on %s: %v\n", *addr, err)
	}

}

func previewHTMLHandler(w gohttp.ResponseWriter, r *gohttp.Request) {
	gohttp.ServeFile(w, r, "cmd/serve/static/preview.html")
}

func defaultHandler(w gohttp.ResponseWriter, r *gohttp.Request) {
	gohttp.NotFound(w, r)
}
