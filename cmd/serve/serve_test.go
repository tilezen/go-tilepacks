package main

import (
	"bytes"
	"log"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestLoggingMiddleware(t *testing.T) {
	// loggingMiddleware must pass the request to the next handler and write a
	// log line containing the request path, method, and remote address.
	var logBuf bytes.Buffer
	logger := log.New(&logBuf, "", 0)

	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	})
	handler := loggingMiddleware(logger)(inner)

	req := httptest.NewRequest("GET", "/tilepath", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code != 200 {
		t.Errorf("expected 200, got %d", rr.Code)
	}
	if !bytes.Contains(logBuf.Bytes(), []byte("/tilepath")) {
		t.Errorf("expected log entry to contain '/tilepath', got: %s", logBuf.String())
	}
}

func TestDefaultHandler(t *testing.T) {
	// defaultHandler must return 404 for any request — it is the fallback
	// route for paths that don't match the tile or preview endpoints.
	req := httptest.NewRequest("GET", "/unrecognized", nil)
	rr := httptest.NewRecorder()
	defaultHandler(rr, req)
	if rr.Code != 404 {
		t.Errorf("expected 404, got %d", rr.Code)
	}
}
