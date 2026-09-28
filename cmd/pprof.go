package main

import (
	"net/http"
	_ "net/http/pprof"
	"os"
)

// RABBIT_PPROF=127.0.0.1:6060 exposes net/http/pprof for profiling.
func init() {
	if addr := os.Getenv("RABBIT_PPROF"); addr != "" {
		go http.ListenAndServe(addr, nil)
	}
}
