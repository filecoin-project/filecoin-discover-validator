// +build !tinygo

package dagger

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"time"
)

func (smr *statSummary) writeAsJSON(writeTo io.Writer) {

	// because the golang json encoder is rather garbage
	if smr.Roots == nil {
		smr.Roots = []rootStats{}
	}

	jsonl, err := json.Marshal(smr)
	if err != nil {
		log.Fatalf("Encoding stats failed: %s", err)
	}

	if _, err := fmt.Fprintf(writeTo, "%s\n", jsonl); err != nil {
		log.Fatalf("Emitting stats failed: %s", err)
	}
}

func (dgr *Dagger) orderlyHashersShutdown() {
	wantCount := runtime.NumGoroutine() - dgr.cfg.AsyncHashersCount
	close(dgr.shutdownSemaphore)

	if wantCount < 2 {
		return
	}

	// we will be checking for leaked goroutines - wait a bit for hashers to shut down
	dgr.mu.Unlock()
	for {
		time.Sleep(2 * time.Millisecond)
		if runtime.NumGoroutine() <= wantCount {
			break
		}
	}
	dgr.mu.Lock()
}

// https://github.com/tinygo-org/tinygo/issues/1285
var tinygoWorkaroundNumCPU = runtime.NumCPU
var tinygoWorkaroundGetpagesize = os.Getpagesize
var tinygoWorkaroundGoVersion = runtime.Version
