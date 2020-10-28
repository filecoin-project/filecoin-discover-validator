package dagger

import (
	"sync"

	"github.com/ipfs/go-qringbuf"
	"github.com/filecoin-project/filecoin-discover-validator/internal/constants"
	dgrblock "github.com/filecoin-project/filecoin-discover-validator/internal/dagger/block"

	"github.com/filecoin-project/filecoin-discover-validator/chunker"
	dgrchunker "github.com/filecoin-project/filecoin-discover-validator/internal/dagger/chunker"
	"github.com/filecoin-project/filecoin-discover-validator/internal/dagger/chunker/fixedsize"

	dgrcollector "github.com/filecoin-project/filecoin-discover-validator/internal/dagger/collector"
	"github.com/filecoin-project/filecoin-discover-validator/internal/dagger/collector/filcommp"
)

var availableChunkers = map[string]dgrchunker.Initializer{
	"fixed-size": fixedsize.NewChunker,
}
var availableCollectors = map[string]dgrcollector.Initializer{
	"fil-commP": filcommp.NewCollector,
}

type dgrChunkerUnit struct {
	_         constants.Incomparabe
	instance  chunker.Chunker
	constants dgrchunker.InstanceConstants
}

type carUnit struct {
	_      constants.Incomparabe
	hdr    *dgrblock.Header
	region *qringbuf.Region
}

type Dagger struct {
	// speederization shortcut flags for internal logic
	generateRoots bool

	curStreamOffset   int64
	cfg               config
	statSummary       statSummary
	chainedChunkers   []dgrChunkerUnit
	chainedCollectors []dgrcollector.Collector
	cidFormatter      func(*dgrblock.Header) string
	shutdownSemaphore chan struct{}
	qrb               *qringbuf.QuantizedRingBuffer
	asyncWG           sync.WaitGroup
	mu                sync.Mutex
	seenRoots         seenRoots
}

func (dgr *Dagger) Destroy() {

	dgr.mu.Lock()

	if constants.PerformSanityChecks && dgr.cfg.AsyncHashersCount > 0 {
		dgr.orderlyHashersShutdown()
	} else {
		close(dgr.shutdownSemaphore)
	}

	dgr.shutdownSemaphore = nil
	dgr.qrb = nil
	dgr.mu.Unlock()
}
