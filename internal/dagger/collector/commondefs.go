package dgrcollector

import (
	dgrblock "github.com/filecoin-project/filecoin-discover-validator/internal/dagger/block"
)

type Collector interface {
	AppendData(formLeafBlockAndAppend dgrblock.DataSource) (resultingLeafBlock *dgrblock.Header, err error)
	AppendBlock(blockToAppendToStream *dgrblock.Header) error
	FlushState() (rootBlockAfterReducingAndDestroyingObjectState *dgrblock.Header)
}

type DaggerConfig struct {
	ChunkerChainMaxResult int // used for initialization sanity checks
	ChainPosition         int // used for DAG-stats layering

	// for collectors doing their own hashing
	AsyncHashersCount int
	ShutdownSemaphore <-chan struct{}

	NextCollector Collector
}

type Initializer func(
	collectorCLISubArgs []string,
	cfg *DaggerConfig,
) (instance Collector, initErrorStrings []string)
