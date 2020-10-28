package dgrchunker

import (
	"github.com/filecoin-project/filecoin-discover-validator/chunker"
	"github.com/filecoin-project/filecoin-discover-validator/internal/constants"
)

type InstanceConstants struct {
	_            constants.Incomparabe
	MinChunkSize int
	MaxChunkSize int
}

type DaggerConfig struct {
	IsLastInChain bool
}

type Initializer func(
	chunkerCLISubArgs []string,
	cfg *DaggerConfig,
) (
	instance chunker.Chunker,
	constants InstanceConstants,
	initErrorStrings []string,
)
