package filcommp

import (
	sha256simd "github.com/minio/sha256-simd"
	dgrcollector "github.com/filecoin-project/filecoin-discover-validator/internal/dagger/collector"
	"github.com/filecoin-project/filecoin-discover-validator/internal/dagger/util/argparser"
)

func NewCollector(args []string, dgrCfg *dgrcollector.DaggerConfig) (_ dgrcollector.Collector, initErrs []string) {

	if args == nil {
		initErrs = argparser.SubHelp(
			"None\n",
			nil,
		)
		return
	}

	if len(args) > 1 {
		initErrs = append(initErrs, "collector takes no arguments")
	}
	if dgrCfg.NextCollector != nil || dgrCfg.ChainPosition != 1 {
		initErrs = append(initErrs, "collector must be used standalone, can not be mixed with others")
	}

	if len(initErrs) > 0 {
		return
	}

	// Initialize collector
	cp := &commpCollector{
		fr32WorkBuf:       make([]byte, StrideSize),
		stackedNulPadding: make([][]byte, maxLayers),
		globalShutdown:    dgrCfg.ShutdownSemaphore,
	}

	// initialize the nul padding stack (cheap to do upfront)
	h := sha256simd.New()
	for i := range cp.stackedNulPadding {

		if i == 0 {
			cp.stackedNulPadding[0] = make([]byte, 32)
		} else {
			h.Reset()

			// yes, twice
			h.Write(cp.stackedNulPadding[i-1])
			h.Write(cp.stackedNulPadding[i-1])

			cp.stackedNulPadding[i] = h.Sum(make([]byte, 0, 32))
			cp.stackedNulPadding[i][31] &= 0x3F
		}
	}

	// Initialize state
	cp.reset()

	return cp, initErrs
}
