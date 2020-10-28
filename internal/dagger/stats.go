package dagger

import (
	"fmt"
	"log"

	"github.com/ipfs/go-qringbuf"
	dgrblock "github.com/filecoin-project/filecoin-discover-validator/internal/dagger/block"

	"github.com/filecoin-project/filecoin-discover-validator/internal/constants"
	"github.com/filecoin-project/filecoin-discover-validator/internal/util/text"
)

// The bit reduction is to make the internal seen map smaller memory-wise
// That many bits are taken off the *end* of any non-identity CID
// We could remove the shortening, but for now there's no reason to, and
// as an extra benefit it makes the murmur3 case *way* easier to code
const seenHashSize = 128 / 8

type blockPostProcessResult struct {
	_ constants.Incomparabe
}

type seenRoot struct {
	order int
	cid   []byte
}

type seenRoots map[[seenHashSize]byte]seenRoot

func seenKey(b *dgrblock.Header) (id *[seenHashSize]byte) {
	if b == nil {
		return
	}

	cid := b.Cid()
	id = new([seenHashSize]byte)
	copy(
		id[:],
		cid[(len(cid)-seenHashSize):],
	)
	return
}

type statSummary struct {
	EventType string `json:"event"`
	Dag       struct {
		Nodes   int64 `json:"nodes"`
		Size    int64 `json:"wireSize"`
		Payload int64 `json:"payload"`
	} `json:"logicalDag"`
	Streams  int64       `json:"subStreams"`
	Roots    []rootStats `json:"roots,omitempty"`
	SysStats struct {
		qringbuf.Stats
		ElapsedNsecs int64 `json:"elapsedNanoseconds"`

		// getrusage() section
		CpuUserNsecs int64 `json:"cpuUserNanoseconds"`
		CpuSysNsecs  int64 `json:"cpuSystemNanoseconds"`
		MaxRssBytes  int64 `json:"maxMemoryUsed"`
		MinFlt       int64 `json:"cacheMinorFaults"`
		MajFlt       int64 `json:"cacheMajorFaults"`
		BioRead      int64 `json:"blockIoReads,omitempty"`
		BioWrite     int64 `json:"blockIoWrites,omitempty"`
		Sigs         int64 `json:"signalsReceived,omitempty"`
		CtxSwYield   int64 `json:"contextSwitchYields"`
		CtxSwForced  int64 `json:"contextSwitchForced"`

		// for context
		PageSize int `json:"pageSize"`
		CPU      struct {
			NameStr        string `json:"name"`
			FeaturesStr    string `json:"features"`
			Cores          int    `json:"cores"`
			ThreadsPerCore int    `json:"threadsPerCore"`
			FreqMHz        int    `json:"mhz"`
			Vendor         string `json:"vendor"`
			Family         int    `json:"family"`
			Model          int    `json:"model"`
		} `json:"cpu"`
		GoMaxProcs int    `json:"goMaxProcs"`
		GoNumCPU   int    `json:"goNumCPU"`
		Os         string `json:"os"`

		ArgvExpanded []string `json:"argvExpanded"`
		ArgvInitial  []string `json:"argvInitial"`
		GoVersion    string   `json:"goVersion"`
	} `json:"sys"`
}
type rootStats struct {
	Cid         string `json:"cid"`
	SizeDag     uint64 `json:"wireSize"`
	SizePayload uint64 `json:"payload"`
	Dup         bool   `json:"duplicate,omitempty"`
}

func (dgr *Dagger) OutputSummary() {

	// no stats emitters - nowhere to output
	if dgr.cfg.emitters[emStatsText] == nil && dgr.cfg.emitters[emStatsJsonl] == nil {
		return
	}

	smr := &dgr.statSummary

	if statsJsonlOut := dgr.cfg.emitters[emStatsJsonl]; statsJsonlOut != nil {
		// emit the JSON last, so that piping to e.g. `jq` works nicer
		defer smr.writeAsJSON(statsJsonlOut)
	}

	statsTextOut := dgr.cfg.emitters[emStatsText]
	if statsTextOut == nil {
		return
	}

	var substreamsDesc string
	if dgr.cfg.MultipartStream {
		substreamsDesc = fmt.Sprintf(
			" from %s substreams",
			text.Commify64(dgr.statSummary.Streams),
		)
	}

	writeTextOutf := func(f string, args ...interface{}) {
		if _, err := fmt.Fprintf(statsTextOut, f, args...); err != nil {
			log.Fatalf("Emitting '%s' failed: %s", emStatsText, err)
		}
	}

	writeTextOutf(
		"\nRan on %d-core/%d-thread %s"+
			"\nProcessing took %0.2f seconds using %0.2f vCPU and %0.2f MiB peak memory"+
			"\nPerforming %s system reads using %0.2f vCPU at about %0.2f MiB/s"+
			"\n\nTotal payload length:%17s bytes%s\n\n",

		smr.SysStats.CPU.Cores,
		smr.SysStats.CPU.Cores*smr.SysStats.CPU.ThreadsPerCore,
		smr.SysStats.CPU.NameStr,

		float64(smr.SysStats.ElapsedNsecs)/
			1000000000,

		float64(smr.SysStats.CpuUserNsecs)/
			float64(smr.SysStats.ElapsedNsecs),

		float64(smr.SysStats.MaxRssBytes)/
			(1024*1024),

		text.Commify64(smr.SysStats.ReadCalls),

		float64(smr.SysStats.CpuSysNsecs)/
			float64(smr.SysStats.ElapsedNsecs),

		(float64(smr.Dag.Payload)/(1024*1024))/
			(float64(smr.SysStats.ElapsedNsecs)/1000000000),

		text.Commify64(smr.Dag.Payload),

		substreamsDesc,
	)

	if smr.Dag.Nodes > 0 {
		writeTextOutf(
			"Forming DAG covering:%17s bytes of %s logical nodes\n",
			text.Commify64(smr.Dag.Size), text.Commify64(smr.Dag.Nodes),
		)
	}

}
