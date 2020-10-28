package dagger

import (
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"sort"
	"strings"

	"github.com/klauspost/cpuid"

	dgrchunker "github.com/filecoin-project/filecoin-discover-validator/internal/dagger/chunker"
	dgrcollector "github.com/filecoin-project/filecoin-discover-validator/internal/dagger/collector"
	"github.com/filecoin-project/filecoin-discover-validator/internal/dagger/collector/filcommp"

	"github.com/pborman/getopt/v2"
	"github.com/pborman/options"
	"github.com/filecoin-project/filecoin-discover-validator/internal/constants"
	"github.com/filecoin-project/filecoin-discover-validator/internal/dagger/util/argparser"
	"github.com/filecoin-project/filecoin-discover-validator/internal/util/text"
)

type config struct {
	optSet *getopt.Set

	// where to output
	emitters emissionTargets

	//
	// Bulk of CLI options definition starts here, the rest further down in initArgvParser()
	//

	Help            bool `getopt:"-h --help         Display basic help"`
	HelpAll         bool `getopt:"--help-all        Display full help including options for every currently supported chunker/collector"`
	MultipartStream bool `getopt:"--multipart       Expect multiple SInt64BE-size-prefixed streams on stdIN"`

	emittersStdErr []string // Emitter spec: option/helptext in initArgvParser()
	emittersStdOut []string // Emitter spec: option/helptext in initArgvParser()

	// no-option-attached, these are instantiation error accumulators
	erroredChunkers   []string
	erroredCollectors []string

	AsyncHashersCount  int `getopt:"--async-hashers=integer         Number of concurrent short-lived goroutines performing hashing. Set to 0 (disable) for predictable benchmarking. Default:"`
	RingBufferSize     int `getopt:"--ring-buffer-size=bytes        The size of the quantized ring buffer used for ingestion. Default:"`
	RingBufferSectSize int `getopt:"--ring-buffer-sync-size=bytes   (EXPERT SETTING) The size of each buffer synchronization sector. Default:"` // option vaguely named 'sync' to not confuse users
	RingBufferMinRead  int `getopt:"--ring-buffer-min-sysread=bytes (EXPERT SETTING) Perform next read(2) only when the specified amount of free space is available in the buffer. Default:"`

	StatsActive uint `getopt:"--stats-active=uint   A bitfield representing activated stat aggregations: bit0:BlockSizing, bit1:RingbufferTiming. Default:"`

	requestedChunkers   string // Chunker chain: option/helptext in initArgvParser()
	requestedCollectors string // Collector chain: option/helptext in initArgvParser()
}

const (
	gatherStatsBlocks = 1 << iota
	gatherStatsRingbuf
)

type emissionTargets map[string]io.Writer

const (
	emNone       = "none"
	emStatsText  = "stats-text"
	emStatsJsonl = "stats-jsonl"
	emRootsJsonl = "roots-jsonl"
)

// where the CLI initial error messages go
var argParseErrOut = os.Stderr

func NewFromArgv(argv []string) (dgr *Dagger) {

	dgr = &Dagger{
		// Some minimal non-controversial defaults, all overridable
		// Try really hard to *NOT* have defaults that influence resulting CIDs
		cfg: config{
			AsyncHashersCount: tinygoWorkaroundNumCPU(), // SANCHECK yes, this is high: seems the simd version knows what to do...

			StatsActive: 0 | gatherStatsBlocks,

			// RingBufferSize: 2*constants.MaxLeafPayloadSize + 256*1024, // bare-minimum with defaults
			RingBufferSize: 24 * 1024 * 1024, // SANCHECK low seems good somehow... fits in L3 maybe?

			//SANCHECK: these numbers have not been validated
			RingBufferMinRead:  256 * 1024,
			RingBufferSectSize: 64 * 1024,

			emittersStdOut: []string{emRootsJsonl},
			emittersStdErr: []string{emStatsText},

			// not defaults but rather the list of known/configured emitters
			emitters: emissionTargets{
				emNone:       nil,
				emStatsText:  nil,
				emStatsJsonl: nil,
				emRootsJsonl: nil,
			},
		},

		shutdownSemaphore: make(chan struct{}),
	}

	// init some constants
	{
		s := &dgr.statSummary
		s.EventType = "summary"

		s.SysStats.ArgvInitial = make([]string, len(argv)-1)
		copy(s.SysStats.ArgvInitial, argv[1:])

		s.SysStats.PageSize = tinygoWorkaroundGetpagesize()
		s.SysStats.Os = runtime.GOOS
		s.SysStats.GoMaxProcs = runtime.GOMAXPROCS(-1)
		s.SysStats.GoNumCPU = tinygoWorkaroundNumCPU()
		s.SysStats.GoVersion = tinygoWorkaroundGoVersion()
		s.SysStats.CPU.NameStr = cpuid.CPU.BrandName
		s.SysStats.CPU.Cores = cpuid.CPU.PhysicalCores
		s.SysStats.CPU.ThreadsPerCore = cpuid.CPU.ThreadsPerCore
		s.SysStats.CPU.FreqMHz = int(cpuid.CPU.Hz / 1000000)
		s.SysStats.CPU.Vendor = cpuid.CPU.VendorString
		s.SysStats.CPU.Family = cpuid.CPU.Family
		s.SysStats.CPU.Model = cpuid.CPU.Model

		feats := cpuid.CPU.Features.Strings()
		sort.Strings(feats)
		s.SysStats.CPU.FeaturesStr = strings.Join(feats, " ")
	}

	cfg := &dgr.cfg
	cfg.initArgvParser()

	// accumulator for multiple errors, to present to the user all at once
	argParseErrs := argparser.Parse(argv, cfg.optSet)

	if cfg.Help || cfg.HelpAll {
		cfg.printUsage()
		os.Exit(0)
	}

	// commP is very special
	if cfg.requestedCollectors == "fil-commP" {
		requiredChunker := fmt.Sprintf("fixed-size_%d", filcommp.StrideSize)
		if cfg.requestedChunkers == "" {
			cfg.requestedChunkers = requiredChunker
		}
		if cfg.requestedChunkers != requiredChunker {
			argParseErrs = append(argParseErrs, fmt.Sprintf(
				"fil-commP requires a specific chunkers spec of '%s'",
				requiredChunker,
			))
		}
	}

	argParseErrs = append(argParseErrs, dgr.setupChunkerChain()...)
	argParseErrs = append(argParseErrs, dgr.setupCollectorChain()...)
	argParseErrs = append(argParseErrs, dgr.setupEmitters()...)

	if len(argParseErrs) != 0 {
		fmt.Fprint(argParseErrOut, "\nFatal error parsing arguments:\n\n")
		cfg.printUsage()

		sort.Strings(argParseErrs)
		fmt.Fprintf(
			argParseErrOut,
			"Fatal error parsing arguments:\n\t%s\n",
			strings.Join(argParseErrs, "\n\t"),
		)
		os.Exit(2)
	}

	// Opts *still* check out - take a snapshot of what we ended up with

	// All cid-determining opt come last in a predefined order
	cidOpts := []string{
		"chunkers",
		"collectors",
	}
	cidOptsIdx := map[string]struct{}{}
	for _, n := range cidOpts {
		cidOptsIdx[n] = struct{}{}
	}

	// first do the generic options
	cfg.optSet.VisitAll(func(o getopt.Option) {
		switch o.LongName() {
		case "help", "help-all", "ipfs-add-compatible-command":
			// do nothing for these
		default:
			// skip these keys too, they come next
			if _, exists := cidOptsIdx[o.LongName()]; !exists {
				dgr.statSummary.SysStats.ArgvExpanded = append(
					dgr.statSummary.SysStats.ArgvExpanded, fmt.Sprintf(`--%s=%s`,
						o.LongName(),
						o.Value().String(),
					),
				)
			}
		}
	})
	sort.Strings(dgr.statSummary.SysStats.ArgvExpanded)

	// now do the remaining cid-determining options
	for _, n := range cidOpts {
		dgr.statSummary.SysStats.ArgvExpanded = append(
			dgr.statSummary.SysStats.ArgvExpanded, fmt.Sprintf(`--%s=%s`,
				n,
				cfg.optSet.GetValue(n),
			),
		)
	}

	return
}

func (cfg *config) printUsage() {
	cfg.optSet.PrintUsage(argParseErrOut)
	if cfg.HelpAll || len(cfg.erroredChunkers) > 0 || len(cfg.erroredCollectors) > 0 {
		printPluginUsage(
			argParseErrOut,
			cfg.erroredCollectors,
			cfg.erroredChunkers,
		)
	} else {
		fmt.Fprint(argParseErrOut, "\nTry --help-all for more info\n\n")
	}
}

func printPluginUsage(
	out io.Writer,
	listCollectors []string,
	listChunkers []string,
) {

	// if nothing was requested explicitly - list everything
	if len(listCollectors) == 0 && len(listChunkers) == 0 {
		for name, initializer := range availableCollectors {
			if initializer != nil {
				listCollectors = append(listCollectors, name)
			}
		}
		for name, initializer := range availableChunkers {
			if initializer != nil {
				listChunkers = append(listChunkers, name)
			}
		}
	}

	if len(listCollectors) != 0 {
		fmt.Fprint(out, "\n")
		sort.Strings(listCollectors)
		for _, name := range listCollectors {
			fmt.Fprintf(
				out,
				"[C]ollector '%s'\n",
				name,
			)
			_, h := availableCollectors[name](nil, nil)
			if len(h) == 0 {
				fmt.Fprint(out, "  -- no helptext available --\n\n")
			} else {
				fmt.Fprintln(out, strings.Join(h, "\n"))
			}
		}
	}

	if len(listChunkers) != 0 {
		fmt.Fprint(out, "\n")
		sort.Strings(listChunkers)
		for _, name := range listChunkers {
			fmt.Fprintf(
				out,
				"[C]hunker '%s'\n",
				name,
			)
			_, _, h := availableChunkers[name](nil, nil)
			if len(h) == 0 {
				fmt.Fprint(out, "  -- no helptext available --\n\n")
			} else {
				fmt.Fprintln(out, strings.Join(h, "\n"))
			}
		}
	}

	fmt.Fprint(out, "\n")
}

func (cfg *config) initArgvParser() {
	// The default documented way of using pborman/options is to muck with globals
	// Operate over objects instead, allowing us to re-parse argv multiple times
	o := getopt.New()
	if err := options.RegisterSet("", cfg, o); err != nil {
		log.Fatalf("option set registration failed: %s", err)
	}
	cfg.optSet = o

	// program does not take freeform args
	// need to override this for sensible help render
	o.SetParameters("")

	// Several options have the help-text assembled programmatically
	o.FlagLong(&cfg.requestedChunkers, "chunkers", 0,
		"Stream chunking algorithm chain. Each chunker is one of: "+text.AvailableMapKeys(availableChunkers),
		"ch1_o1.1_o1.2_..._o1.N__ch2_o2.1_o2.2_..._o2.N__ch3_...",
	)
	o.FlagLong(&cfg.requestedCollectors, "collectors", 0,
		"Node-forming algorithm chain. Each collector is one of: "+text.AvailableMapKeys(availableCollectors),
		"co1_o1.1_o1.2_..._o1.N__co2_...",
	)
	o.FlagLong(&cfg.emittersStdErr, "emit-stderr", 0, fmt.Sprintf(
		"One or more emitters to activate on stdERR. Available emitters are %s. Default: ",
		text.AvailableMapKeys(cfg.emitters),
	), "comma,sep,emitters")
	o.FlagLong(&cfg.emittersStdOut, "emit-stdout", 0,
		"One or more emitters to activate on stdOUT. Available emitters same as above. Default: ",
		"comma,sep,emitters",
	)
}

func (dgr *Dagger) setupEmitters() (argErrs []string) {

	activeStderr := make(map[string]bool, len(dgr.cfg.emittersStdErr))
	for _, s := range dgr.cfg.emittersStdErr {
		activeStderr[s] = true
		if val, exists := dgr.cfg.emitters[s]; !exists {
			argErrs = append(argErrs, fmt.Sprintf("invalid emitter '%s' specified for --emit-stderr. Available emitters are: %s",
				s,
				text.AvailableMapKeys(dgr.cfg.emitters),
			))
		} else if s == emNone {
			continue
		} else if val != nil {
			argErrs = append(argErrs, fmt.Sprintf("Emitter '%s' specified more than once", s))
		} else {
			dgr.cfg.emitters[s] = os.Stderr
		}
	}
	activeStdout := make(map[string]bool, len(dgr.cfg.emittersStdOut))
	for _, s := range dgr.cfg.emittersStdOut {
		activeStdout[s] = true
		if val, exists := dgr.cfg.emitters[s]; !exists {
			argErrs = append(argErrs, fmt.Sprintf("invalid emitter '%s' specified for --emit-stdout. Available emitters are: %s",
				s,
				text.AvailableMapKeys(dgr.cfg.emitters),
			))
		} else if s == emNone {
			continue
		} else if val != nil {
			argErrs = append(argErrs, fmt.Sprintf("Emitter '%s' specified more than once", s))
		} else {
			dgr.cfg.emitters[s] = os.Stdout
		}
	}

	for _, exclusiveEmitter := range []string{
		emNone,
		emStatsText,
	} {
		if activeStderr[exclusiveEmitter] && len(activeStderr) > 1 {
			argErrs = append(argErrs, fmt.Sprintf(
				"When specified, emitter '%s' must be the sole argument to --emit-stderr",
				exclusiveEmitter,
			))
		}
		if activeStdout[exclusiveEmitter] && len(activeStdout) > 1 {
			argErrs = append(argErrs, fmt.Sprintf(
				"When specified, emitter '%s' must be the sole argument to --emit-stdout",
				exclusiveEmitter,
			))
		}
	}

	// set couple shortcuts based on emitter config
	dgr.generateRoots = (dgr.cfg.emitters[emRootsJsonl] != nil || dgr.cfg.emitters[emStatsJsonl] != nil)

	return
}

func (dgr *Dagger) setupChunkerChain() (argErrs []string) {

	// bail early
	if dgr.cfg.requestedChunkers == "" {
		return []string{
			"You must specify at least one stream chunker via '--chunkers=algname1_opt1_opt2__algname2_...'. Available chunker names are: " +
				text.AvailableMapKeys(availableChunkers),
		}
	}

	individualChunkers := strings.Split(dgr.cfg.requestedChunkers, "__")

	for chunkerNum, chunkerCmd := range individualChunkers {
		chunkerArgs := strings.Split(chunkerCmd, "_")
		init, exists := availableChunkers[chunkerArgs[0]]
		if !exists {
			argErrs = append(argErrs, fmt.Sprintf(
				"Chunker '%s' not found. Available chunker names are: %s",
				chunkerArgs[0],
				text.AvailableMapKeys(availableChunkers),
			))
			continue
		}

		for n := range chunkerArgs {
			if n > 0 {
				chunkerArgs[n] = "--" + chunkerArgs[n]
			}
		}

		chunkerInstance, chunkerConstants, initErrors := init(
			chunkerArgs,
			&dgrchunker.DaggerConfig{
				IsLastInChain: (chunkerNum == len(individualChunkers)-1),
			},
		)

		if len(initErrors) == 0 {
			if chunkerConstants.MaxChunkSize < 1 || chunkerConstants.MaxChunkSize > constants.MaxLeafPayloadSize {
				initErrors = append(initErrors, fmt.Sprintf(
					"returned MaxChunkSize constant '%d' out of range [1:%d]",
					chunkerConstants.MaxChunkSize,
					constants.MaxLeafPayloadSize,
				))
			} else if chunkerConstants.MinChunkSize < 0 || chunkerConstants.MinChunkSize > chunkerConstants.MaxChunkSize {
				initErrors = append(initErrors, fmt.Sprintf(
					"returned MinChunkSize constant '%d' out of range [0:%d]",
					chunkerConstants.MinChunkSize,
					chunkerConstants.MaxChunkSize,
				))
			}
		}

		if len(initErrors) > 0 {
			dgr.cfg.erroredChunkers = append(dgr.cfg.erroredChunkers, chunkerArgs[0])
			for _, e := range initErrors {
				argErrs = append(argErrs, fmt.Sprintf(
					"Initialization of chunker '%s' failed: %s",
					chunkerArgs[0],
					e,
				))
			}
		} else {
			dgr.chainedChunkers = append(dgr.chainedChunkers, dgrChunkerUnit{
				instance:  chunkerInstance,
				constants: chunkerConstants,
			})
		}
	}

	return
}

func (dgr *Dagger) setupCollectorChain() (argErrs []string) {

	// bail early
	if dgr.cfg.optSet.IsSet("collectors") && dgr.cfg.requestedCollectors == "" {
		return []string{
			"When specified, collector chain must be in the form '--collectors=algname1_opt1_opt2__algname2_...'. Available collector names are: " +
				text.AvailableMapKeys(availableCollectors),
		}
	}

	commonCfg := dgrcollector.DaggerConfig{
		AsyncHashersCount: dgr.cfg.AsyncHashersCount,
		ShutdownSemaphore: dgr.shutdownSemaphore,
	}

	for _, c := range dgr.chainedChunkers {
		if c.constants.MaxChunkSize > commonCfg.ChunkerChainMaxResult {
			commonCfg.ChunkerChainMaxResult = c.constants.MaxChunkSize
		}
	}

	individualCollectors := strings.Split(dgr.cfg.requestedCollectors, "__")

	// we need to process the collectors in reverse, in order to populate NextCollector
	dgr.chainedCollectors = make([]dgrcollector.Collector, len(individualCollectors))
	for collectorNum := len(individualCollectors); collectorNum > 0; collectorNum-- {

		collectorCmd := individualCollectors[collectorNum-1]

		collectorArgs := strings.Split(collectorCmd, "_")
		init, exists := availableCollectors[collectorArgs[0]]
		if !exists {
			argErrs = append(argErrs, fmt.Sprintf(
				"Collector '%s' not found. Available collector names are: %s",
				collectorArgs[0],
				text.AvailableMapKeys(availableCollectors),
			))
			continue
		}

		for n := range collectorArgs {
			if n > 0 {
				collectorArgs[n] = "--" + collectorArgs[n]
			}
		}

		collectorCfg := commonCfg // SHALLOW COPY!!!
		collectorCfg.ChainPosition = collectorNum
		if collectorNum != len(individualCollectors) {
			collectorCfg.NextCollector = dgr.chainedCollectors[collectorNum]
		}

		if collectorInstance, initErrors := init(
			collectorArgs,
			&collectorCfg,
		); len(initErrors) > 0 {

			dgr.cfg.erroredCollectors = append(dgr.cfg.erroredCollectors, collectorArgs[0])
			for _, e := range initErrors {
				argErrs = append(argErrs, fmt.Sprintf(
					"Initialization of collector '%s' failed: %s",
					collectorArgs[0],
					e,
				))
			}
		} else {
			dgr.chainedCollectors[collectorNum-1] = collectorInstance
		}
	}

	return
}
