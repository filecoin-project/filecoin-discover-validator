package dagger

import (
	"encoding/binary"
	"fmt"
	"io"
	"time"

	"github.com/ipfs/go-qringbuf"
	"github.com/filecoin-project/filecoin-discover-validator/internal/constants"
	"github.com/filecoin-project/filecoin-discover-validator/internal/zcpstring"

	"github.com/filecoin-project/filecoin-discover-validator/chunker"
	dgrblock "github.com/filecoin-project/filecoin-discover-validator/internal/dagger/block"

	"github.com/filecoin-project/filecoin-discover-validator/internal/util/text"
)

// SANCHECK: not sure if any of these make sense, nor have I measured the cost
const (
	chunkQueueSizeTop      = 256
	chunkQueueSizeSubchunk = 32
)

const (
	ErrorString = IngestionEventType(iota)
	NewRootJsonl
)

type IngestionEvent struct {
	_    constants.Incomparabe
	Type IngestionEventType
	Body string
}
type IngestionEventType int

var preProcessTasks, postProcessTasks func(dgr *Dagger)

func (dgr *Dagger) ProcessReader(inputReader io.Reader) (commp []byte, err error) {

	var t0 time.Time

	defer func() {

		// a little helper to deal with error stack craziness
		deferErrors := make(chan error, 1)

		// if we are already in error - just put it on the channel
		// we already sent the event earlier
		if err != nil {
			deferErrors <- err
		}

		// we need to wait all crunching to complete, then shutdown emitter, then measure/return
		dgr.asyncWG.Wait()

		if err == nil && len(deferErrors) > 0 {
			err = <-deferErrors
		}

		if postProcessTasks != nil {
			postProcessTasks(dgr)
		}

		dgr.qrb = nil

		dgr.statSummary.SysStats.ElapsedNsecs = time.Since(t0).Nanoseconds()
	}()

	defer func() {
		if err != nil {

			var buffered int
			if dgr.qrb != nil {
				dgr.qrb.Lock()
				buffered = dgr.qrb.Buffered()
				dgr.qrb.Unlock()
			}

			err = fmt.Errorf(
				"failure at byte offset %s of sub-stream #%d with %s bytes buffered/unprocessed: %s",
				text.Commify64(dgr.curStreamOffset),
				dgr.statSummary.Streams,
				text.Commify(buffered),
				err,
			)
		}
	}()

	if preProcessTasks != nil {
		preProcessTasks(dgr)
	}
	t0 = time.Now()

	dgr.qrb, err = qringbuf.NewFromReader(inputReader, qringbuf.Config{
		// MinRegion must be twice the maxchunk, otherwise chunking chains won't work (hi, Claude Shannon)
		MinRegion:   2 * constants.MaxLeafPayloadSize,
		MinRead:     dgr.cfg.RingBufferMinRead,
		MaxCopy:     2 * constants.MaxLeafPayloadSize, // SANCHECK having it equal to the MinRegion may be daft...
		BufferSize:  dgr.cfg.RingBufferSize,
		SectorSize:  dgr.cfg.RingBufferSectSize,
		Stats:       &dgr.statSummary.SysStats.Stats,
		TrackTiming: ((dgr.cfg.StatsActive & gatherStatsRingbuf) == gatherStatsRingbuf),
	})
	if err != nil {
		return
	}

	// use 64bits everywhere
	var substreamSize int64

	// outer stream loop: read() syscalls happen only here and in the qrb.collector()
	for {
		if dgr.cfg.MultipartStream {

			err := binary.Read(
				inputReader,
				binary.BigEndian,
				&substreamSize,
			)
			dgr.statSummary.SysStats.ReadCalls++

			if err == io.EOF {
				// no new multipart coming - bail
				break
			} else if err != nil {
				return nil, fmt.Errorf(
					"error reading next 8-byte multipart substream size: %s",
					err,
				)
			}

			if substreamSize == 0 {
				continue
			}

			dgr.statSummary.Streams++
			dgr.curStreamOffset = 0
		}

		if dgr.cfg.MultipartStream && substreamSize == 0 {
			// If we got here: cfg.ProcessNulInputs is true
			// Special case for a one-time zero-CID emission
			if err := dgr.streamAppend(nil); err != nil {
				return nil, err
			}
		} else if err := dgr.processStream(substreamSize); err != nil {
			if err == io.ErrUnexpectedEOF {
				return nil, fmt.Errorf(
					"unexpected end of substream #%s after %s bytes (stream expected to be %s bytes long)",
					text.Commify64(dgr.statSummary.Streams),
					text.Commify64(dgr.curStreamOffset+int64(dgr.qrb.Buffered())),
					text.Commify64(substreamSize),
				)
			} else if err != io.EOF {
				return nil, err
			}
		}

		if dgr.generateRoots || dgr.seenRoots != nil {

			// cascading flush across the chain
			var rootBlock *dgrblock.Header
			for _, c := range dgr.chainedCollectors {
				rootBlock = c.FlushState()
			}

			if rootBlock != nil {

				if dgr.seenRoots != nil {
					dgr.mu.Lock()

					var rootSeen bool
					if sk := seenKey(rootBlock); sk != nil {
						if _, rootSeen = dgr.seenRoots[*sk]; !rootSeen {
							dgr.seenRoots[*sk] = seenRoot{
								order: len(dgr.seenRoots),
								cid:   rootBlock.Cid(),
							}
						}
					}

					dgr.statSummary.Roots = append(dgr.statSummary.Roots, rootStats{
						Cid:         dgr.cidFormatter(rootBlock),
						SizePayload: rootBlock.SizeCumulativePayload(),
						SizeDag:     rootBlock.SizeCumulativeDag(),
						Dup:         rootSeen,
					})

					dgr.mu.Unlock()
				}
			}

			return rootBlock.Cid(), nil
		}

		// we are in EOF-state: if we are not expecting multiparts - we are done
		if !dgr.cfg.MultipartStream {
			break
		}
	}

	return
}

// This is essentially a union:
// - either subSplits will be provided for recursion
// - or a chunk with its region will be sent
type recursiveSplitResult struct {
	_              constants.Incomparabe
	subSplits      <-chan *recursiveSplitResult
	chunkBufRegion *qringbuf.Region
	chunk          chunker.Chunk
}

type chunkingInconsistencyHandler func(
	chunkerIdx int,
	workRegionStreamOffset int64,
	workRegionSize,
	workRegionPos int,
	errStr string,
)

func (dgr *Dagger) processStream(streamLimit int64) error {

	// begin reading and filling buffer
	if err := dgr.qrb.StartFill(streamLimit); err != nil {
		return err
	}

	var streamEndInView bool
	var availableFromReader, processedFromReader int
	var streamOffset int64

	chunkingErr := make(chan error)

	// this callback is passed through the recursive chain instead of a bare channel
	// providing reasonable diag context
	errHandler := func(chunkerIdx int, wrOffset int64, wrSize, wrPos int, errStr string) {
		chunkingErr <- fmt.Errorf(`

chunking error
--------------
StreamEndInView:     %17t
MainRegionSize:      %17s
SubRegionSize:       %17s
ErrorAtSubRegionPos: %17s
SubRegionRemaining:  %17s
StreamOffset:        %17s
SubBufOffset:        %17s
ErrorOffset:         %17s
chunker: #%d %T
--------------
chunking error: %s%s`,
			streamEndInView,
			text.Commify(availableFromReader),
			text.Commify(wrSize),
			text.Commify(wrPos),
			text.Commify(wrSize-wrPos),
			text.Commify64(streamOffset),
			text.Commify64(wrOffset),
			text.Commify64(wrOffset+int64(wrPos)),
			chunkerIdx,
			dgr.chainedChunkers[chunkerIdx].instance,
			errStr,
			"\n\n",
		)
	}

	for {

		// next 2 lines evaluate processedInRound and availableForRound from *LAST* iteration
		streamOffset += int64(processedFromReader)
		workRegion, readErr := dgr.qrb.NextRegion(availableFromReader - processedFromReader)

		if workRegion == nil || (readErr != nil && readErr != io.EOF) {
			return readErr
		}

		availableFromReader = workRegion.Size()
		processedFromReader = 0
		streamEndInView = (readErr == io.EOF)

		rescursiveSplitResults := make(chan *recursiveSplitResult, chunkQueueSizeTop)
		go dgr.recursivelySplitBuffer(
			// The entire reserved buffer to split recursively
			// Guaranteed to be left intact until we call NextSlice
			workRegion,
			// Where we are (for error reporting)
			streamOffset,
			// We need to tell the top chunker when nothing else is coming, to prevent the entire chain repeating work otherwise
			streamEndInView,
			// Index of top chunker
			0,
			// the channel for the chunking results
			rescursiveSplitResults,
			// func() instead of a channel, closes over the common error channel and several stream position vars
			errHandler,
		)

	receiveChunks:
		for {
			select {
			case err := <-chunkingErr:
				return err
			case res, chanOpen := <-rescursiveSplitResults:
				if !chanOpen {
					break receiveChunks
				}
				processedSize, err := dgr.gatherRecursiveResults(res)
				processedFromReader += processedSize
				if err != nil {
					return err
				}
			}
		}

		dgr.statSummary.Dag.Payload += int64(processedFromReader)
	}
}

func (dgr *Dagger) gatherRecursiveResults(result *recursiveSplitResult) (int, error) {
	if result.subSplits != nil {
		var substreamSize int
		for {
			subRes, channelOpen := <-result.subSplits
			if !channelOpen {
				return substreamSize, nil
			}
			processedSize, err := dgr.gatherRecursiveResults(subRes)
			substreamSize += processedSize
			if err != nil {
				return substreamSize, err
			}
		}
	}

	result.chunkBufRegion.Reserve()
	return result.chunk.Size, dgr.streamAppend(result)
}

func (dgr *Dagger) recursivelySplitBuffer(
	workRegion *qringbuf.Region,
	workRegionStreamOffset int64,
	useEntireRegion bool,
	chunkerIdx int,
	recursiveResultsReturn chan<- *recursiveSplitResult,
	errHandler chunkingInconsistencyHandler,
) {
	var processedBytes int

	dgr.chainedChunkers[chunkerIdx].instance.Split(
		workRegion.Bytes(),
		useEntireRegion,
		func(c chunker.Chunk) error {

			if c.Size <= 0 ||
				c.Size > workRegion.Size()-processedBytes {
				err := fmt.Errorf("returned chunk size %s out of bounds", text.Commify(c.Size))
				errHandler(chunkerIdx, workRegionStreamOffset, workRegion.Size(), processedBytes,
					err.Error(),
				)
				return err
			}

			if len(dgr.chainedChunkers) > chunkerIdx+1 && !c.Meta.Bool("no-subchunking") {
				// we are not last in the chain - subchunk
				subSplits := make(chan *recursiveSplitResult, chunkQueueSizeSubchunk)
				go dgr.recursivelySplitBuffer(
					workRegion.SubRegion(
						processedBytes,
						c.Size,
					),
					workRegionStreamOffset+int64(processedBytes),
					true, // subchunkers always "use entire region" by definition
					chunkerIdx+1,
					subSplits,
					errHandler,
				)
				recursiveResultsReturn <- &recursiveSplitResult{subSplits: subSplits}
			} else {
				recursiveResultsReturn <- &recursiveSplitResult{
					chunk: c,
					chunkBufRegion: workRegion.SubRegion(
						processedBytes,
						c.Size,
					),
				}
			}

			processedBytes += c.Size
			return nil
		},
	)

	if processedBytes == 0 &&
		len(dgr.chainedChunkers) > chunkerIdx+1 {
		// We didn't manage to find *anything*, and there is a subsequent chunker
		// Pass it the entire frame in a "tail-call" fashion, have *them* close
		// the result channel when done
		dgr.recursivelySplitBuffer(
			workRegion,
			workRegionStreamOffset,
			useEntireRegion,
			chunkerIdx+1,
			recursiveResultsReturn,
			errHandler,
		)
		return
	}

	if processedBytes <= 0 || processedBytes > workRegion.Size() {
		errHandler(
			chunkerIdx,
			workRegionStreamOffset,
			workRegion.Size(),
			processedBytes,
			fmt.Sprintf(
				"returned %s bytes as chunks for a buffer of %s bytes",
				text.Commify(processedBytes),
				text.Commify(workRegion.Size()),
			),
		)
	}

	close(recursiveResultsReturn)
}

func (dgr *Dagger) streamAppend(res *recursiveSplitResult) error {

	var ds dgrblock.DataSource
	var dr *qringbuf.Region
	if res != nil {
		dr = res.chunkBufRegion
		ds.Chunk = res.chunk
		ds.Content = zcpstring.WrapSlice(dr.Bytes())

	}

	// the central spot where we advance the "global" counter
	dgr.curStreamOffset += int64(ds.Size)

	_, err := dgr.chainedCollectors[0].AppendData(ds)
	if err != nil {
		return err
	}

	dr.Release()
	return nil
}
