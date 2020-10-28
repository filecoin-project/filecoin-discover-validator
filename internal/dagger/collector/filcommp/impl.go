package filcommp

import (
	"errors"
	"fmt"
	"math"
	"sync"

	sha256simd "github.com/minio/sha256-simd"
	"github.com/filecoin-project/filecoin-discover-validator/internal/constants"
	dgrblock "github.com/filecoin-project/filecoin-discover-validator/internal/dagger/block"
)

const maxLayers = 31 // == log2( 64 GiB / 32 )

const StrideSize = constants.MaxLeafPayloadSize - (constants.MaxLeafPayloadSize % 127)
const MaxPiecePayload = uint64(127 * (((1 << maxLayers) * 32) / 128))

type state struct {
	shortChunkSeen bool
	payloadSize    uint64
	layerQueues    []chan hashTaskResult
	resultCommP    chan []byte
}

type commpCollector struct {
	sync.Mutex
	state
	fr32WorkBuf       []byte
	stackedNulPadding [][]byte
	globalShutdown    <-chan struct{}
}

type hashTaskResult []byte

func (*commpCollector) AppendBlock(*dgrblock.Header) error {
	return errors.New("unexpected invocation of commpCollector.AppendBlock()")
}

func (cp *commpCollector) FlushState() *dgrblock.Header {
	defer cp.reset()

	// This is how we signal to the bottom of the stack that we are done
	// which in turn collapses the rest all the way to cp.resultCommP
	close(cp.layerQueues[0])

	// cid := append(
	// 	make([]byte, 0, 7+32),
	// 	"\x01"+ // cid V1
	// 		"\x81\xe2\x03"+ // 0xF101
	// 		"\x92\x20"+ // 0x1012
	// 		"\x20"..., // 32 bytes of 254sha256
	// )

	// cid = append(cid, <-cp.resultCommP...)

	return dgrblock.WrapCid(
		<-cp.resultCommP,
		0,
		uint64(
			math.Pow(
				2,
				math.Ceil(
					math.Log2(
						float64(cp.payloadSize)/127,
					),
				)+7,
			),
		),
		cp.payloadSize,
	)
}

func (cp *commpCollector) hash254Into(out chan<- hashTaskResult, data ...[]byte) {

	h := sha256simd.New()
	for i := range data {
		h.Write(data[i])
	}
	d := h.Sum(data[0][:0]) // perfectly fine to reuse-reduce-recycle
	d[31] &= 0x3F

	out <- d
}

func (cp *commpCollector) reset() {
	// allocate a new state - GCing everything prior
	cp.state = state{
		layerQueues: make([]chan hashTaskResult, maxLayers),
		resultCommP: make(chan []byte, 1),
	}

	cp.layerQueues[0] = make(chan hashTaskResult, 1024) // SANCHECK: too much? too little?
	cp.addLayer(0)
}

func (cp *commpCollector) addLayer(myIdx int) {
	// the next layer channel, which we might *not* use
	cp.layerQueues[myIdx+1] = make(chan hashTaskResult, 1024) // SANCHECK: too much? too little?

	go func() {
		var chunkHold hashTaskResult

		for {

			select {
			case <-cp.globalShutdown:
				return
			case chunk, queueIsOpen := <-cp.layerQueues[myIdx]:

				// the dream is collapsing
				if !queueIsOpen {

					// I am last
					if cp.layerQueues[myIdx+2] == nil {
						cp.resultCommP <- chunkHold
						return
					}

					if chunkHold != nil {
						cp.hash254Into(
							cp.layerQueues[myIdx+1],
							chunkHold,
							cp.stackedNulPadding[myIdx+1], // stackedNulPadding is one longer than the main queue
						)
					}

					// signal the next in line that they are done too
					close(cp.layerQueues[myIdx+1])
					return
				}

				if chunkHold == nil {
					chunkHold = chunk
				} else {

					// I am last right now
					if cp.layerQueues[myIdx+2] == nil {
						cp.addLayer(myIdx + 1)
					}

					cp.hash254Into(cp.layerQueues[myIdx+1], chunkHold, chunk)
					chunkHold = nil
				}
			}
		}
	}()
}

func (cp *commpCollector) AppendData(ds dgrblock.DataSource) (*dgrblock.Header, error) {

	if cp.shortChunkSeen {
		return nil, errors.New("additional data appended after a short (thus supposedly final) chunk")
	}

	cp.fr32WorkBuf = ds.Content.AppendTo(cp.fr32WorkBuf[:0])
	cp.payloadSize += uint64(ds.Size)
	rem := cp.payloadSize % 127
	if rem != 0 {

		if cp.payloadSize < 127 {
			// https://github.com/filecoin-project/rust-fil-proofs/issues/1231
			return nil, errors.New("minimum input of 127 bytes required for commP calculation")
		}

		cp.shortChunkSeen = true
		cp.fr32WorkBuf = append(cp.fr32WorkBuf, make([]byte, 127-rem)...)
	}

	for wbufIdx := 0; wbufIdx < len(cp.fr32WorkBuf); wbufIdx += 127 {

		// holds this round's shifts of the original 127 bytes plus the 6 bit overflow
		// at the end of the expansion cycle
		expansion := make([]byte, 128)

		if cp.payloadSize > MaxPiecePayload {
			return nil, fmt.Errorf("maximum proving tree payload size of %d bytes exceeded", MaxPiecePayload)
		}

		window := cp.fr32WorkBuf[wbufIdx : wbufIdx+127 : wbufIdx+127]

		// Cycle over four(4) 31-byte groups, leaving 1 byte in between:
		// 31 + 1 + 31 + 1 + 31 + 1 + 31 = 127

		// First 31 bytes + 6 bits are taken as-is (trimmed later)
		// Note that copying them into the expansion buffer is not strictly
		// necessary: one could feed the range to the hasher directly. However
		// there are significant optimizations to be had when feeding exactly 64
		// bytes at a time to the sha256 implementation, thus keeping the copy()
		copy(expansion, window[:32])

		// first 2-bit "shim" forced into the otherwise identical bitstream
		expansion[31] &= 0x3F

		// simplify pointer math
		windowPlus1, expansionPlus1 := window[1:], expansion[1:]

		//  In: {{ C[7] C[6] }} X[7] X[6] X[5] X[4] X[3] X[2] X[1] X[0] Y[7] Y[6] Y[5] Y[4] Y[3] Y[2] Y[1] Y[0] Z[7] Z[6] Z[5]...
		// Out:                 X[5] X[4] X[3] X[2] X[1] X[0] C[7] C[6] Y[5] Y[4] Y[3] Y[2] Y[1] Y[0] X[7] X[6] Z[5] Z[4] Z[3]...
		for i := 31; i < 63; i++ {
			expansionPlus1[i] = windowPlus1[i]<<2 | window[i]>>6
		}

		// next 2-bit shim
		expansion[63] &= 0x3F

		//  In: {{ C[7] C[6] C[5] C[4] }} X[7] X[6] X[5] X[4] X[3] X[2] X[1] X[0] Y[7] Y[6] Y[5] Y[4] Y[3] Y[2] Y[1] Y[0] Z[7] Z[6] Z[5]...
		// Out:                           X[3] X[2] X[1] X[0] C[7] C[6] C[5] C[4] Y[3] Y[2] Y[1] Y[0] X[7] X[6] X[5] X[4] Z[3] Z[2] Z[1]...
		for i := 63; i < 95; i++ {
			expansionPlus1[i] = windowPlus1[i]<<4 | window[i]>>4
		}

		// next 2-bit shim
		expansion[95] &= 0x3F

		//  In: {{ C[7] C[6] C[5] C[4] C[3] C[2] }} X[7] X[6] X[5] X[4] X[3] X[2] X[1] X[0] Y[7] Y[6] Y[5] Y[4] Y[3] Y[2] Y[1] Y[0] Z[7] Z[6] Z[5]...
		// Out:                                     X[1] X[0] C[7] C[6] C[5] C[4] C[3] C[2] Y[1] Y[0] X[7] X[6] X[5] X[4] X[3] X[2] Z[1] Z[0] Y[7]...
		for i := 95; i < 126; i++ {
			expansionPlus1[i] = windowPlus1[i]<<6 | window[i]>>2
		}
		// the final 6 bit remainder is exactly the value of the last expanded byte
		expansion[127] = window[126] >> 2

		cp.hash254Into(cp.layerQueues[0], expansion[:64])
		cp.hash254Into(cp.layerQueues[0], expansion[64:])
	}

	return nil, nil
}
