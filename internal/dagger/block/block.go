package dgrblock

import (
	"github.com/filecoin-project/filecoin-discover-validator/chunker"
	"github.com/filecoin-project/filecoin-discover-validator/internal/constants"
	"github.com/filecoin-project/filecoin-discover-validator/internal/zcpstring"
)

func WrapCid(cid []byte, blockSize int, dagSize, payloadSize uint64) *Header {

	gone := int32(1)
	return &Header{
		cid:              cid,
		cidReady:         precomputedCidReady,
		contentGone:      &gone,
		sizeBlock:        blockSize,
		totalSizeDag:     dagSize,
		totalSizePayload: payloadSize,
	}
}

type Header struct {
	sizeBlock int
	// sizeCidRefs      int
	totalSizePayload uint64
	totalSizeDag     uint64
	cid              []byte
	cidReady         chan struct{}
	contentGone      *int32
	content          *zcpstring.ZcpString
}

func (h *Header) Cid() []byte {
	<-h.cidReady
	return h.cid
}
func (h *Header) SizeBlock() int                { return h.sizeBlock }
func (h *Header) SizeCumulativeDag() uint64     { return h.totalSizeDag }
func (h *Header) SizeCumulativePayload() uint64 { return h.totalSizePayload }

type DataSource struct {
	_             constants.Incomparabe
	chunker.Chunk // critically *NOT* a reference, so that an empty DataSource{} is usable on its own
	Content       *zcpstring.ZcpString
}

type hashTask struct {
	postTruncateCidTo int
	hdr               *Header
}

// Makes code easier to follow - in most conditionals in maker below
// the CID is "ready" instantly/synchronously. It is only in the very
// last case that we spawn an actual goroutine: then we make a *new* channel
var precomputedCidReady = make(chan struct{})

func init() {
	close(precomputedCidReady)
}
