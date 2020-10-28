package encoding

import (
	"encoding/binary"
	"io"
	"log"
	"math"
	"math/bits"

	"github.com/filecoin-project/filecoin-discover-validator/internal/constants"
)

func VarintWireSize(v uint64) int {
	if constants.PerformSanityChecks && v > math.MaxInt64 {
		log.Panicf("Value %#v too large for a varint: https://github.com/multiformats/unsigned-varint#practical-maximum-of-9-bytes-for-security", v)
	}

	if v == 0 {
		return 1
	}

	return (bits.Len64(v) + 6) / 7
}
func VarintSlice(v uint64) []byte {
	return AppendVarint(
		make([]byte, 0, VarintWireSize(v)),
		v,
	)
}
func AppendVarint(tgt []byte, v uint64) []byte {
	for v > 127 {
		tgt = append(tgt, byte(v|128))
		v >>= 7
	}
	return append(tgt, byte(v))
}

func CborHeaderWiresize(l uint64) int {
	switch {
	case l <= 23:
		return 1
	case l <= math.MaxUint8:
		return 2
	case l <= math.MaxUint16:
		return 3
	case l <= math.MaxUint32:
		return 5
	default:
		return 9
	}
}

func CborHeaderWrite(w io.Writer, t byte, l uint64) (err error) {
	switch {

	case l <= 23:
		_, err = w.Write([]byte{(t << 5) | byte(l)})

	case l <= math.MaxUint8:
		_, err = w.Write([]byte{
			(t << 5) | 24,
			uint8(l),
		})

	case l <= math.MaxUint16:
		var b [3]byte
		b[0] = (t << 5) | 25
		binary.BigEndian.PutUint16(b[1:], uint16(l))
		_, err = w.Write(b[:])

	case l <= math.MaxUint32:
		var b [5]byte
		b[0] = (t << 5) | 26
		binary.BigEndian.PutUint32(b[1:], uint32(l))
		_, err = w.Write(b[:])

	default:
		var b [9]byte
		b[0] = (t << 5) | 27
		binary.BigEndian.PutUint64(b[1:], l)
		_, err = w.Write(b[:])

	}

	return
}
