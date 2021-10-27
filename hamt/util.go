package hamt

import (
	"fmt"
	"math/bits"

	"github.com/ipfs/go-unixfs/internal"

	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"github.com/spaolacci/murmur3"
)

// hashBits is a helper that allows the reading of the 'next n bits' as an integer.
type hashBits struct {
	b        []byte
	consumed int
}

func newHashBits(val string) *hashBits {
	return &hashBits{b: internal.HAMTHashFunction([]byte(val))}
}

func newConsumedHashBits(val string, consumed int) *hashBits {
	hv := &hashBits{b: internal.HAMTHashFunction([]byte(val))}
	hv.consumed = consumed
	return hv
}

func mkmask(n int) byte {
	return (1 << uint(n)) - 1
}

// Next returns the next 'i' bits of the hashBits value as an integer, or an
// error if there aren't enough bits.
func (hb *hashBits) Next(i int) (int, error) {
	if hb.consumed+i > len(hb.b)*8 {
		return 0, fmt.Errorf("sharded directory too deep")
	}
	return hb.next(i), nil
}

func (hb *hashBits) next(i int) int {
	curbi := hb.consumed / 8
	leftb := 8 - (hb.consumed % 8)

	curb := hb.b[curbi]
	if i == leftb {
		out := int(mkmask(i) & curb)
		hb.consumed += i
		return out
	} else if i < leftb {
		a := curb & mkmask(leftb) // mask out the high bits we don't want
		b := a & ^mkmask(leftb-i) // mask out the low bits we don't want
		c := b >> uint(leftb-i)   // shift whats left down
		hb.consumed += i
		return int(c)
	} else {
		out := int(mkmask(leftb) & curb)
		out <<= uint(i - leftb)
		hb.consumed += leftb
		out += hb.next(i - leftb)
		return out
	}
}

func Logtwo(v int) (int, error) {
	if v <= 0 {
		return 0, fmt.Errorf("hamt size should be a power of two")
	}
	lg2 := bits.TrailingZeros(uint(v))
	if 1<<uint(lg2) != v {
		return 0, fmt.Errorf("hamt size should be a power of two")
	}
	return lg2, nil
}

func murmur3Hash(val []byte) []byte {
	h := murmur3.New64()
	h.Write(val)
	return h.Sum(nil)
}

// We convert CIDs to arbitrary strings as CIDv1 raw identity hashes.
func packBytesIntoCID(data []byte) cid.Cid {
	h, err := multihash.Sum(data, multihash.IDENTITY, -1)
	if err != nil {
		panic(fmt.Sprintf("error while packing data %s: %s",
			data, err))
	}
	return cid.NewCidV1(cid.Raw, h)
}

func unpackBytesFromCID(c cid.Cid) []byte {
	if c.Version() != 1 {
		panic("trying to unpack CIDv0")
	}
	decoded, err := multihash.Decode(c.Hash())
	if err != nil {
		panic(fmt.Sprintf("error while unpacking data from CID %s: %s",
			c, err))
	}
	return decoded.Digest
}

// Coordinate in a HAMT to single out a single node.
// For simplicity this is independent of the hash the node encodes and just
// the position in the nodes.
// See `getLinksHAMTNode` for details.
type hamtPos struct {
	// Slice of child positions (from the immediate parent perspective).
	// FIXME: We are assuming a shard width <= 256 (the default) to easily
	//  convert to and from a byte slice.
	childSlicePos []byte
}

func (hp *hamtPos) addChildPos(childIndex int) *hamtPos {
	return &hamtPos{ append(hp.childSlicePos, byte(childIndex))}
}

func posFromBytes(data []byte) *hamtPos{
	return &hamtPos{data}
}

func posToBytes(hp *hamtPos) []byte {
	return hp.childSlicePos
}
