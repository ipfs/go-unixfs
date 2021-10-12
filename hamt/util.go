package hamt

import (
	"context"
	"encoding/binary"
	"fmt"
	ipld "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/go-unixfs"
	"github.com/spaolacci/murmur3"
	"math"
	"math/bits"
)

// hashBits is a helper that allows the reading of the 'next n bits' as an integer.
type hashBits struct {
	b        []byte
	consumed int
}

func newHashBits(val string) *hashBits {
	return &hashBits{b: HAMTHashFunction([]byte(val))}
}

func newConsumedHashBits(val string, consumed int) *hashBits {
	hv := &hashBits{b: HAMTHashFunction([]byte(val))}
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

func logtwo(v int) (int, error) {
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

// ONLY FOR TESTING: Return the same value as the hash.
func IdHash(val []byte) []byte {
	return val
}

// CreateCompleteHAMT creates a HAMT the following properties:
// * its height (distance/edges from root to deepest node) is specified by treeHeight.
// * all leaf Shard nodes have the same depth (and have only 'value' links).
// * all internal Shard nodes point only to other Shards (and hence have zero 'value' links).
// * the total number of 'value' links (directory entries) is:
//   io.DefaultShardWidth ^ (treeHeight + 1).
// FIXME: HAMTHashFunction needs to be set to IdHash by the caller. We depend on
//  this simplification for the current logic to work. (HAMTHashFunction is a
//  global setting of the package, it is hard-coded in the serialized Shard node
//  and not allowed to be changed on a per HAMT/Shard basis.)
//  (If we didn't rehash inside setValue then we could just generate
//  the fake hash as in io.SetAndPrevious through `newHashBits()` and pass
//  it as an argument making the hash independent of tree manipulation; that
//  sounds as the correct way to go in general and we wouldn't need this.)
func CreateCompleteHAMT(ds ipld.DAGService, treeHeight int, childsPerNode int) (ipld.Node, error) {
	if treeHeight < 1 {
		panic("treeHeight < 1")
	}
	if treeHeight > 8 {
		panic("treeHeight > 8: we don't allow a key larger than what can be enconded in a 64-bit word")
	}
	//if HAMTHashFunction != IdHash {
	//	panic("we do not support a hash function other than ID")
	//}
	// FIXME: Any clean and simple way to do this? Otherwise remove check.

	rootShard, err := NewShard(ds, childsPerNode)
	if err != nil {
		return nil, err
	}
	// FIXME: Do we need to set the CID builder? Not part of the NewShard
	//  interface so it shouldn't be mandatory.

	// Assuming we are using the ID hash function we can just insert all
	// the combinations of a byte slice that will reach the desired height.
	totalChildren := int(math.Pow(float64(childsPerNode), float64(treeHeight)))
	for i := 0; i < totalChildren; i++ {
		var hashbuf [8]byte
		binary.LittleEndian.PutUint64(hashbuf[:], uint64(i))
		var oldLink *ipld.Link
		// FIXME: This is wrong for childsPerNode/DefaultShardWidth different
		//  than 256 (i.e., one byte of key per level).
		oldLink, err = rootShard.SetAndPrevious(context.Background(), string(hashbuf[:treeHeight]), unixfs.EmptyFileNode())
		if err != nil {
			return nil, err
		}
		if oldLink != nil {
			// We shouldn't be overwriting any value, otherwise the tree
			// won't be complete.
			return nil, fmt.Errorf("we have overwritten entry %s",
				oldLink.Cid)
		}
	}
	// FIXME: Check depth of every Shard to be sure?

	return rootShard.Node()
}
