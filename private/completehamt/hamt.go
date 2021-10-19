package completehamt

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"

	"github.com/ipfs/go-unixfs"
	"github.com/ipfs/go-unixfs/hamt"

	ipld "github.com/ipfs/go-ipld-format"
)

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
		panic("treeHeight > 8: we don't allow a key larger than what can be encoded in a 64-bit word")
	}
	//if HAMTHashFunction != IdHash {
	//	panic("we do not support a hash function other than ID")
	//}
	// FIXME: Any clean and simple way to do this? Otherwise remove check.

	rootShard, err := hamt.NewShard(ds, childsPerNode)
	if err != nil {
		return nil, err
	}

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

	return rootShard.Node()
}

// Return the same value as the hash.
func IdHash(val []byte) []byte {
	return val
}
