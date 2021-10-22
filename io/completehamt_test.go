package io

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"testing"

	mdtest "github.com/ipfs/go-merkledag/test"
	"github.com/stretchr/testify/assert"

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
// FIXME: HAMTHashFunction needs to be set to idHash by the caller. We depend on
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

	rootShard, err := hamt.NewShard(ds, childsPerNode)
	if err != nil {
		return nil, err
	}

	// Assuming we are using the ID hash function we can just insert all
	// the combinations of a byte slice that will reach the desired height.
	totalChildren := int(math.Pow(float64(childsPerNode), float64(treeHeight)))
	log2ofChilds, err := hamt.Logtwo(childsPerNode)
	if err != nil {
		return nil, err
	}
	if log2ofChilds*treeHeight%8 != 0 {
		return nil, fmt.Errorf("childsPerNode * treeHeight should be multiple of 8")
	}
	bytesInKey := log2ofChilds * treeHeight / 8
	for i := 0; i < totalChildren; i++ {
		var hashbuf [8]byte
		binary.LittleEndian.PutUint64(hashbuf[:], uint64(i))
		var oldLink *ipld.Link
		oldLink, err = rootShard.SetAndPrevious(context.Background(), string(hashbuf[:bytesInKey]), unixfs.EmptyFileNode())
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
func idHash(val []byte) []byte {
	return val
}

func TestCreateCompleteShard(t *testing.T) {
	ds := mdtest.Mock()
	childsPerNode := 16
	treeHeight := 2
	node, err := CreateCompleteHAMT(ds, treeHeight, childsPerNode)
	assert.NoError(t, err)

	shard, err := hamt.NewHamtFromDag(ds, node)
	assert.NoError(t, err)
	links, err := shard.EnumLinks(context.Background())
	assert.NoError(t, err)

	childNodes := int(math.Pow(float64(childsPerNode), float64(treeHeight)))
	assert.Equal(t, childNodes, len(links))
}
