package completehamt

import (
	"context"
	"math"
	"testing"

	"github.com/ipfs/go-unixfs/hamt"

	mdtest "github.com/ipfs/go-merkledag/test"
	"github.com/stretchr/testify/assert"
)

func TestCreateCompleteShard(t *testing.T) {
	ds := mdtest.Mock()
	childsPerNode := 256
	treeHeight := 2 // This is the limit of what we can fastly generate,
	// the default width is too big (256). We may need to refine
	// CreateCompleteHAMT encoding of the key to reduce the tableSize.
	node, err := CreateCompleteHAMT(ds, treeHeight, 256)
	assert.NoError(t, err)

	shard, err := hamt.NewHamtFromDag(ds, node)
	assert.NoError(t, err)
	links, err := shard.EnumAll(context.Background())
	assert.NoError(t, err)

	childNodes := int(math.Pow(float64(childsPerNode), float64(treeHeight)))
	//internalNodes := int(math.Pow(float64(childsPerNode), float64(treeHeight-1)))
	//totalNodes := childNodes + internalNodes
	assert.Equal(t, childNodes, len(links))
}
