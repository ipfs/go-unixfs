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
	childsPerNode := 16
	treeHeight := 2
	node, err := CreateCompleteHAMT(ds, treeHeight, childsPerNode)
	assert.NoError(t, err)

	shard, err := hamt.NewHamtFromDag(ds, node)
	assert.NoError(t, err)
	links, err := shard.EnumLinks(context.Background())
	assert.NoError(t, err)

	childNodes := int(math.Pow(float64(childsPerNode), float64(treeHeight)))
	//internalNodes := int(math.Pow(float64(childsPerNode), float64(treeHeight-1)))
	//totalNodes := childNodes + internalNodes
	assert.Equal(t, childNodes, len(links))
}
