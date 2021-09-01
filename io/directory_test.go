package io

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	cid "github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
	mdag "github.com/ipfs/go-merkledag"
	mdtest "github.com/ipfs/go-merkledag/test"

	ft "github.com/ipfs/go-unixfs"
	"github.com/ipfs/go-unixfs/hamt"

	"github.com/stretchr/testify/assert"
)

func TestEmptyNode(t *testing.T) {
	n := ft.EmptyDirNode()
	if len(n.Links()) != 0 {
		t.Fatal("empty node should have 0 links")
	}
}

func TestDirectoryGrowth(t *testing.T) {
	ds := mdtest.Mock()
	dir := NewDirectory(ds)
	ctx := context.Background()

	d := ft.EmptyDirNode()
	ds.Add(ctx, d)

	nelems := 10000

	for i := 0; i < nelems; i++ {
		err := dir.AddChild(ctx, fmt.Sprintf("dir%d", i), d)
		if err != nil {
			t.Fatal(err)
		}
	}

	_, err := dir.GetNode()
	if err != nil {
		t.Fatal(err)
	}

	links, err := dir.Links(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if len(links) != nelems {
		t.Fatal("didnt get right number of elements")
	}

	dirc := d.Cid()

	names := make(map[string]bool)
	for _, l := range links {
		names[l.Name] = true
		if !l.Cid.Equals(dirc) {
			t.Fatal("link wasnt correct")
		}
	}

	for i := 0; i < nelems; i++ {
		dn := fmt.Sprintf("dir%d", i)
		if !names[dn] {
			t.Fatal("didnt find directory: ", dn)
		}

		_, err := dir.Find(context.Background(), dn)
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestDuplicateAddDir(t *testing.T) {
	ds := mdtest.Mock()
	dir := NewDirectory(ds)
	ctx := context.Background()
	nd := ft.EmptyDirNode()

	err := dir.AddChild(ctx, "test", nd)
	if err != nil {
		t.Fatal(err)
	}

	err = dir.AddChild(ctx, "test", nd)
	if err != nil {
		t.Fatal(err)
	}

	lnks, err := dir.Links(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if len(lnks) != 1 {
		t.Fatal("expected only one link")
	}
}

func TestBasicDirectory_estimatedSize(t *testing.T) {
	ds := mdtest.Mock()
	basicDir := newEmptyBasicDirectory(ds)

	testDirectorySizeEstimation(t, basicDir, ds, func(dir Directory) int {
		return dir.(*BasicDirectory).estimatedSize
	})
}

func TestHAMTDirectory_sizeChange(t *testing.T) {
	ds := mdtest.Mock()
	hamtDir, err := newEmptyHAMTDirectory(ds)
	assert.NoError(t, err)

	testDirectorySizeEstimation(t, hamtDir, ds, func(dir Directory) int {
		// Since we created a HAMTDirectory from scratch with size 0 its
		// internal sizeChange delta will in fact track the directory size
		// throughout this run.
		return dir.(*HAMTDirectory).sizeChange
	})
}

func fullSizeEnumeration(dir Directory) int {
	size := 0
	dir.ForEachLink(context.Background(), func(l *ipld.Link) error {
		size += estimatedLinkSize(l.Name, l.Cid)
		return nil
	})
	return size
}

func testDirectorySizeEstimation(t *testing.T, dir Directory, ds ipld.DAGService, size func(Directory) int) {
	estimatedLinkSize = mockLinkSizeFunc(1)
	defer func() { estimatedLinkSize = productionLinkSize }()

	ctx := context.Background()
	child := ft.EmptyFileNode()
	assert.NoError(t, ds.Add(ctx, child))

	// Several overwrites should not corrupt the size estimation.
	assert.NoError(t, dir.AddChild(ctx, "child", child))
	assert.NoError(t, dir.AddChild(ctx, "child", child))
	assert.NoError(t, dir.AddChild(ctx, "child", child))
	assert.NoError(t, dir.RemoveChild(ctx, "child"))
	assert.NoError(t, dir.AddChild(ctx, "child", child))
	assert.NoError(t, dir.RemoveChild(ctx, "child"))
	assert.Equal(t, 0, size(dir), "estimated size is not zero after removing all entries")

	dirEntries := 100
	for i := 0; i < dirEntries; i++ {
		assert.NoError(t, dir.AddChild(ctx, fmt.Sprintf("child-%03d", i), child))
	}
	assert.Equal(t, dirEntries, size(dir), "estimated size inaccurate after adding many entries")

	assert.NoError(t, dir.RemoveChild(ctx, "child-045")) // just random values
	assert.NoError(t, dir.RemoveChild(ctx, "child-063"))
	assert.NoError(t, dir.RemoveChild(ctx, "child-011"))
	assert.NoError(t, dir.RemoveChild(ctx, "child-000"))
	assert.NoError(t, dir.RemoveChild(ctx, "child-099"))
	dirEntries -= 5
	assert.Equal(t, dirEntries, size(dir), "estimated size inaccurate after removing some entries")

	// All of the following remove operations will fail (won't impact dirEntries):
	assert.Error(t, dir.RemoveChild(ctx, "nonexistent-name"))
	assert.Error(t, dir.RemoveChild(ctx, "child-045")) // already removed
	assert.Error(t, dir.RemoveChild(ctx, "child-100"))
	assert.Equal(t, dirEntries, size(dir), "estimated size inaccurate after failed remove attempts")

	// Restore a directory from original's node and check estimated size consistency.
	dirNode, err := dir.GetNode()
	assert.NoError(t, err)
	restoredDir, err := NewDirectoryFromNode(ds, dirNode.(*mdag.ProtoNode))
	assert.NoError(t, err)
	assert.Equal(t, size(dir), fullSizeEnumeration(restoredDir), "restored directory's size doesn't match original's")
	// We don't use the estimation size function for the restored directory
	// because in the HAMT case this function depends on the sizeChange variable
	// that will be cleared when loading the directory from the node.
	// This also covers the case of comparing the size estimation `size()` with
	// the full enumeration function `fullSizeEnumeration()` to make sure it's
	// correct.
}

// Any entry link size will have the fixedSize passed.
func mockLinkSizeFunc(fixedSize int) func(linkName string, linkCid cid.Cid) int {
	return func(_ string, _ cid.Cid) int {
		return fixedSize
	}
}

func checkBasicDirectory(t *testing.T, dir Directory, errorMessage string) {
	if _, ok := dir.(*UpgradeableDirectory).Directory.(*BasicDirectory); !ok {
		t.Fatal(errorMessage)
	}
}

func checkHAMTDirectory(t *testing.T, dir Directory, errorMessage string) {
	if _, ok := dir.(*UpgradeableDirectory).Directory.(*HAMTDirectory); !ok {
		t.Fatal(errorMessage)
	}
}

func TestProductionLinkSize(t *testing.T) {
	link, err := ipld.MakeLink(ft.EmptyDirNode())
	assert.NoError(t, err)
	link.Name = "directory_link_name"
	assert.Equal(t, 53, productionLinkSize(link.Name, link.Cid))

	link, err = ipld.MakeLink(ft.EmptyFileNode())
	assert.NoError(t, err)
	link.Name = "file_link_name"
	assert.Equal(t, 48, productionLinkSize(link.Name, link.Cid))

	ds := mdtest.Mock()
	basicDir := newEmptyBasicDirectory(ds)
	assert.NoError(t, err)
	for i := 0; i < 10; i++ {
		basicDir.AddChild(context.Background(), strconv.FormatUint(uint64(i), 10), ft.EmptyFileNode())
	}
	basicDirNode, err := basicDir.GetNode()
	assert.NoError(t, err)
	link, err = ipld.MakeLink(basicDirNode)
	assert.NoError(t, err)
	link.Name = "basic_dir"
	assert.Equal(t, 43, productionLinkSize(link.Name, link.Cid))
}

// Test HAMTDirectory <-> BasicDirectory switch based on directory size. The
// switch is managed by the UpgradeableDirectory abstraction.
func TestUpgradeableDirectorySwitch(t *testing.T) {
	oldHamtOption := HAMTShardingSize
	defer func() { HAMTShardingSize = oldHamtOption }()
	HAMTShardingSize = 0 // Disable automatic switch at the start.
	estimatedLinkSize = mockLinkSizeFunc(1)
	defer func() { estimatedLinkSize = productionLinkSize }()

	ds := mdtest.Mock()
	dir := NewDirectory(ds)
	checkBasicDirectory(t, dir, "new dir is not BasicDirectory")

	ctx := context.Background()
	child := ft.EmptyDirNode()
	err := ds.Add(ctx, child)
	assert.NoError(t, err)

	err = dir.AddChild(ctx, "1", child)
	assert.NoError(t, err)
	checkBasicDirectory(t, dir, "added child, option still disabled")

	// Set a threshold so big a new entry won't trigger the change.
	HAMTShardingSize = math.MaxInt32

	err = dir.AddChild(ctx, "2", child)
	assert.NoError(t, err)
	checkBasicDirectory(t, dir, "added child, option now enabled but at max")

	// Now set it so low to make sure any new entry will trigger the upgrade.
	HAMTShardingSize = 1

	// We are already above the threshold, we trigger the switch with an overwrite
	// (any AddChild() should reevaluate the size).
	err = dir.AddChild(ctx, "2", child)
	assert.NoError(t, err)
	checkHAMTDirectory(t, dir, "added child, option at min, should switch up")

	// Set threshold at the number of current entries and delete the last one
	// to trigger a switch and evaluate if the rest of the entries are conserved.
	HAMTShardingSize = 2
	err = dir.RemoveChild(ctx, "2")
	assert.NoError(t, err)
	checkBasicDirectory(t, dir, "removed threshold entry, option at min, should switch down")
}

func TestIntegrityOfDirectorySwitch(t *testing.T) {
	ds := mdtest.Mock()
	dir := NewDirectory(ds)
	checkBasicDirectory(t, dir, "new dir is not BasicDirectory")

	ctx := context.Background()
	child := ft.EmptyDirNode()
	err := ds.Add(ctx, child)
	assert.NoError(t, err)

	basicDir := newEmptyBasicDirectory(ds)
	hamtDir, err := newEmptyHAMTDirectory(ds)
	assert.NoError(t, err)
	for i := 0; i < 1000; i++ {
		basicDir.AddChild(ctx, strconv.FormatUint(uint64(i), 10), child)
		hamtDir.AddChild(ctx, strconv.FormatUint(uint64(i), 10), child)
	}
	compareDirectoryEntries(t, basicDir, hamtDir)

	hamtDirFromSwitch, err := basicDir.SwitchToSharding(ctx)
	assert.NoError(t, err)
	basicDirFromSwitch, err := hamtDir.switchToBasic(ctx)
	assert.NoError(t, err)
	compareDirectoryEntries(t, basicDir, basicDirFromSwitch)
	compareDirectoryEntries(t, hamtDir, hamtDirFromSwitch)
}

// Test that we fetch as little nodes as needed to reach the HAMTShardingSize
// during the sizeBelowThreshold computation.
// FIXME: This only works for a sequential DAG walk.
// FIXME: Failing in the CI for Ubuntu. This may likely be an indication of race
//  bug in the code, but `go test -race ./io/` is passing, so probably in the abuse
//  of static configurations being modified *inside* the tests.
func TestHAMTEnumerationWhenComputingSize(t *testing.T) {
	// Adjust HAMT global/static options for the test to simplify its logic.
	// FIXME: These variables weren't designed to be modified and we should
	//  review in depth side effects (like the probable Ubuntu error).
	// Set all link sizes to a uniform 1 so the estimated directory size
	// is just the count of its entry links (in HAMT/Shard terminology these
	// are the "value" links pointing to anything that is *not* another Shard).
	estimatedLinkSize = mockLinkSizeFunc(1)
	defer func() { estimatedLinkSize = productionLinkSize }()
	// Use an identity hash function to ease the construction of "complete" HAMTs
	// (see CreateCompleteHAMT below for more details). (Ideally this should be
	// a parameter we pass and not a global option we modify in the caller.)
	oldHashFunc := hamt.HAMTHashFunction
	defer func() { hamt.HAMTHashFunction = oldHashFunc }()
	hamt.HAMTHashFunction = hamt.IdHash
	oldShardWidth := DefaultShardWidth
	defer func() { DefaultShardWidth = oldShardWidth }()
	DefaultShardWidth = 256 // FIXME: Review number. From 256 to 8.

	// FIXME: Taken from private github.com/ipfs/go-merkledag@v0.2.3/merkledag.go.
	defaultConcurrentFetch := 32

	// We create a "complete" HAMT (see CreateCompleteHAMT for more details)
	// with a regular structure to be able to predict how many Shard nodes we
	// will need to fetch in order to reach the HAMTShardingSize threshold in
	// sizeBelowThreshold (assuming a sequential DAG walk function).
	oldHamtOption := HAMTShardingSize
	defer func() { HAMTShardingSize = oldHamtOption }()
	// (Some arbitrary values below that make this test not that expensive.)
	treeHeight := 2 // FIXME: Review number. From 2 to 3.
	// How many leaf shards nodes (with value links,
	// i.e., directory entries) do we need to reach the threshold.
	thresholdToWidthRatio := defaultConcurrentFetch
	// FIXME: Review dag.Walk algorithm to better figure out this estimate.

	HAMTShardingSize = DefaultShardWidth * thresholdToWidthRatio
	// With this structure we will then need to fetch the following nodes:
	// * `thresholdToWidthRatio` leaf Shards with enough value links to reach
	//    the HAMTShardingSize threshold.
	// * `(treeHeight - 1)` internal nodes to reach those leaf Shard nodes
	//    (assuming we have thresholdToWidthRatio below the DefaultShardWidth,
	//     i.e., all leaf nodes come from the same parent).
	//nodesToFetch := thresholdToWidthRatio + treeHeight - 1
	nodesToFetch := defaultConcurrentFetch * 2 // FIXME: Review.
	ds := mdtest.Mock()
	completeHAMTRoot, err := hamt.CreateCompleteHAMT(ds, treeHeight, DefaultShardWidth)
	assert.NoError(t, err)

	countGetsDS := newCountGetsDS(ds)
	hamtDir, err := newHAMTDirectoryFromNode(countGetsDS, completeHAMTRoot)
	assert.NoError(t, err)

	countGetsDS.resetCounter()
	countGetsDS.setRequestDelay(100 * time.Millisecond)
	// FIXME: Only works with sequential DAG walk (now hardcoded, needs to be
	//  added to the internal API) where we can predict the Get requests and
	//  tree traversal. It would be desirable to have some test for the concurrent
	//  walk (which is the one used in production).
	below, err := hamtDir.sizeBelowThreshold(context.TODO(), 0)
	assert.NoError(t, err)
	assert.False(t, below)
	assert.Equal(t, nodesToFetch, countGetsDS.uniqueCidsFetched())
	//assert.True(t, countGetsDS.uniqueCidsFetched() <= nodesToFetch)
}

// Compare entries in the leftDir against the rightDir and possibly
// missingEntries in the second.
func compareDirectoryEntries(t *testing.T, leftDir Directory, rightDir Directory) {
	leftLinks, err := getAllLinksSortedByName(leftDir)
	assert.NoError(t, err)
	rightLinks, err := getAllLinksSortedByName(rightDir)
	assert.NoError(t, err)

	assert.Equal(t, len(leftLinks), len(rightLinks))

	for i, leftLink := range leftLinks {
		assert.Equal(t, leftLink, rightLinks[i]) // FIXME: Can we just compare the entire struct?
	}
}

func getAllLinksSortedByName(d Directory) ([]*ipld.Link, error) {
	entries, err := d.Links(context.Background())
	if err != nil {
		return nil, err
	}
	sortLinksByName(entries)
	return entries, nil
}

func sortLinksByName(l []*ipld.Link) {
	sort.SliceStable(l, func(i, j int) bool {
		return strings.Compare(l[i].Name, l[j].Name) == -1 // FIXME: Is this correct?
	})
}

// FIXME: Remove if we end up not using this for the integrity checks.
//  (We could also get rid of getDagService() while doing it.)
//func copyDir(t *testing.T, d Directory) Directory {
//	dirNode, err := d.GetNode()
//	assert.NoError(t, err)
//	// Extract the DAG service from the directory (i.e., its link entries saved
//	// in it). This is not exposed in the interface and we won't change that now.
//	// FIXME: Still, this isn't nice.
//	var ds ipld.DAGService
//	switch v := d.(type) {
//	case *BasicDirectory:
//		ds = v.dserv
//	case *HAMTDirectory:
//		ds = v.dserv
//	case *UpgradeableDirectory:
//		ds = v.getDagService()
//	default:
//		panic("unknown directory type")
//	}
//	copiedDir, err := NewDirectoryFromNode(ds, dirNode)
//	assert.NoError(t, err)
//	return copiedDir
//}

func TestDirBuilder(t *testing.T) {
	ds := mdtest.Mock()
	dir := NewDirectory(ds)
	ctx := context.Background()

	child := ft.EmptyDirNode()
	err := ds.Add(ctx, child)
	if err != nil {
		t.Fatal(err)
	}

	count := 5000

	for i := 0; i < count; i++ {
		err := dir.AddChild(ctx, fmt.Sprintf("entry %d", i), child)
		if err != nil {
			t.Fatal(err)
		}
	}

	dirnd, err := dir.GetNode()
	if err != nil {
		t.Fatal(err)
	}

	links, err := dir.Links(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if len(links) != count {
		t.Fatal("not enough links dawg", len(links), count)
	}

	adir, err := NewDirectoryFromNode(ds, dirnd)
	if err != nil {
		t.Fatal(err)
	}

	links, err = adir.Links(ctx)
	if err != nil {
		t.Fatal(err)
	}

	names := make(map[string]bool)
	for _, lnk := range links {
		names[lnk.Name] = true
	}

	for i := 0; i < count; i++ {
		n := fmt.Sprintf("entry %d", i)
		if !names[n] {
			t.Fatal("COULDNT FIND: ", n)
		}
	}

	if len(links) != count {
		t.Fatal("wrong number of links", len(links), count)
	}

	linkResults := dir.EnumLinksAsync(ctx)

	asyncNames := make(map[string]bool)
	var asyncLinks []*ipld.Link

	for linkResult := range linkResults {
		if linkResult.Err != nil {
			t.Fatal(linkResult.Err)
		}
		asyncNames[linkResult.Link.Name] = true
		asyncLinks = append(asyncLinks, linkResult.Link)
	}

	for i := 0; i < count; i++ {
		n := fmt.Sprintf("entry %d", i)
		if !asyncNames[n] {
			t.Fatal("COULDNT FIND: ", n)
		}
	}

	if len(asyncLinks) != count {
		t.Fatal("wrong number of links", len(asyncLinks), count)
	}
}

// countGetsDS is a DAG service that keeps track of the number of
// unique CIDs fetched.
type countGetsDS struct {
	ipld.DAGService

	cidsFetched map[cid.Cid]struct{}
	mapLock     sync.Mutex

	getRequestDelay time.Duration
}

var _ ipld.DAGService = (*countGetsDS)(nil)

func newCountGetsDS(ds ipld.DAGService) *countGetsDS {
	return &countGetsDS{
		ds,
		make(map[cid.Cid]struct{}),
		sync.Mutex{},
		0,
	}
}

func (d *countGetsDS) resetCounter() {
	d.mapLock.Lock()
	defer d.mapLock.Unlock()
	d.cidsFetched = make(map[cid.Cid]struct{})
}

func (d *countGetsDS) uniqueCidsFetched() int {
	d.mapLock.Lock()
	defer d.mapLock.Unlock()
	return len(d.cidsFetched)
}

func (d *countGetsDS) setRequestDelay(timeout time.Duration) {
	d.getRequestDelay = timeout
}

func (d *countGetsDS) Get(ctx context.Context, c cid.Cid) (ipld.Node, error) {
	node, err := d.DAGService.Get(ctx, c)
	if err != nil {
		return nil, err
	}

	d.mapLock.Lock()
	_, cidRequestedBefore := d.cidsFetched[c]
	d.cidsFetched[c] = struct{}{}
	d.mapLock.Unlock()

	if d.getRequestDelay != 0 && !cidRequestedBefore {
		// First request gets a timeout to simulate a network fetch.
		// Subsequent requests get no timeout simulating an in-disk cache.
		time.Sleep(d.getRequestDelay)
	}

	return node, nil
}

// Process sequentially (blocking) calling Get which tracks requests.
func (d *countGetsDS) GetMany(ctx context.Context, cids []cid.Cid) <-chan *ipld.NodeOption {
	panic("GetMany not supported")
}
