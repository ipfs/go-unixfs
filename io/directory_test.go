package io

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"
	"testing"

	cid "github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
	mdag "github.com/ipfs/go-merkledag"
	mdtest "github.com/ipfs/go-merkledag/test"

	ft "github.com/ipfs/go-unixfs"

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

// FIXME: Nothing blocking but nice to have:
//  * Check estimated size against link enumeration (indirectly done in the
//    restored node check from NewDirectoryFromNode).
//  * Check estimated size against encoded node (the difference should only be
//    a small percentage for a directory with 10s of entries).
// FIXME: Add a test for the HAMT sizeChange abstracting some of the code from
//  this one.
func TestBasicDirectory_estimatedSize(t *testing.T) {
	ds := mdtest.Mock()
	ctx := context.Background()
	child := ft.EmptyFileNode()
	err := ds.Add(ctx, child)
	if err != nil {
		t.Fatal(err)
	}

	basicDir := newEmptyBasicDirectory(ds)

	// Several overwrites should not corrupt the size estimation.
	basicDir.AddChild(ctx, "child", child)
	basicDir.AddChild(ctx, "child", child)
	basicDir.AddChild(ctx, "child", child)
	basicDir.RemoveChild(ctx, "child")
	basicDir.AddChild(ctx, "child", child)
	basicDir.RemoveChild(ctx, "child")
	// FIXME: Check errors above (abstract adds/removals in iteration).
	if basicDir.estimatedSize != 0 {
		t.Fatal("estimated size is not zero after removing all entries")
	}

	for i := 0; i < 100; i++ {
		basicDir.AddChild(ctx, fmt.Sprintf("child-%03d", i), child) // e.g., "child-045"
	}
	// Estimated entry size: name (9) + CID (32 from hash and 2 extra for header)
	entrySize := 9 + 32 + 2
	expectedSize := 100 * entrySize
	if basicDir.estimatedSize != expectedSize {
		t.Fatalf("estimated size (%d) inaccurate after adding many entries (expected %d)",
			basicDir.estimatedSize, expectedSize)
	}

	basicDir.RemoveChild(ctx, "child-045") // just random values
	basicDir.RemoveChild(ctx, "child-063")
	basicDir.RemoveChild(ctx, "child-011")
	basicDir.RemoveChild(ctx, "child-000")
	basicDir.RemoveChild(ctx, "child-099")

	basicDir.RemoveChild(ctx, "child-045")        // already removed, won't impact size
	basicDir.RemoveChild(ctx, "nonexistent-name") // also doesn't count
	basicDir.RemoveChild(ctx, "child-100")        // same
	expectedSize -= 5 * entrySize
	if basicDir.estimatedSize != expectedSize {
		t.Fatalf("estimated size (%d) inaccurate after removing some entries (expected %d)",
			basicDir.estimatedSize, expectedSize)
	}

	// Restore a directory from original's node and check estimated size consistency.
	basicDirSingleNode, _ := basicDir.GetNode() // no possible error
	restoredBasicDir := newBasicDirectoryFromNode(ds, basicDirSingleNode.(*mdag.ProtoNode))
	if basicDir.estimatedSize != restoredBasicDir.estimatedSize {
		t.Fatalf("restored basic directory size (%d) doesn't match original estimate (%d)",
			basicDir.estimatedSize, restoredBasicDir.estimatedSize)
	}
}
// FIXME: Add a similar one for HAMT directory, stressing particularly the
//  deleted/overwritten entries and their computation in the size variation.

func mockLinkSizeFunc(fixedSize int) func(linkName string, linkCid cid.Cid) int {
	return func(_ string, _ cid.Cid) int {
		return fixedSize
	}
}

// Basic test on extreme threshold to trigger switch. More fine-grained sizes
// are checked in TestBasicDirectory_estimatedSize (without the swtich itself
// but focusing on the size computation).
// FIXME: Ideally, instead of checking size computation on one test and directory
//  upgrade on another a better structured test should test both dimensions
//  simultaneously.
func TestUpgradeableDirectory(t *testing.T) {
	// FIXME: Modifying these static configuraitons is probably not
	//  concurrent-friendly.
	oldHamtOption := HAMTShardingSize
	defer func() { HAMTShardingSize = oldHamtOption }()
	estimatedLinkSize = mockLinkSizeFunc(1)
	defer func() { estimatedLinkSize = productionLinkSize }()

	ds := mdtest.Mock()
	dir := NewDirectory(ds)
	ctx := context.Background()
	child := ft.EmptyDirNode()
	err := ds.Add(ctx, child)
	if err != nil {
		t.Fatal(err)
	}

	HAMTShardingSize = 0 // Create a BasicDirectory.
	if _, ok := dir.(*UpgradeableDirectory).Directory.(*BasicDirectory); !ok {
		t.Fatal("UpgradeableDirectory doesn't contain BasicDirectory")
	}

	// Set a threshold so big a new entry won't trigger the change.
	HAMTShardingSize = math.MaxInt32

	err = dir.AddChild(ctx, "test", child)
	if err != nil {
		t.Fatal(err)
	}

	if _, ok := dir.(*UpgradeableDirectory).Directory.(*HAMTDirectory); ok {
		t.Fatal("UpgradeableDirectory was upgraded to HAMTDirectory for a large threshold")
	}

	// Now set it so low to make sure any new entry will trigger the upgrade.
	HAMTShardingSize = 1

	err = dir.AddChild(ctx, "test", child) // overwriting an entry should also trigger the switch
	if err != nil {
		t.Fatal(err)
	}

	if _, ok := dir.(*UpgradeableDirectory).Directory.(*HAMTDirectory); !ok {
		t.Fatal("UpgradeableDirectory wasn't upgraded to HAMTDirectory for a low threshold")
	}
	upgradedDir := copyDir(t, dir)

	// Remove the single entry triggering the switch back to BasicDirectory
	err = dir.RemoveChild(ctx, "test")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := dir.(*UpgradeableDirectory).Directory.(*BasicDirectory); !ok {
		t.Fatal("UpgradeableDirectory wasn't downgraded to BasicDirectory after removal of the single entry")
	}

	// Check integrity between switches.
	// We need to account for the removed entry that triggered the switch
	// back.
	// FIXME: Abstract this for arbitrary entries.
	missingLink, err := ipld.MakeLink(child)
	assert.NoError(t, err)
	missingLink.Name = "test"
	compareDirectoryEntries(t, upgradedDir, dir, []*ipld.Link{missingLink})
}

// Compare entries in the leftDir against the rightDir and possibly
// missingEntries in the second.
func compareDirectoryEntries(t *testing.T, leftDir Directory, rightDir Directory, missingEntries []*ipld.Link) {
	leftLinks, err := getAllLinksSortedByName(leftDir)
	assert.NoError(t, err)
	rightLinks, err := getAllLinksSortedByName(rightDir)
	assert.NoError(t, err)
	rightLinks = append(rightLinks, missingEntries...)
	sortLinksByName(rightLinks)

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

func copyDir(t *testing.T, d Directory) Directory {
	dirNode, err := d.GetNode()
	assert.NoError(t, err)
	// Extract the DAG service from the directory (i.e., its link entries saved
	// in it). This is not exposed in the interface and we won't change that now.
	// FIXME: Still, this isn't nice.
	var ds ipld.DAGService
	switch v := d.(type) {
	case *BasicDirectory:
		ds = v.dserv
	case *HAMTDirectory:
		ds = v.dserv
	case *UpgradeableDirectory:
		ds = v.getDagService()
	default:
		panic("unknown directory type")
	}
	copiedDir, err := NewDirectoryFromNode(ds, dirNode)
	assert.NoError(t, err)
	return copiedDir
}

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
