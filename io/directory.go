package io

import (
	"context"
	"fmt"
	"os"
	"time"

	mdag "github.com/ipfs/go-merkledag"

	format "github.com/ipfs/go-unixfs"
	"github.com/ipfs/go-unixfs/hamt"

	"github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
	logging "github.com/ipfs/go-log"
)

var log = logging.Logger("unixfs")

// HAMTShardingSize is a global option that allows switching to a HAMTDirectory
// when the BasicDirectory grows above the size (in bytes) signalled by this
// flag. The default size of 0 disables the option.
// The size is not the *exact* block size of the encoded BasicDirectory but just
// the estimated size based byte length of links name and CID (BasicDirectory's
// ProtoNode doesn't use the Data field so this estimate is pretty accurate).
var HAMTShardingSize = 0

// Time in seconds allowed to fetch the shards to compute the size before
// returning an error.
var EvaluateHAMTTransitionTimeout = time.Duration(1)

// DefaultShardWidth is the default value used for hamt sharding width.
var DefaultShardWidth = 256

// Directory defines a UnixFS directory. It is used for creating, reading and
// editing directories. It allows to work with different directory schemes,
// like the basic or the HAMT implementation.
//
// It just allows to perform explicit edits on a single directory, working with
// directory trees is out of its scope, they are managed by the MFS layer
// (which is the main consumer of this interface).
type Directory interface {

	// SetCidBuilder sets the CID Builder of the root node.
	SetCidBuilder(cid.Builder)

	// AddChild adds a (name, key) pair to the root node.
	AddChild(context.Context, string, ipld.Node) error

	// ForEachLink applies the given function to Links in the directory.
	ForEachLink(context.Context, func(*ipld.Link) error) error

	// EnumLinksAsync returns a channel which will receive Links in the directory
	// as they are enumerated, where order is not gauranteed
	EnumLinksAsync(context.Context) <-chan format.LinkResult

	// Links returns the all the links in the directory node.
	Links(context.Context) ([]*ipld.Link, error)

	// Find returns the root node of the file named 'name' within this directory.
	// In the case of HAMT-directories, it will traverse the tree.
	//
	// Returns os.ErrNotExist if the child does not exist.
	Find(context.Context, string) (ipld.Node, error)

	// RemoveChild removes the child with the given name.
	//
	// Returns os.ErrNotExist if the child doesn't exist.
	RemoveChild(context.Context, string) error

	// GetNode returns the root of this directory.
	GetNode() (ipld.Node, error)

	// GetCidBuilder returns the CID Builder used.
	GetCidBuilder() cid.Builder
}

// TODO: Evaluate removing `dserv` from this layer and providing it in MFS.
// (The functions should in that case add a `DAGService` argument.)

// BasicDirectory is the basic implementation of `Directory`. All the entries
// are stored in a single node.
type BasicDirectory struct {
	node  *mdag.ProtoNode
	dserv ipld.DAGService

	// Internal variable used to cache the estimated size of the basic directory:
	// for each link, aggregate link name + link CID. DO NOT CHANGE THIS
	// as it will affect the HAMT transition behavior in HAMTShardingSize.
	// (We maintain this value up to date even if the HAMTShardingSize is off
	// since potentially the option could be activated on the fly.)
	estimatedSize int
}

// HAMTDirectory is the HAMT implementation of `Directory`.
// (See package `hamt` for more information.)
type HAMTDirectory struct {
	shard *hamt.Shard
	dserv ipld.DAGService

	// Track the changes in size by the AddChild and RemoveChild calls
	// for the HAMTShardingSize option.
	sizeChange int
}

func newEmptyBasicDirectory(dserv ipld.DAGService) *BasicDirectory {
	return newBasicDirectoryFromNode(dserv, format.EmptyDirNode())
}

func newBasicDirectoryFromNode(dserv ipld.DAGService, node *mdag.ProtoNode) *BasicDirectory {
	basicDir := new(BasicDirectory)
	basicDir.node = node
	basicDir.dserv = dserv

	// Scan node links (if any) to restore estimated size.
	basicDir.computeEstimatedSize()

	return basicDir
}

// NewDirectory returns a Directory implemented by UpgradeableDirectory
// containing a BasicDirectory that can be converted to a HAMTDirectory.
func NewDirectory(dserv ipld.DAGService) Directory {
	return &UpgradeableDirectory{newEmptyBasicDirectory(dserv)}
}

// ErrNotADir implies that the given node was not a unixfs directory
var ErrNotADir = fmt.Errorf("merkledag node was not a directory or shard")

// NewDirectoryFromNode loads a unixfs directory from the given IPLD node and
// DAGService.
func NewDirectoryFromNode(dserv ipld.DAGService, node ipld.Node) (Directory, error) {
	protoBufNode, ok := node.(*mdag.ProtoNode)
	if !ok {
		return nil, ErrNotADir
	}

	fsNode, err := format.FSNodeFromBytes(protoBufNode.Data())
	if err != nil {
		return nil, err
	}

	switch fsNode.Type() {
	case format.TDirectory:
		return &UpgradeableDirectory{newBasicDirectoryFromNode(dserv, protoBufNode.Copy().(*mdag.ProtoNode))}, nil
	case format.THAMTShard:
		shard, err := hamt.NewHamtFromDag(dserv, node)
		if err != nil {
			return nil, err
		}
		return &HAMTDirectory{
			dserv: dserv,
			shard: shard,
		}, nil
	}

	return nil, ErrNotADir
}

func (d *BasicDirectory) computeEstimatedSize() {
	d.estimatedSize = 0
	d.ForEachLink(context.TODO(), func(l *ipld.Link) error {
		d.addToEstimatedSize(l.Name, l.Cid)
		return nil
	})
}

func estimatedLinkSize(linkName string, linkCid cid.Cid) int {
	return len(linkName) + linkCid.ByteLen()
}

func (d *BasicDirectory) addToEstimatedSize(name string, linkCid cid.Cid) {
	d.estimatedSize += estimatedLinkSize(name, linkCid)
}

func (d *BasicDirectory) removeFromEstimatedSize(name string, linkCid cid.Cid) {
	d.estimatedSize -= estimatedLinkSize(name, linkCid)
	if d.estimatedSize < 0 {
		// Something has gone very wrong. Log an error and recompute the
		// size from scratch.
		log.Error("BasicDirectory's estimatedSize went below 0")
		d.computeEstimatedSize()
	}
}

// SetCidBuilder implements the `Directory` interface.
func (d *BasicDirectory) SetCidBuilder(builder cid.Builder) {
	d.node.SetCidBuilder(builder)
}

// AddChild implements the `Directory` interface. It adds (or replaces)
// a link to the given `node` under `name`.
func (d *BasicDirectory) AddChild(ctx context.Context, name string, node ipld.Node) error {
	// Remove old link (if it existed; ignore `ErrNotExist` otherwise).
	err := d.RemoveChild(ctx, name)
	if err != nil && err != os.ErrNotExist {
		return err
	}

	err = d.node.AddNodeLink(name, node)
	if err != nil {
		return err
	}
	d.addToEstimatedSize(name, node.Cid())
	return nil
}

// EnumLinksAsync returns a channel which will receive Links in the directory
// as they are enumerated, where order is not gauranteed
func (d *BasicDirectory) EnumLinksAsync(ctx context.Context) <-chan format.LinkResult {
	linkResults := make(chan format.LinkResult)
	go func() {
		defer close(linkResults)
		for _, l := range d.node.Links() {
			select {
			case linkResults <- format.LinkResult{
				Link: l,
				Err:  nil,
			}:
			case <-ctx.Done():
				return
			}
		}
	}()
	return linkResults
}

// ForEachLink implements the `Directory` interface.
func (d *BasicDirectory) ForEachLink(ctx context.Context, f func(*ipld.Link) error) error {
	for _, l := range d.node.Links() {
		if err := f(l); err != nil {
			return err
		}
	}
	return nil
}

// Links implements the `Directory` interface.
func (d *BasicDirectory) Links(ctx context.Context) ([]*ipld.Link, error) {
	return d.node.Links(), nil
}

// Find implements the `Directory` interface.
func (d *BasicDirectory) Find(ctx context.Context, name string) (ipld.Node, error) {
	lnk, err := d.node.GetNodeLink(name)
	if err == mdag.ErrLinkNotFound {
		err = os.ErrNotExist
	}
	if err != nil {
		return nil, err
	}

	return d.dserv.Get(ctx, lnk.Cid)
}

// RemoveChild implements the `Directory` interface.
func (d *BasicDirectory) RemoveChild(ctx context.Context, name string) error {
	// We need to *retrieve* the link before removing it to update the estimated
	// size. This means we may iterate the links slice twice: if traversing this
	// becomes a problem, a factor of 2 isn't going to make much of a difference.
	// We'd likely need to cache a link resolution map in that case.
	link, err := d.node.GetNodeLink(name)
	if err == mdag.ErrLinkNotFound {
		return os.ErrNotExist
	}
	if err != nil {
		return err // at the moment there is no other error besides ErrLinkNotFound
	}

	// The name actually existed so we should update the estimated size.
	d.removeFromEstimatedSize(link.Name, link.Cid)

	return d.node.RemoveNodeLink(name)
	// GetNodeLink didn't return ErrLinkNotFound so this won't fail with that
	// and we don't need to convert the error again.
}

// GetNode implements the `Directory` interface.
func (d *BasicDirectory) GetNode() (ipld.Node, error) {
	return d.node, nil
}

// GetCidBuilder implements the `Directory` interface.
func (d *BasicDirectory) GetCidBuilder() cid.Builder {
	return d.node.CidBuilder()
}

// SwitchToSharding returns a HAMT implementation of this directory.
func (d *BasicDirectory) SwitchToSharding(ctx context.Context) (Directory, error) {
	hamtDir := new(HAMTDirectory)
	hamtDir.dserv = d.dserv

	shard, err := hamt.NewShard(d.dserv, DefaultShardWidth)
	if err != nil {
		return nil, err
	}
	shard.SetCidBuilder(d.node.CidBuilder())
	hamtDir.shard = shard

	for _, lnk := range d.node.Links() {
		node, err := d.dserv.Get(ctx, lnk.Cid)
		if err != nil {
			return nil, err
		}

		err = hamtDir.shard.Set(ctx, lnk.Name, node)
		if err != nil {
			return nil, err
		}
	}

	return hamtDir, nil
}

// SetCidBuilder implements the `Directory` interface.
func (d *HAMTDirectory) SetCidBuilder(builder cid.Builder) {
	d.shard.SetCidBuilder(builder)
}

// AddChild implements the `Directory` interface.
func (d *HAMTDirectory) AddChild(ctx context.Context, name string, nd ipld.Node) error {
	// FIXME: This needs to be moved to Shard internals to make sure we are
	//  actually adding a new entry (increasing size) or just replacing an
	//  old one (do nothing, or get a difference in entry size).
	d.addToSizeChange(name, nd.Cid())

	return d.shard.Set(ctx, name, nd)
}

// ForEachLink implements the `Directory` interface.
func (d *HAMTDirectory) ForEachLink(ctx context.Context, f func(*ipld.Link) error) error {
	return d.shard.ForEachLink(ctx, f)
}

// EnumLinksAsync returns a channel which will receive Links in the directory
// as they are enumerated, where order is not gauranteed
func (d *HAMTDirectory) EnumLinksAsync(ctx context.Context) <-chan format.LinkResult {
	return d.shard.EnumLinksAsync(ctx)
}

// Links implements the `Directory` interface.
func (d *HAMTDirectory) Links(ctx context.Context) ([]*ipld.Link, error) {
	return d.shard.EnumLinks(ctx)
}

// Find implements the `Directory` interface. It will traverse the tree.
func (d *HAMTDirectory) Find(ctx context.Context, name string) (ipld.Node, error) {
	lnk, err := d.shard.Find(ctx, name)
	if err != nil {
		return nil, err
	}

	return lnk.GetNode(ctx, d.dserv)
}

// RemoveChild implements the `Directory` interface.
func (d *HAMTDirectory) RemoveChild(ctx context.Context, name string) error {
	// FIXME: Same note as in AddChild, with the added consideration that
	//  we need to retrieve the entry before removing to adjust size.
	d.removeFromSizeChange(name, cid.Undef)

	return d.shard.Remove(ctx, name)
}

// GetNode implements the `Directory` interface.
func (d *HAMTDirectory) GetNode() (ipld.Node, error) {
	return d.shard.Node()
}

// GetCidBuilder implements the `Directory` interface.
func (d *HAMTDirectory) GetCidBuilder() cid.Builder {
	return d.shard.CidBuilder()
}

// switchToBasic returns a BasicDirectory implementation of this directory.
func (d *HAMTDirectory) switchToBasic(ctx context.Context) (*BasicDirectory, error) {
	basicDir := newEmptyBasicDirectory(d.dserv)
	basicDir.SetCidBuilder(d.GetCidBuilder())

	d.ForEachLink(nil, func(lnk *ipld.Link) error {
		node, err := d.dserv.Get(ctx, lnk.Cid)
		if err != nil {
			return err
		}

		err = basicDir.AddChild(ctx, lnk.Name, node)
		if err != nil {
			return err
		}

		return nil
	})
	// FIXME: The above was adapted from SwitchToSharding. We can probably optimize:
	//  1. Do not retrieve the full node but create the link from the
	//     HAMT entry and add it to the BasicDirectory with a (new)
	//     plumbing function that operates below the AddChild level.
	//  2. Do not enumerate all link from scratch (harder than previous
	//     item). We call this function only from the UpgradeableDirectory
	//     when we went below the threshold: the detection will be done through
	//     (partial) enumeration. We may be able to reuse some of that work
	//     (likely retaining the links in a cache). (All this is uncertain
	//     at this point as we don't know how partial enumeration will be
	//     implemented.)

	return basicDir, nil
}

func (d *HAMTDirectory) addToSizeChange(name string, linkCid cid.Cid) {
	d.sizeChange += estimatedLinkSize(name, linkCid)
}

func (d *HAMTDirectory) removeFromSizeChange(name string, linkCid cid.Cid) {
	d.sizeChange -= estimatedLinkSize(name, linkCid)
}

// Evaluate directory size and check if it's below HAMTShardingSize threshold
// (to trigger a transition to a BasicDirectory). It returns two `bool`s:
// * whether it's below (true) or equal/above (false)
// * whether the passed timeout to compute the size has been exceeded
// Instead of enumearting the entire tree we eagerly call EnumLinksAsync
// until we either reach a value above the threshold (in that case no need)
// to keep counting or the timeout runs out in which case the `below` return
// value is not to be trusted as we didn't have time to count enough shards.
func (d *HAMTDirectory) sizeBelowThreshold(timeout time.Duration) (below bool, timeoutExceeded bool) {
	if HAMTShardingSize == 0 {
		panic("asked to compute HAMT size with HAMTShardingSize option off (0)")
	}

	// We don't necessarily compute the full size of *all* shards as we might
	// end early if we already know we're above the threshold or run out of time.
	partialSize := 0

	ctx, cancel := context.WithTimeout(context.Background(), time.Second * timeout)
	for linkResult := range d.EnumLinksAsync(ctx) {
		if linkResult.Err != nil {
			continue
			// The timeout exceeded errors will be coming through here but I'm
			// not sure if we can just compare against a generic DeadlineExceeded
			// error here to return early and avoid iterating the entire loop.
			// (We might confuse a specific DeadlineExceeded of an internal function
			//  with our context here.)
			// Since *our* DeadlineExceeded will quickly propagate to any other
			// pending fetches it seems that iterating the entire loop won't add
			// much more cost anyway.
			// FIXME: Check the above reasoning.
		}
		if linkResult.Link == nil {
			panic("empty link result (both values nil)")
			// FIXME: Is this *ever* possible?
		}
		partialSize += estimatedLinkSize(linkResult.Link.Name, linkResult.Link.Cid)

		if partialSize >= HAMTShardingSize {
			// We have already fetched enough shards to assert we are
			//  above the threshold, so no need to keep fetching.
			cancel()
			return false, false
		}
	}
	// At this point either we enumerated all shards or run out of time.
	// Figure out which.

	if ctx.Err() == context.Canceled {
		panic("the context was canceled but we're still evaluating a possible switch")
	}
	if partialSize >= HAMTShardingSize {
		panic("we reach the threshold but we're still evaluating a possible switch")
	}

	if ctx.Err() == context.DeadlineExceeded {
		return false, true
	}

	// If we reach this then:
	// * We are below the threshold (we didn't return inside the EnumLinksAsync
	//   loop).
	// * The context wasn't cancelled so we iterated *all* shards
	//   and are sure that we have the full size.
	// FIXME: Can we actually verify the last claim here to be sure?
	//  (Iterating all the shards in the HAMT as a plumbing function maybe.
	//   If they're in memory it shouldn't be that expensive, we won't be
	//   switching that often, probably.)
	return true, false
}

// UpgradeableDirectory wraps a Directory interface and provides extra logic
// to upgrade from its BasicDirectory implementation to HAMTDirectory.
// FIXME: Rename to something that reflects the new bi-directionality. We no
//  longer go in the "forward" direction (upgrade) but we also go "backward".
//  Possible alternatives: SwitchableDirectory or DynamicDirectory. Also consider
//  more generic-sounding names like WrapperDirectory that emphasize that this
//  is just the middleman and has no real Directory-implementing logic.
type UpgradeableDirectory struct {
	Directory
}

var _ Directory = (*UpgradeableDirectory)(nil)

// AddChild implements the `Directory` interface. We check when adding new entries
// if we should switch to HAMTDirectory according to global option(s).
func (d *UpgradeableDirectory) AddChild(ctx context.Context, name string, nd ipld.Node) error {
	err := d.Directory.AddChild(ctx, name, nd)
	if err != nil {
		return err
	}

	// Evaluate possible HAMT upgrade.
	if HAMTShardingSize == 0 {
		return nil
	}
	basicDir, ok := d.Directory.(*BasicDirectory)
	if !ok {
		return nil
	}
	if basicDir.estimatedSize >= HAMTShardingSize {
		// Ideally to minimize performance we should check if this last
		// `AddChild` call would bring the directory size over the threshold
		// *before* executing it since we would end up switching anyway and
		// that call would be "wasted". This is a minimal performance impact
		// and we prioritize a simple code base.
		hamtDir, err := basicDir.SwitchToSharding(ctx)
		if err != nil {
			return err
		}
		d.Directory = hamtDir
	}

	return nil
}

// FIXME: Consider implementing RemoveChild to do an eager enumeration if
//  the HAMT sizeChange goes below a certain point (normally lower than just
//  zero to not enumerate in *any* occasion the size delta goes transiently
//  negative).
//func (d *UpgradeableDirectory) RemoveChild(ctx context.Context, name string) error

// GetNode implements the `Directory` interface. Used in the case where we wrap
// a HAMTDirectory that might need to be downgraded to a BasicDirectory. The
// upgrade path is in AddChild; we delay the downgrade until we are forced to
// commit to a CID (through the returned node) to avoid costly enumeration in
// the sharding case (that by definition will have potentially many more entries
// than the BasicDirectory).
// FIXME: We need to be *very* sure that the returned downgraded BasicDirectory
//  will be in fact *above* the HAMTShardingSize threshold to avoid churning.
//  We may even need to use a basic low/high water markings (like a small
//  percentage above and below the original user-set HAMTShardingSize).
func (d *UpgradeableDirectory) GetNode() (ipld.Node, error) {
	hamtDir, ok := d.Directory.(*HAMTDirectory)
	if !ok {
		return d.Directory.GetNode() // BasicDirectory
	}

	if HAMTShardingSize == 0 && // Option disabled.
		hamtDir.sizeChange < 0 { // We haven't reduced the HAMT size.
		return d.Directory.GetNode()
	}

	// We have reduced the directory size, check if it didn't go under
	// the HAMTShardingSize threshold.

	belowThreshold, timeoutExceeded := hamtDir.sizeBelowThreshold(EvaluateHAMTTransitionTimeout)

	if timeoutExceeded {
		// We run out of time before confirming if we're indeed below the
		// threshold. When in doubt error to not return inconsistent structures.
		return nil, fmt.Errorf("not enought time to fetch shards")
		// FIXME: Abstract in new error for testing.
	}

	if belowThreshold {
		// Switch.
		basicDir, err := hamtDir.switchToBasic(context.Background())
		// FIXME: The missing context will be provided once we move this to
		//  AddChild and remove child.
		if err != nil {
			return nil, err
		}
		d.Directory = basicDir
	}

	return d.Directory.GetNode()
}
