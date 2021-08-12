// Package hamt implements a Hash Array Mapped Trie over ipfs merkledag nodes.
// It is implemented mostly as described in the wikipedia article on HAMTs,
// however the table size is variable (usually 256 in our usages) as opposed to
// 32 as suggested in the article.  The hash function used is currently
// Murmur3, but this value is configurable (the datastructure reports which
// hash function its using).
//
// The one algorithmic change we implement that is not mentioned in the
// wikipedia article is the collapsing of empty shards.
// Given the following tree: ( '[' = shards, '{' = values )
// [ 'A' ] -> [ 'B' ] -> { "ABC" }
//    |       L-> { "ABD" }
//    L-> { "ASDF" }
// If we simply removed "ABC", we would end up with a tree where shard 'B' only
// has a single child.  This causes two issues, the first, is that now we have
// an extra lookup required to get to "ABD".  The second issue is that now we
// have a tree that contains only "ABD", but is not the same tree that we would
// get by simply inserting "ABD" into a new tree.  To address this, we always
// check for empty shard nodes upon deletion and prune them to maintain a
// consistent tree, independent of insertion order.
package hamt

import (
	"context"
	"fmt"
	"os"

	bitfield "github.com/ipfs/go-bitfield"
	cid "github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
	dag "github.com/ipfs/go-merkledag"
	format "github.com/ipfs/go-unixfs"
)

const (
	// HashMurmur3 is the multiformats identifier for Murmur3
	HashMurmur3 uint64 = 0x22
)

func (ds *Shard) isValueNode() bool {
	return ds.key != "" && ds.val != nil
}

// A Shard represents the HAMT. It should be initialized with NewShard().
type Shard struct {
	childer *childer

	tableSize    int
	tableSizeLg2 int

	builder  cid.Builder
	hashFunc uint64

	prefixPadStr string
	maxpadlen    int

	dserv ipld.DAGService

	// leaf node
	key string
	val *ipld.Link
}

// NewShard creates a new, empty HAMT shard with the given size.
func NewShard(dserv ipld.DAGService, size int) (*Shard, error) {
	ds, err := makeShard(dserv, size)
	if err != nil {
		return nil, err
	}

	ds.hashFunc = HashMurmur3
	return ds, nil
}

func makeShard(ds ipld.DAGService, size int) (*Shard, error) {
	lg2s, err := logtwo(size)
	if err != nil {
		return nil, err
	}
	maxpadding := fmt.Sprintf("%X", size-1)
	s := &Shard{
		tableSizeLg2: lg2s,
		prefixPadStr: fmt.Sprintf("%%0%dX", len(maxpadding)),
		maxpadlen:    len(maxpadding),
		childer:      newChilder(ds, size),
		tableSize:    size,
		dserv:        ds,
	}

	s.childer.sd = s

	return s, nil
}

// NewHamtFromDag creates new a HAMT shard from the given DAG.
func NewHamtFromDag(dserv ipld.DAGService, nd ipld.Node) (*Shard, error) {
	pbnd, ok := nd.(*dag.ProtoNode)
	if !ok {
		return nil, dag.ErrNotProtobuf
	}

	fsn, err := format.FSNodeFromBytes(pbnd.Data())
	if err != nil {
		return nil, err
	}

	if fsn.Type() != format.THAMTShard {
		return nil, fmt.Errorf("node was not a dir shard")
	}

	if fsn.HashType() != HashMurmur3 {
		return nil, fmt.Errorf("only murmur3 supported as hash function")
	}

	size := int(fsn.Fanout())

	ds, err := makeShard(dserv, size)
	if err != nil {
		return nil, err
	}

	ds.childer.makeChilder(fsn.Data(), pbnd.Links())

	ds.hashFunc = fsn.HashType()
	ds.builder = pbnd.CidBuilder()

	return ds, nil
}

// SetCidBuilder sets the CID Builder
func (ds *Shard) SetCidBuilder(builder cid.Builder) {
	ds.builder = builder
}

// CidBuilder gets the CID Builder, may be nil if unset
func (ds *Shard) CidBuilder() cid.Builder {
	return ds.builder
}

// Node serializes the HAMT structure into a merkledag node with unixfs formatting
func (ds *Shard) Node() (ipld.Node, error) {
	out := new(dag.ProtoNode)
	out.SetCidBuilder(ds.builder)

	sliceIndex := 0
	// TODO: optimized 'for each set bit'
	for childIndex := 0; childIndex < ds.tableSize; childIndex++ {
		if !ds.childer.has(childIndex) {
			continue
		}

		ch := ds.childer.child(sliceIndex)
		if ch != nil {
			clnk, err := ch.Link()
			if err != nil {
				return nil, err
			}

			err = out.AddRawLink(ds.linkNamePrefix(childIndex)+ch.key, clnk)
			if err != nil {
				return nil, err
			}
		} else {
			// child unloaded, just copy in link with updated name
			lnk := ds.childer.link(sliceIndex)
			label := lnk.Name[ds.maxpadlen:]

			err := out.AddRawLink(ds.linkNamePrefix(childIndex)+label, lnk)
			if err != nil {
				return nil, err
			}
		}
		sliceIndex++
	}

	data, err := format.HAMTShardData(ds.childer.bitfield.Bytes(), uint64(ds.tableSize), HashMurmur3)
	if err != nil {
		return nil, err
	}

	out.SetData(data)

	err = ds.dserv.Add(context.TODO(), out)
	if err != nil {
		return nil, err
	}

	return out, nil
}

func (ds *Shard) makeShardValue(lnk *ipld.Link) (*Shard, error) {
	lnk2 := *lnk
	s, err := makeShard(ds.dserv, ds.tableSize)
	if err != nil {
		return nil, err
	}

	s.key = lnk.Name[ds.maxpadlen:]
	s.val = &lnk2

	return s, nil
}

// Set sets 'name' = nd in the HAMT
func (ds *Shard) Set(ctx context.Context, name string, nd ipld.Node) error {
	_, err := ds.SetAndPrevious(ctx, name, nd)
	return err
}

// SetAndPrevious sets a link pointing to the passed node as the value under the
// name key in this Shard or its children. It also returns the previous link
// under that name key (if any).
func (ds *Shard) SetAndPrevious(ctx context.Context, name string, node ipld.Node) (*ipld.Link, error) {
	hv := &hashBits{b: hash([]byte(name))}
	err := ds.dserv.Add(ctx, node)
	if err != nil {
		return nil, err
	}

	lnk, err := ipld.MakeLink(node)
	if err != nil {
		return nil, err
	}
	lnk.Name = ds.linkNamePrefix(0) + name

	return ds.setValue(ctx, hv, name, lnk)
}

// Remove deletes the named entry if it exists. Otherwise, it returns
// os.ErrNotExist.
func (ds *Shard) Remove(ctx context.Context, name string) error {
	_, err := ds.RemoveAndPrevious(ctx, name)
	return err
}

// RemoveAndPrevious is similar to the public Remove but also returns the
// old removed link (if it exists).
func (ds *Shard) RemoveAndPrevious(ctx context.Context, name string) (*ipld.Link, error) {
	hv := &hashBits{b: hash([]byte(name))}
	return ds.setValue(ctx, hv, name, nil)
}

// Find searches for a child node by 'name' within this hamt
func (ds *Shard) Find(ctx context.Context, name string) (*ipld.Link, error) {
	hv := &hashBits{b: hash([]byte(name))}

	var out *ipld.Link
	err := ds.getValue(ctx, hv, name, func(sv *Shard) error {
		out = sv.val
		return nil
	})
	if err != nil {
		return nil, err
	}

	return out, nil
}

type linkType int

const (
	invalidLink linkType = iota
	shardLink
	shardValueLink
)

func (ds *Shard) childLinkType(lnk *ipld.Link) (linkType, error) {
	if len(lnk.Name) < ds.maxpadlen {
		return invalidLink, fmt.Errorf("invalid link name '%s'", lnk.Name)
	}
	if len(lnk.Name) == ds.maxpadlen {
		return shardLink, nil
	}
	return shardValueLink, nil
}

// Link returns a merklelink to this shard node
func (ds *Shard) Link() (*ipld.Link, error) {
	if ds.isValueNode() {
		return ds.val, nil
	}

	nd, err := ds.Node()
	if err != nil {
		return nil, err
	}

	err = ds.dserv.Add(context.TODO(), nd)
	if err != nil {
		return nil, err
	}

	return ipld.MakeLink(nd)
}

func (ds *Shard) getValue(ctx context.Context, hv *hashBits, key string, cb func(*Shard) error) error {
	childIndex, err := hv.Next(ds.tableSizeLg2)
	if err != nil {
		return err
	}

	if ds.childer.has(childIndex) {
		child, err := ds.childer.get(ctx, ds.childer.sliceIndex(childIndex))
		if err != nil {
			return err
		}

		if child.isValueNode() {
			if child.key == key {
				return cb(child)
			}
		} else {
			return child.getValue(ctx, hv, key, cb)
		}
	}

	return os.ErrNotExist
}

// EnumLinks collects all links in the Shard.
func (ds *Shard) EnumLinks(ctx context.Context) ([]*ipld.Link, error) {
	var links []*ipld.Link

	linkResults := ds.EnumLinksAsync(ctx)

	for linkResult := range linkResults {
		if linkResult.Err != nil {
			return links, linkResult.Err
		}
		links = append(links, linkResult.Link)
	}
	return links, nil
}

// ForEachLink walks the Shard and calls the given function.
func (ds *Shard) ForEachLink(ctx context.Context, f func(*ipld.Link) error) error {
	return ds.walkTrie(ctx, func(sv *Shard) error {
		lnk := sv.val
		lnk.Name = sv.key

		return f(lnk)
	})
}

// EnumLinksAsync returns a channel which will receive Links in the directory
// as they are enumerated, where order is not guaranteed
func (ds *Shard) EnumLinksAsync(ctx context.Context) <-chan format.LinkResult {
	linkResults := make(chan format.LinkResult)
	ctx, cancel := context.WithCancel(ctx)
	go func() {
		defer close(linkResults)
		defer cancel()
		getLinks := makeAsyncTrieGetLinks(ds.dserv, linkResults)
		cset := cid.NewSet()
		rootNode, err := ds.Node()
		if err != nil {
			emitResult(ctx, linkResults, format.LinkResult{Link: nil, Err: err})
			return
		}
		err = dag.Walk(ctx, getLinks, rootNode.Cid(), cset.Visit, dag.Concurrent())
		if err != nil {
			emitResult(ctx, linkResults, format.LinkResult{Link: nil, Err: err})
		}
	}()
	return linkResults
}

// makeAsyncTrieGetLinks builds a getLinks function that can be used with EnumerateChildrenAsync
// to iterate a HAMT shard. It takes an IPLD Dag Service to fetch nodes, and a call back that will get called
// on all links to leaf nodes in a HAMT tree, so they can be collected for an EnumLinks operation
func makeAsyncTrieGetLinks(dagService ipld.DAGService, linkResults chan<- format.LinkResult) dag.GetLinks {

	return func(ctx context.Context, currentCid cid.Cid) ([]*ipld.Link, error) {
		node, err := dagService.Get(ctx, currentCid)
		if err != nil {
			return nil, err
		}
		directoryShard, err := NewHamtFromDag(dagService, node)
		if err != nil {
			return nil, err
		}

		childShards := make([]*ipld.Link, 0, directoryShard.childer.length())
		links := directoryShard.childer.links
		for idx := range directoryShard.childer.children {
			lnk := links[idx]
			lnkLinkType, err := directoryShard.childLinkType(lnk)

			if err != nil {
				return nil, err
			}
			if lnkLinkType == shardLink {
				childShards = append(childShards, lnk)
			} else {
				sv, err := directoryShard.makeShardValue(lnk)
				if err != nil {
					return nil, err
				}
				formattedLink := sv.val
				formattedLink.Name = sv.key
				emitResult(ctx, linkResults, format.LinkResult{Link: formattedLink, Err: nil})
			}
		}
		return childShards, nil
	}
}

func emitResult(ctx context.Context, linkResults chan<- format.LinkResult, r format.LinkResult) {
	// make sure that context cancel is processed first
	// the reason is due to the concurrency of EnumerateChildrenAsync
	// it's possible for EnumLinksAsync to complete and close the linkResults
	// channel before this code runs
	select {
	case <-ctx.Done():
		return
	default:
	}
	select {
	case linkResults <- r:
	case <-ctx.Done():
	}
}

func (ds *Shard) walkTrie(ctx context.Context, cb func(*Shard) error) error {
	return ds.childer.each(ctx, func(s *Shard) error {
		if s.isValueNode() {
			if err := cb(s); err != nil {
				return err
			}
		} else {
			if err := s.walkTrie(ctx, cb); err != nil {
				return err
			}
		}
		return nil
	})
}

// setValue sets the link `value` in the given key, either creating the entry
// if it didn't exist or overwriting the old one. It returns the old entry (if any).
func (ds *Shard) setValue(ctx context.Context, hv *hashBits, key string, value *ipld.Link) (oldValue *ipld.Link, err error) {
	idx, err := hv.Next(ds.tableSizeLg2)
	if err != nil {
		return
	}

	if !ds.childer.has(idx) {
		// Entry does not exist, create a new one.
		return nil, ds.childer.insert(key, value, idx)
	}

	i := ds.childer.sliceIndex(idx)
	child, err := ds.childer.get(ctx, i)
	if err != nil {
		return
	}

	if child.isValueNode() {
		// Leaf node. This is the base case of this recursive function.
		// FIXME: Misleading: the base case also includes a recursive call
		//  in the case both keys share the same slot in the new child shard
		//  below.
		if child.key == key {
			// We are in the correct shard (tree level) so we modify this child
			// and return.
			oldValue = child.val

			if value == nil { // Remove old entry.
				return oldValue, ds.childer.rm(idx)
			}

			child.val = value // Overwrite entry.
			return
		}

		if value == nil {
			return nil, os.ErrNotExist
			// FIXME: Move this case to the top of the clause as
			//  if child.key != key
			//  and remove the indentation on the big if.
		}

		// We are in the same slot with another entry with a different key
		// so we need to fork this leaf node into a shard with two childs:
		// the old entry and the new one being inserted here.
		// We don't overwrite anything here so we keep:
		//   `oldValue = nil`

		// The child of this shard will now be a new shard. The old child value
		// will be a child of this new shard (along with the new value being
		// inserted).
		grandChild := child
		child, err = NewShard(ds.dserv, ds.tableSize)
		if err != nil {
			return nil, err
		}
		child.builder = ds.builder
		chhv := &hashBits{
			b:        hash([]byte(grandChild.key)),
			consumed: hv.consumed,
		}

		// We explicitly ignore the oldValue returned by the next two insertions
		// (which will be nil) to highlight there is no overwrite here: they are
		// done with different keys to a new (empty) shard. (At best this shard
		// will create new ones until we find different slots for both.)
		// FIXME: These two shouldn't be recursive calls where one function
		//  adds the child and the other replaces it with a shard to go one
		//  level deeper. This should just be a simple loop that traverses the
		//  shared prefix in the key adding as many shards as needed and finally
		//  inserts both leaf links together.
		_, err = child.setValue(ctx, hv, key, value)
		if err != nil {
			return
		}
		_, err = child.setValue(ctx, chhv, grandChild.key, grandChild.val)
		if err != nil {
			return
		}

		// Replace this leaf node with the new Shard node.
		ds.childer.set(child, i)
		return nil, nil
	} else {
		// We are in a Shard (internal node). We will recursively call this
		// function until finding the leaf (the logic of the `if` case above).
		oldValue, err = child.setValue(ctx, hv, key, value)
		if err != nil {
			return
		}

		if value == nil {
			// We have removed an entry, check if we should remove shards
			// as well.
			switch child.childer.length() {
			case 0:
				// empty sub-shard, prune it
				// Note: this shouldnt normally ever happen
				//       in the event of another implementation creates flawed
				//       structures, this will help to normalize them.
				return oldValue, ds.childer.rm(idx)
			case 1:
				// The single child _should_ be a value by
				// induction. However, we allow for it to be a
				// shard in case an implementation is broken.

				// Have we loaded the child? Prefer that.
				schild := child.childer.child(0)
				if schild != nil {
					if schild.isValueNode() {
						ds.childer.set(schild, i)
					}
					return
				}

				// Otherwise, work with the link.
				slnk := child.childer.link(0)
				var lnkType linkType
				lnkType, err = child.childer.sd.childLinkType(slnk)
				if err != nil {
					return
				}
				if lnkType == shardValueLink {
					// sub-shard with a single value element, collapse it
					ds.childer.setLink(slnk, i)
				}
				return
			}
		}

		return
	}
}

// linkNamePrefix takes in the bitfield index of an entry and returns its hex prefix
func (ds *Shard) linkNamePrefix(idx int) string {
	return fmt.Sprintf(ds.prefixPadStr, idx)
}

// childer wraps the links, children and bitfield
// and provides basic operation (get, rm, insert and set) of manipulating children.
// The slices `links` and `children` are always coordinated to have the entries
// in the same index. A `childIndex` belonging to one of the original `Shard.size`
// entries corresponds to a `sliceIndex` in `links` and `children` (the conversion
// is done through `bitfield`).
type childer struct {
	sd       *Shard
	dserv    ipld.DAGService
	bitfield bitfield.Bitfield

	// Only one of links/children will be non-nil for every child/link.
	links    []*ipld.Link
	children []*Shard
}

func newChilder(ds ipld.DAGService, size int) *childer {
	return &childer{
		dserv:    ds,
		bitfield: bitfield.NewBitfield(size),
	}
}

func (s *childer) makeChilder(data []byte, links []*ipld.Link) *childer {
	s.children = make([]*Shard, len(links))
	s.bitfield.SetBytes(data)
	if len(links) > 0 {
		s.links = make([]*ipld.Link, len(links))
		copy(s.links, links)
	}

	return s
}

// Return the `sliceIndex` associated with a child.
func (s *childer) sliceIndex(childIndex int) (sliceIndex int) {
	return s.bitfield.OnesBefore(childIndex)
}

func (s *childer) child(sliceIndex int) *Shard {
	return s.children[sliceIndex]
}

func (s *childer) link(sliceIndex int) *ipld.Link {
	return s.links[sliceIndex]
}

func (s *childer) insert(key string, lnk *ipld.Link, idx int) error {
	if lnk == nil {
		return os.ErrNotExist
	}

	lnk.Name = s.sd.linkNamePrefix(idx) + key
	i := s.sliceIndex(idx)
	sd := &Shard{key: key, val: lnk}

	s.children = append(s.children[:i], append([]*Shard{sd}, s.children[i:]...)...)
	s.links = append(s.links[:i], append([]*ipld.Link{nil}, s.links[i:]...)...)
	// Add a `nil` placeholder in `links` so the rest of the entries keep the same
	// index as `children`.
	s.bitfield.SetBit(idx)

	return nil
}

func (s *childer) set(sd *Shard, i int) {
	s.children[i] = sd
	s.links[i] = nil
}

func (s *childer) setLink(lnk *ipld.Link, i int) {
	s.children[i] = nil
	s.links[i] = lnk
}

func (s *childer) rm(childIndex int) error {
	i := s.sliceIndex(childIndex)

	if err := s.check(i); err != nil {
		return err
	}

	copy(s.children[i:], s.children[i+1:])
	s.children = s.children[:len(s.children)-1]

	copy(s.links[i:], s.links[i+1:])
	s.links = s.links[:len(s.links)-1]

	s.bitfield.UnsetBit(childIndex)

	return nil
}

// get returns the i'th child of this shard. If it is cached in the
// children array, it will return it from there. Otherwise, it loads the child
// node from disk.
func (s *childer) get(ctx context.Context, sliceIndex int) (*Shard, error) {
	if err := s.check(sliceIndex); err != nil {
		return nil, err
	}

	c := s.child(sliceIndex)
	if c != nil {
		return c, nil
	}

	return s.loadChild(ctx, sliceIndex)
}

// loadChild reads the i'th child node of this shard from disk and returns it
// as a 'child' interface
func (s *childer) loadChild(ctx context.Context, sliceIndex int) (*Shard, error) {
	lnk := s.link(sliceIndex)
	lnkLinkType, err := s.sd.childLinkType(lnk)
	if err != nil {
		return nil, err
	}

	var c *Shard
	if lnkLinkType == shardLink {
		nd, err := lnk.GetNode(ctx, s.dserv)
		if err != nil {
			return nil, err
		}
		cds, err := NewHamtFromDag(s.dserv, nd)
		if err != nil {
			return nil, err
		}

		c = cds
	} else {
		s, err := s.sd.makeShardValue(lnk)
		if err != nil {
			return nil, err
		}
		c = s
	}

	s.set(c, sliceIndex)

	return c, nil
}

func (s *childer) has(childIndex int) bool {
	return s.bitfield.Bit(childIndex)
}

func (s *childer) length() int {
	return len(s.children)
}

func (s *childer) each(ctx context.Context, cb func(*Shard) error) error {
	for i := range s.children {
		c, err := s.get(ctx, i)
		if err != nil {
			return err
		}

		if err := cb(c); err != nil {
			return err
		}
	}

	return nil
}

func (s *childer) check(sliceIndex int) error {
	if sliceIndex >= len(s.children) || sliceIndex < 0 {
		return fmt.Errorf("invalid index passed to operate children (likely corrupt bitfield)")
	}

	if len(s.children) != len(s.links) {
		return fmt.Errorf("inconsistent lengths between children array and Links array")
	}

	return nil
}
