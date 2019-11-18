// Package balanced provides methods to build balanced DAGs, which are generalistic
// DAGs in which all leaves (nodes representing chunks of data) are at the same
// distance from the root. Nodes can have only a maximum number of children; to be
// able to store more leaf data nodes balanced DAGs are extended by increasing its
// depth (and having more intermediary nodes).
//
// Internal nodes are always represented by UnixFS nodes (of type `File`) encoded
// inside DAG nodes (see the `go-unixfs` package for details of UnixFS). In
// contrast, leaf nodes with data have multiple possible representations: UnixFS
// nodes as above, raw nodes with just the file data (no format) and Filestore
// nodes (that directly link to the file on disk using a format stored on a raw
// node, see the `go-ipfs/filestore` package for details of Filestore.)
//
// In the case the entire file fits into just one node it will be formatted as a
// (single) leaf node (without parent) with the possible representations already
// mentioned. This is the only scenario where the root can be of a type different
// that the UnixFS node.
//
// Notes:
// 1. In the implementation. `FSNodeOverDag` structure is used for representing
//    the UnixFS node encoded inside the DAG node.
//    (see https://github.com/ipfs/go-ipfs/pull/5118.)
// 2. `TFile` is used for backwards-compatibility. It was a bug causing the leaf
//    nodes to be generated with this type instead of `TRaw`. The former one
//    should be used (like the trickle builder does).
//    (See https://github.com/ipfs/go-ipfs/pull/5120.)
//
//                                                 +-------------+
//                                                 |   Root 4    |
//                                                 +-------------+
//                                                       |
//                            +--------------------------+----------------------------+
//                            |                                                       |
//                      +-------------+                                         +-------------+
//                      |   Node 2    |                                         |   Node 5    |
//                      +-------------+                                         +-------------+
//                            |                                                       |
//              +-------------+-------------+                           +-------------+
//              |                           |                           |
//       +-------------+             +-------------+             +-------------+
//       |   Node 1    |             |   Node 3    |             |   Node 6    |
//       +-------------+             +-------------+             +-------------+
//              |                           |                           |
//       +------+------+             +------+------+             +------+
//       |             |             |             |             |
//  +=========+   +=========+   +=========+   +=========+   +=========+
//  | Chunk 1 |   | Chunk 2 |   | Chunk 3 |   | Chunk 4 |   | Chunk 5 |
//  +=========+   +=========+   +=========+   +=========+   +=========+
//
package balanced

import (
	"context"
	"errors"
	"fmt"
	ft "github.com/TRON-US/go-unixfs"
	h "github.com/TRON-US/go-unixfs/importer/helpers"
	pb "github.com/TRON-US/go-unixfs/pb"
	"github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
	dag "github.com/ipfs/go-merkledag"
)

// Layout builds a balanced DAG layout. In a balanced DAG of depth 1, leaf nodes
// with data are added to a single `root` until the maximum number of links is
// reached. Then, to continue adding more data leaf nodes, a `newRoot` is created
// pointing to the old `root` (which will now become and intermediary node),
// increasing the depth of the DAG to 2. This will increase the maximum number of
// data leaf nodes the DAG can have (`Maxlinks() ^ depth`). The `fillNodeRec`
// function will add more intermediary child nodes to `newRoot` (which already has
// `root` as child) that in turn will have leaf nodes with data added to them.
// After that process is completed (the maximum number of links is reached),
// `fillNodeRec` will return and the loop will be repeated: the `newRoot` created
// will become the old `root` and a new root will be created again to increase the
// depth of the DAG. The process is repeated until there is no more data to add
// (i.e. the DagBuilderHelper’s Done() function returns true).
//
// The nodes are filled recursively, so the DAG is built from the bottom up. Leaf
// nodes are created first using the chunked file data and its size. The size is
// then bubbled up to the parent (internal) node, which aggregates all the sizes of
// its children and bubbles that combined size up to its parent, and so on up to
// the root. This way, a balanced DAG acts like a B-tree when seeking to a byte
// offset in the file the graph represents: each internal node uses the file size
// of its children as an index when seeking.
//
//      `Layout` creates a root and hands it off to be filled:
//
//             +-------------+
//             |   Root 1    |
//             +-------------+
//                    |
//       ( fillNodeRec fills in the )
//       ( chunks on the root.      )
//                    |
//             +------+------+
//             |             |
//        + - - - - +   + - - - - +
//        | Chunk 1 |   | Chunk 2 |
//        + - - - - +   + - - - - +
//
//                           ↓
//      When the root is full but there's more data...
//                           ↓
//
//             +-------------+
//             |   Root 1    |
//             +-------------+
//                    |
//             +------+------+
//             |             |
//        +=========+   +=========+   + - - - - +
//        | Chunk 1 |   | Chunk 2 |   | Chunk 3 |
//        +=========+   +=========+   + - - - - +
//
//                           ↓
//      ...Layout's job is to create a new root.
//                           ↓
//
//                            +-------------+
//                            |   Root 2    |
//                            +-------------+
//                                  |
//                    +-------------+ - - - - - - - - +
//                    |                               |
//             +-------------+            ( fillNodeRec creates the )
//             |   Node 1    |            ( branch that connects    )
//             +-------------+            ( "Root 2" to "Chunk 3."  )
//                    |                               |
//             +------+------+             + - - - - -+
//             |             |             |
//        +=========+   +=========+   + - - - - +
//        | Chunk 1 |   | Chunk 2 |   | Chunk 3 |
//        +=========+   +=========+   + - - - - +
//
func Layout(db *h.DagBuilderHelper) (ipld.Node, error) {
	if !db.IsMultiDagBuilder() {
		return layout(db, db.IsThereMetaData())
	}

	dbs := db.MultiHelpers()
	type ne struct {
		n   ipld.Node
		err error
	}
	dbcs := make([]chan ne, len(dbs))
	// Build sub trees concurrently
	for i, dbh := range dbs {
		dbcs[i] = make(chan ne)
		go func(db *h.DagBuilderHelper, dbc chan ne) {
			n, err := Layout(db)
			dbc <- ne{n, err}
		}(dbh, dbcs[i])
	}
	// Create the new root and attach the shard roots as children
	// Must be added in the original order
	newRoot := db.NewFSNodeOverDag(ft.TFile)
	for i := 0; i < len(dbs); i++ {
		res := <-dbcs[i]
		if res.err != nil {
			return nil, res.err
		}
		fileSize, err := res.n.Size()
		if err != nil {
			return nil, err
		}
		err = newRoot.AddChild(res.n, fileSize, db)
		if err != nil {
			return nil, err
		}
	}
	root, err := newRoot.Commit()
	if err != nil {
		return nil, err
	}

	// Add token metadata DAG, if exists, as a child to a new 'newRoot'.
	if db.IsThereMetaData() {
		fileSize, err := root.Size()
		if err != nil {
			return nil, err
		}
		root, err = db.AttachMetadataDag(root, fileSize, db.GetMetaDb().GetMetaDagRoot())
		if err != nil {
			return nil, err
		}
	}
	return root, db.Add(root)
}

// layout is the helper for the Layout logic except it can be invoked by Layout
// multiple times for multi-split chunkers.
func layout(db *h.DagBuilderHelper, addMetaDag bool) (ipld.Node, error) {
	if db.Done() {
		// No data, return just an empty node.
		root, err := db.NewLeafNode(nil, ft.TFile)
		if err != nil {
			return nil, err
		}
		// This works without Filestore support (`ProcessFileStore`).
		// TODO: Why? Is there a test case missing?
		if addMetaDag {
			root, err = db.AttachMetadataDag(root, 0, db.GetMetaDb().GetMetaDagRoot())
			if err != nil {
				return nil, err
			}
		}
		return root, db.Add(root)
	}

	// The first `root` will be a single leaf node with data
	// (corner case), after that subsequent `root` nodes will
	// always be internal nodes (with a depth > 0) that can
	// be handled by the loop.
	root, fileSize, err := db.NewLeafDataNode(ft.TFile)
	if err != nil {
		return nil, err
	}

	// Each time a DAG of a certain `depth` is filled (because it
	// has reached its maximum capacity of `db.Maxlinks()` per node)
	// extend it by making it a sub-DAG of a bigger DAG with `depth+1`.
	for depth := 1; !db.Done(); depth++ {

		// Add the old `root` as a child of the `newRoot`.
		newRoot := db.NewFSNodeOverDag(ft.TFile)
		err = newRoot.AddChild(root, fileSize, db)
		if err != nil {
			return nil, err
		}

		// Fill the `newRoot` (that has the old `root` already as child)
		// and make it the current `root` for the next iteration (when
		// it will become "old").
		root, fileSize, err = fillNodeRec(db, newRoot, depth, ft.TFile)
		if err != nil {
			return nil, err
		}
	}

	// Add token metadata DAG, if exists, as a child to the 'newRoot'.
	if addMetaDag {
		root, err = db.AttachMetadataDag(root, fileSize, db.GetMetaDb().GetMetaDagRoot())
		if err != nil {
			return nil, err
		}
	}

	return root, db.Add(root)
}

// BuildMetadataDag builds a DAG for the given db.TokenMetadata byte array and
// sets the root node to db.metaDagRoot.
func BuildMetadataDag(db *h.DagBuilderHelper) error {
	mdb := db.GetMetaDb()
	mdb.SetDb(db)
	mdb.SetSpl()

	_, err := BuildNewMetaDataDag(mdb)
	if err != nil {
		return err
	}
	return nil
}

// BuildNewMetaDataDag's preconditions include
// mdb's splitter is already set up.
func BuildNewMetaDataDag(mdb *h.MetaDagBuilderHelper) (ipld.Node, error) {
	root, fileSize, err := mdb.NewLeafDataNode(ft.TTokenMeta)
	if err != nil {
		return nil, err
	}

	// Each time a DAG of a certain `depth` is filled (because it
	// has reached its maximum capacity of `mdb.Maxlinks()` per node)
	// extend it by making it a sub-DAG of a bigger DAG with `depth+1`.
	for depth := 1; !mdb.Done(); depth++ {

		// Add the old `root` as a child of the `newRoot`.
		newRoot := mdb.NewFSNodeOverDag(ft.TTokenMeta)
		err = newRoot.AddChild(root, fileSize, mdb)
		if err != nil {
			return nil, err
		}

		// Fill the `newRoot` (that has the old `root` already as child)
		// and make it the current `root` for the next iteration (when
		// it will become "old").
		root, fileSize, err = fillNodeRec(mdb, newRoot, depth, ft.TTokenMeta)
		if err != nil {
			return nil, err
		}
	}

	mdb.SetMetaDagRoot(root)
	err = mdb.Add(root)
	if err != nil {
		return nil, err
	}

	return root, nil
}

// fillNodeRec will "fill" the given internal (non-leaf) `node` with data by
// adding child nodes to it, either leaf data nodes (if `depth` is 1) or more
// internal nodes with higher depth (and calling itself recursively on them
// until *they* are filled with data). The data to fill the node with is
// provided by DagBuilderHelper.
//
// `node` represents a (sub-)DAG root that is being filled. If called recursively,
// it is `nil`, a new node is created. If it has been called from `Layout` (see
// diagram below) it points to the new root (that increases the depth of the DAG),
// it already has a child (the old root). New children will be added to this new
// root, and those children will in turn be filled (calling `fillNodeRec`
// recursively).
//
//                      +-------------+
//                      |   `node`    |
//                      |  (new root) |
//                      +-------------+
//                            |
//              +-------------+ - - - - - - + - - - - - - - - - - - +
//              |                           |                       |
//      +--------------+             + - - - - -  +           + - - - - -  +
//      |  (old root)  |             |  new child |           |            |
//      +--------------+             + - - - - -  +           + - - - - -  +
//              |                          |                        |
//       +------+------+             + - - + - - - +
//       |             |             |             |
//  +=========+   +=========+   + - - - - +    + - - - - +
//  | Chunk 1 |   | Chunk 2 |   | Chunk 3 |    | Chunk 4 |
//  +=========+   +=========+   + - - - - +    + - - - - +
//
// The `node` to be filled uses the `FSNodeOverDag` abstraction that allows adding
// child nodes without packing/unpacking the UnixFS layer node (having an internal
// `ft.FSNode` cache).
//
// It returns the `ipld.Node` representation of the passed `node` filled with
// children and the `nodeFileSize` with the total size of the file chunk (leaf)
// nodes stored under this node (parent nodes store this to enable efficient
// seeking through the DAG when reading data later).
//
// warning: **children** pinned indirectly, but input node IS NOT pinned.
func fillNodeRec(db h.DagBuilderHelperInterface, node *h.FSNodeOverDag, depth int, fsNodeType pb.Data_DataType) (filledNode ipld.Node, nodeFileSize uint64, err error) {
	if depth < 1 {
		return nil, 0, errors.New("attempt to fillNode at depth < 1")
	}

	if node == nil {
		node = db.NewFSNodeOverDag(fsNodeType)
	}

	// Child node created on every iteration to add to parent `node`.
	// It can be a leaf node or another internal node.
	var childNode ipld.Node
	// File size from the child node needed to update the `FSNode`
	// in `node` when adding the child.
	var childFileSize uint64

	// While we have room and there is data available to be added.
	for node.NumChildren() < db.Maxlinks() && !db.Done() {

		if depth == 1 {
			// Base case: add leaf node with data.
			childNode, childFileSize, err = db.NewLeafDataNode(fsNodeType)
			if err != nil {
				return nil, 0, err
			}
		} else {
			// Recursion case: create an internal node to in turn keep
			// descending in the DAG and adding child nodes to it.
			childNode, childFileSize, err = fillNodeRec(db, nil, depth-1, fsNodeType)
			if err != nil {
				return nil, 0, err
			}
		}

		err = node.AddChild(childNode, childFileSize, db)
		if err != nil {
			return nil, 0, err
		}
	}

	nodeFileSize = node.FileSize()
	// Get the final `dag.ProtoNode` with the `FSNode` data encoded inside.
	filledNode, err = node.Commit()
	if err != nil {
		return nil, 0, err
	}

	return filledNode, nodeFileSize, nil
}

func BalancedDagDepth(ctx context.Context, root *h.FSNodeOverDag, dserv ipld.DAGService) (int, error) {
	if root == nil || root.NumChildren() <= 0 {
		return 0, nil
	}

	firstChild, err := root.GetChild(ctx, 0, dserv)
	if err != nil {
		return -1, err
	}
	subDepth, err := BalancedDagDepth(ctx, firstChild, dserv)
	if err != nil {
		return -1, err
	}
	return subDepth + 1, nil
}

type lastChildFindHelper struct {
	ctx context.Context
	db  *h.DagBuilderHelper
}

func newLastChildFindHelper(ctx context.Context, root *h.FSNodeOverDag, db *h.DagBuilderHelper, depth int) *lastChildFindHelper {
	return &lastChildFindHelper{
		ctx: ctx,
		db:  db,
	}
}

type helperArguments struct {
	root       *h.FSNodeOverDag
	lastChild  *h.FSNodeOverDag
	childDepth int
	parent     *h.FSNodeOverDag
}

func (helper *lastChildFindHelper) find(args *helperArguments) error {
	// Failure case
	if args.root == nil {
		return h.ErrUnexpectedNilArgument
	}
	// Base case: root is the last branch child.
	if args.root.NumChildren() <= helper.db.Maxlinks() {
		args.lastChild = args.root
		return nil
	}

	// Normal case: root is not the last branch child.
	var err error = nil
	args.parent = args.root
	args.childDepth--
	args.root, err = args.root.GetChild(helper.ctx, args.root.NumChildren()-1, helper.db.GetDagServ())
	if err != nil {
		return err
	}

	err = helper.find(args)
	if err != nil {
		return err
	}

	return nil
}

type lastChildInfo struct {
	lastChild  *h.FSNodeOverDag
	childDepth int
	parent     *h.FSNodeOverDag
}

func findLastChildInfo(ctx context.Context, root *h.FSNodeOverDag, db *h.DagBuilderHelper, depth int) (*lastChildInfo, error) {
	// Normal case: the given `root` is a branch node.
	findHelper := newLastChildFindHelper(ctx, root, db, depth)
	args := &helperArguments{
		root:       root,
		childDepth: depth,
	}
	err := findHelper.find(args)
	if err != nil {
		return nil, err
	}

	return &lastChildInfo{
		lastChild:  args.lastChild,
		childDepth: args.childDepth,
		parent:     args.parent,
	}, nil
}

// Precondition: The given `childInfo` is for a branch child, not for a leaf.
func appendFillLastBranchChild(ctx context.Context, childInfo *lastChildInfo, db *h.DagBuilderHelper) error {
	// Error case
	if childInfo == nil {
		return h.ErrUnexpectedProgramState
	}

	// Normal case
	child := childInfo.lastChild
	// child
	filledChild, nchildSize, err := fillNodeRec(db, child, childInfo.childDepth, child.GetFileNodeType())
	if err != nil {
		return err
	}
	// Case $3 from the comments of Append()
	parent := childInfo.parent
	if parent != nil {
		last := parent.NumChildren() - 1
		parent.RemoveChild(last, db)
		if err := parent.AddChild(filledChild, nchildSize, db); err != nil {
			return err
		}
	}

	return nil
}

// Append appends the data in `db` to the balanced format dag.
// The given `baseiNode` should include TFile or TTokenMeta FSNode.
// Case #1: The given `baseiNode` is the root & leaf of a DAG of depth 0. It has UnixFS data in it.
// Case #2: `baseiNode` is the root of a DAG of depth 1.
// Case #3: A DAG of depth > 1
func Append(ctx context.Context, baseiNode ipld.Node, db *h.DagBuilderHelper) (out ipld.Node, errOut error) {
	baseD, ok := baseiNode.(*dag.ProtoNode)
	if !ok {
		return nil, dag.ErrNotProtobuf
	}

	tBase, err := h.NewFSNFromDag(baseD)
	if err != nil {
		return nil, err
	}

	fsType := tBase.GetFileNodeType()

	treeDepth, err := BalancedDagDepth(ctx, tBase, db.GetDagServ())
	if err != nil {
		return nil, err
	}

	if treeDepth > 0 {
		// Find the information regarding the last branch child node.
		childInfo, err := findLastChildInfo(ctx, tBase, db, treeDepth)
		if err != nil {
			return nil, err
		}

		// Fill out the last branch child of `childInfo.parent` to make a complete subDag.
		if err := appendFillLastBranchChild(ctx, childInfo, db); err != nil {
			return nil, err
		}
	}
	filledBase, err := tBase.GetDagNode()
	if err != nil {
		return nil, err
	}
	fileSize := tBase.FileSize()
	if !db.Done() {
		treeDepth++
	}

	// Exhaust appending data through normal way.
	for currDepth := treeDepth; !db.Done(); currDepth++ {
		// Add the `filledBase` as a child of the `newRoot`.
		newRoot := db.NewFSNodeOverDag(fsType)
		err = newRoot.AddChild(filledBase, fileSize, db)
		if err != nil {
			return nil, err
		}

		// Fill out the sub-DAG topped by `filledBase`.
		filledBase, fileSize, err = fillNodeRec(db, newRoot, currDepth, fsType)
		if err != nil {
			return nil, err
		}
	}

	return filledBase, db.Add(filledBase)
}

// VerifyParams is used by VerifyBalancedDagStructure
type VerifyParamsForBalanced struct {
	Getter    ipld.NodeGetter
	Ctx       context.Context
	MaxLinks  int
	TreeDepth int
	Prefix    *cid.Prefix
	RawLeaves bool
	Metadata  bool
}

// VerifyBalancedDagStructure checks that the given dag matches
// exactly the balanced dag datastructure layout
func VerifyBalancedDagStructure(nd ipld.Node, p VerifyParamsForBalanced) error {
	return verifyBalancedDagRec(nd, p)
}

// Recursive call for verifying the structure of a balanced dag
func verifyBalancedDagRec(n ipld.Node, p VerifyParamsForBalanced) error {
	codec := cid.DagProtobuf
	depth := p.TreeDepth
	if depth == 0 {
		if len(n.Links()) > 0 {
			return errors.New("expected direct block")
		}
		// zero depth dag is raw data block
		switch nd := n.(type) {
		case *dag.ProtoNode:
			fsn, err := ft.FSNodeFromBytes(nd.Data())
			if err != nil {
				return err
			}

			if fsn.Type() != ft.TFile && fsn.Type() != ft.TRaw && fsn.Type() != ft.TTokenMeta {
				return errors.New("expected data or raw block or metadata block")
			}

			if p.RawLeaves {
				return errors.New("expected raw leaf, got a protobuf node")
			}
		case *dag.RawNode:
			if !p.RawLeaves {
				return errors.New("expected protobuf node as leaf")
			}
			codec = cid.Raw
		default:
			return errors.New("expected ProtoNode or RawNode")
		}
	}

	// verify prefix
	if p.Prefix != nil {
		prefix := n.Cid().Prefix()
		expect := *p.Prefix // make a copy
		expect.Codec = uint64(codec)
		if codec == cid.Raw && expect.Version == 0 {
			expect.Version = 1
		}
		if expect.MhLength == -1 {
			expect.MhLength = prefix.MhLength
		}
		if prefix != expect {
			return fmt.Errorf("unexpected cid prefix: expected: %v; got %v", expect, prefix)
		}
	}

	if depth == 0 {
		return nil
	}

	nd, ok := n.(*dag.ProtoNode)
	if !ok {
		return errors.New("expected ProtoNode")
	}

	// Verify this is a branch node
	fsn, err := ft.FSNodeFromBytes(nd.Data())
	if err != nil {
		return err
	}

	if p.Metadata {
		if fsn.Type() != ft.TTokenMeta {
			return fmt.Errorf("expected token meta as branch node, got: %s", fsn.Type())
		}
	} else {
		if fsn.Type() != ft.TFile {
			return fmt.Errorf("expected file as branch node, got: %s", fsn.Type())
		}
	}

	if fsn.Type() == ft.TFile && len(fsn.Data()) > 0 {
		return errors.New("branch node should not have FS data")
	}

	for i := 0; i < len(nd.Links()); i++ {
		child, err := nd.Links()[i].GetNode(p.Ctx, p.Getter)
		if err != nil {
			return err
		}

		if i < p.MaxLinks {
			// MaxLinks blocks
			err := verifyBalancedDagRec(child, p)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
