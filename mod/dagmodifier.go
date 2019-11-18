// Package mod provides DAG modification utilities to, for example,
// insert additional nodes in a unixfs DAG or truncate them.
package mod

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"

	ft "github.com/TRON-US/go-unixfs"
	ufile "github.com/TRON-US/go-unixfs/file"
	"github.com/TRON-US/go-unixfs/importer/balanced"
	help "github.com/TRON-US/go-unixfs/importer/helpers"
	trickle "github.com/TRON-US/go-unixfs/importer/trickle"
	uio "github.com/TRON-US/go-unixfs/io"

	chunker "github.com/TRON-US/go-btfs-chunker"
	cid "github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
	mdag "github.com/ipfs/go-merkledag"
)

// Common errors
var (
	ErrSeekFail           = errors.New("failed to seek properly")
	ErrUnrecognizedWhence = errors.New("unrecognized whence")
	ErrNotUnixfs          = errors.New("dagmodifier only supports unixfs nodes (proto or raw)")
)

// 2MB
var writebufferSize = 1 << 21

// DagModifier is the only struct licensed and able to correctly
// perform surgery on a DAG 'file'
// Dear god, please rename this to something more pleasant
type DagModifier struct {
	dagserv ipld.DAGService
	curNode ipld.Node

	splitter   chunker.SplitterGen
	ctx        context.Context
	readCancel func()

	writeStart uint64
	curWrOff   uint64
	wrBuf      *bytes.Buffer

	Prefix         cid.Prefix
	RawLeaves      bool
	BalancedFormat bool
	Maxlinks       int

	read uio.DagReader
}

type MetaDagModifier struct {
	*DagModifier
	db *help.DagBuilderHelper
}

// NewDagModifier returns a new DagModifier, the Cid prefix for newly
// created nodes will be inhered from the passed in node.  If the Cid
// version if not 0 raw leaves will also be enabled.  The Prefix and
// RawLeaves options can be overridden by changing them after the call.
func NewDagModifier(ctx context.Context, from ipld.Node, serv ipld.DAGService, spl chunker.SplitterGen) (*DagModifier, error) {
	return newDagModifier(ctx, from, serv, spl, 0, false)
}

func NewDagModifierBalanced(ctx context.Context, from ipld.Node, serv ipld.DAGService, spl chunker.SplitterGen, ml int) (*DagModifier, error) {
	return newDagModifier(ctx, from, serv, spl, ml, true)
}

func NewMetaDagModifierBalanced(mod *DagModifier, db *help.DagBuilderHelper) *MetaDagModifier {
	return &MetaDagModifier{
		mod,
		db,
	}
}

func newDagModifier(ctx context.Context, from ipld.Node, serv ipld.DAGService, spl chunker.SplitterGen, ml int, balanced bool) (*DagModifier, error) {
	switch from.(type) {
	case *mdag.ProtoNode, *mdag.RawNode:
		// ok
	default:
		return nil, ErrNotUnixfs
	}

	prefix := from.Cid().Prefix()
	prefix.Codec = cid.DagProtobuf
	rawLeaves := false
	if prefix.Version > 0 {
		rawLeaves = true
	}

	maxlinks := help.DefaultLinksPerBlock
	if ml > 0 {
		maxlinks = ml
	}
	return &DagModifier{
		curNode:        from.Copy(),
		dagserv:        serv,
		splitter:       spl,
		ctx:            ctx,
		Prefix:         prefix,
		RawLeaves:      rawLeaves,
		BalancedFormat: balanced,
		Maxlinks:       maxlinks,
	}, nil
}

// WriteAt will modify a dag file in place
func (dm *DagModifier) WriteAt(b []byte, offset int64) (int, error) {
	// TODO: this is currently VERY inefficient
	// each write that happens at an offset other than the current one causes a
	// flush to disk, and dag rewrite
	if offset == int64(dm.writeStart) && dm.wrBuf != nil {
		// If we would overwrite the previous write
		if len(b) >= dm.wrBuf.Len() {
			dm.wrBuf.Reset()
		}
	} else if uint64(offset) != dm.curWrOff {
		size, err := dm.Size()
		if err != nil {
			return 0, err
		}
		if offset > size {
			err := dm.expandSparse(offset - size)
			if err != nil {
				return 0, err
			}
		}

		err = dm.Sync()
		if err != nil {
			return 0, err
		}
		dm.writeStart = uint64(offset)
	}

	return dm.Write(b)
}

// A reader that just returns zeros
type zeroReader struct{}

func (zr zeroReader) Read(b []byte) (int, error) {
	for i := range b {
		b[i] = 0
	}
	return len(b), nil
}

// expandSparse grows the file with zero blocks of 4096
// A small blocksize is chosen to aid in deduplication
func (dm *DagModifier) expandSparse(size int64) error {
	r := io.LimitReader(zeroReader{}, size)
	spl := chunker.NewSizeSplitter(r, 4096)
	nnode, err := dm.appendData(dm.curNode, spl)
	if err != nil {
		return err
	}
	err = dm.dagserv.Add(dm.ctx, nnode)
	return err
}

// Write continues writing to the dag at the current offset
func (dm *DagModifier) Write(b []byte) (int, error) {
	if dm.read != nil {
		dm.read = nil
	}
	if dm.wrBuf == nil {
		dm.wrBuf = new(bytes.Buffer)
	}

	n, err := dm.wrBuf.Write(b)
	if err != nil {
		return n, err
	}
	dm.curWrOff += uint64(n)
	if dm.wrBuf.Len() > writebufferSize {
		err := dm.Sync()
		if err != nil {
			return n, err
		}
	}
	return n, nil
}

// Size returns the Filesize of the node
func (dm *DagModifier) Size() (int64, error) {
	fileSize, err := FileSize(dm.curNode)
	if err != nil {
		return 0, err
	}
	if dm.wrBuf != nil && int64(dm.wrBuf.Len())+int64(dm.writeStart) > int64(fileSize) {
		return int64(dm.wrBuf.Len()) + int64(dm.writeStart), nil
	}
	return int64(fileSize), nil
}

func FileSize(n ipld.Node) (uint64, error) {
	switch nd := n.(type) {
	case *mdag.ProtoNode:
		fsn, err := ft.FSNodeFromBytes(nd.Data())
		if err != nil {
			return 0, err
		}
		return fsn.FileSize(), nil
	case *mdag.RawNode:
		return uint64(len(nd.RawData())), nil
	default:
		return 0, ErrNotUnixfs
	}
}

// Sync writes changes to this dag to disk
func (dm *DagModifier) Sync() error {
	// No buffer? Nothing to do
	if dm.wrBuf == nil {
		return nil
	}

	// If we have an active reader, kill it
	if dm.read != nil {
		dm.read = nil
		dm.readCancel()
	}

	// Number of bytes we're going to write
	buflen := dm.wrBuf.Len()

	fs, err := FileSize(dm.curNode)
	if err != nil {
		return err
	}
	if fs < dm.writeStart {
		if err := dm.expandSparse(int64(dm.writeStart - fs)); err != nil {
			return err
		}
	}

	// overwrite existing dag nodes
	thisc, err := dm.modifyDag(dm.curNode, dm.writeStart)
	if err != nil {
		return err
	}

	dm.curNode, err = dm.dagserv.Get(dm.ctx, thisc)
	if err != nil {
		return err
	}

	// need to write past end of current dag
	if dm.wrBuf.Len() > 0 {
		dm.curNode, err = dm.appendData(dm.curNode, dm.splitter(dm.wrBuf))
		if err != nil {
			return err
		}

		err = dm.dagserv.Add(dm.ctx, dm.curNode)
		if err != nil {
			return err
		}
	}

	dm.writeStart += uint64(buflen)
	dm.wrBuf = nil

	return nil
}

// modifyDag writes the data in 'dm.wrBuf' over the data in 'node' starting at 'offset'
// returns the new key of the passed in node.
func (dm *DagModifier) modifyDag(n ipld.Node, offset uint64) (cid.Cid, error) {
	// If we've reached a leaf node.
	if len(n.Links()) == 0 {
		switch nd0 := n.(type) {
		case *mdag.ProtoNode:
			fsn, err := ft.FSNodeFromBytes(nd0.Data())
			if err != nil {
				return cid.Cid{}, err
			}

			_, err = dm.wrBuf.Read(fsn.Data()[offset:])
			if err != nil && err != io.EOF {
				return cid.Cid{}, err
			}

			// Update newly written node..
			b, err := fsn.GetBytes()
			if err != nil {
				return cid.Cid{}, err
			}

			nd := new(mdag.ProtoNode)
			nd.SetData(b)
			nd.SetCidBuilder(nd0.CidBuilder())
			err = dm.dagserv.Add(dm.ctx, nd)
			if err != nil {
				return cid.Cid{}, err
			}

			return nd.Cid(), nil
		case *mdag.RawNode:
			origData := nd0.RawData()
			bytes := make([]byte, len(origData))

			// copy orig data up to offset
			copy(bytes, origData[:offset])

			// copy in new data
			n, err := dm.wrBuf.Read(bytes[offset:])
			if err != nil && err != io.EOF {
				return cid.Cid{}, err
			}

			// copy remaining data
			offsetPlusN := int(offset) + n
			if offsetPlusN < len(origData) {
				copy(bytes[offsetPlusN:], origData[offsetPlusN:])
			}

			nd, err := mdag.NewRawNodeWPrefix(bytes, nd0.Cid().Prefix())
			if err != nil {
				return cid.Cid{}, err
			}
			err = dm.dagserv.Add(dm.ctx, nd)
			if err != nil {
				return cid.Cid{}, err
			}

			return nd.Cid(), nil
		}
	}

	node, ok := n.(*mdag.ProtoNode)
	if !ok {
		return cid.Cid{}, ErrNotUnixfs
	}

	fsn, err := ft.FSNodeFromBytes(node.Data())
	if err != nil {
		return cid.Cid{}, err
	}

	var cur uint64
	for i, bs := range fsn.BlockSizes() {
		// We found the correct child to write into
		if cur+bs > offset {
			child, err := node.Links()[i].GetNode(dm.ctx, dm.dagserv)
			if err != nil {
				return cid.Cid{}, err
			}

			k, err := dm.modifyDag(child, offset-cur)
			if err != nil {
				return cid.Cid{}, err
			}

			node.Links()[i].Cid = k

			// Recache serialized node
			_, err = node.EncodeProtobuf(true)
			if err != nil {
				return cid.Cid{}, err
			}

			if dm.wrBuf.Len() == 0 {
				// No more bytes to write!
				break
			}
			offset = cur + bs
		}
		cur += bs
	}

	err = dm.dagserv.Add(dm.ctx, node)
	return node.Cid(), err
}

// appendData appends the blocks from the given chan to the end of this dag
func (dm *DagModifier) appendData(nd ipld.Node, spl chunker.Splitter) (ipld.Node, error) {
	switch nd := nd.(type) {
	case *mdag.ProtoNode, *mdag.RawNode:
		dbp := &help.DagBuilderParams{
			Dagserv:    dm.dagserv,
			Maxlinks:   dm.Maxlinks,
			CidBuilder: dm.Prefix,
			RawLeaves:  dm.RawLeaves,
		}
		db, err := dbp.New(spl)
		if err != nil {
			return nil, err
		}
		if dm.BalancedFormat {
			return balanced.Append(dm.ctx, nd, db)
		} else {
			return trickle.Append(dm.ctx, nd, db)
		}
	default:
		return nil, ErrNotUnixfs
	}
}

// Read data from this dag starting at the current offset
func (dm *DagModifier) Read(b []byte) (int, error) {
	err := dm.readPrep()
	if err != nil {
		return 0, err
	}

	n, err := dm.read.Read(b)
	dm.curWrOff += uint64(n)
	return n, err
}

func (dm *DagModifier) readPrep() error {
	err := dm.Sync()
	if err != nil {
		return err
	}

	if dm.read == nil {
		ctx, cancel := context.WithCancel(dm.ctx)
		dr, err := uio.NewDagReader(ctx, dm.curNode, dm.dagserv)
		if err != nil {
			cancel()
			return err
		}

		i, err := dr.Seek(int64(dm.curWrOff), io.SeekStart)
		if err != nil {
			cancel()
			return err
		}

		if i != int64(dm.curWrOff) {
			cancel()
			return ErrSeekFail
		}

		dm.readCancel = cancel
		dm.read = dr
	}

	return nil
}

// CtxReadFull reads data from this dag starting at the current offset
func (dm *DagModifier) CtxReadFull(ctx context.Context, b []byte) (int, error) {
	err := dm.readPrep()
	if err != nil {
		return 0, err
	}

	n, err := dm.read.CtxReadFull(ctx, b)
	dm.curWrOff += uint64(n)
	return n, err
}

// GetNode gets the modified DAG Node
func (dm *DagModifier) GetNode() (ipld.Node, error) {
	err := dm.Sync()
	if err != nil {
		return nil, err
	}
	return dm.curNode.Copy(), nil
}

// HasChanges returned whether or not there are unflushed changes to this dag
func (dm *DagModifier) HasChanges() bool {
	return dm.wrBuf != nil
}

// Seek modifies the offset according to whence. See unixfs/io for valid whence
// values.
func (dm *DagModifier) Seek(offset int64, whence int) (int64, error) {
	err := dm.Sync()
	if err != nil {
		return 0, err
	}

	fisize, err := dm.Size()
	if err != nil {
		return 0, err
	}

	var newoffset uint64
	switch whence {
	case io.SeekCurrent:
		newoffset = dm.curWrOff + uint64(offset)
	case io.SeekStart:
		newoffset = uint64(offset)
	case io.SeekEnd:
		newoffset = uint64(fisize) - uint64(offset)
	default:
		return 0, ErrUnrecognizedWhence
	}

	if int64(newoffset) > fisize {
		if err := dm.expandSparse(int64(newoffset) - fisize); err != nil {
			return 0, err
		}
	}
	dm.curWrOff = newoffset
	dm.writeStart = newoffset

	if dm.read != nil {
		_, err = dm.read.Seek(offset, whence)
		if err != nil {
			return 0, err
		}
	}

	return int64(dm.curWrOff), nil
}

// Truncate truncates the current Node to 'size' and replaces it with the
// new one.
func (dm *DagModifier) Truncate(size int64) error {
	err := dm.Sync()
	if err != nil {
		return err
	}

	realSize, err := dm.Size()
	if err != nil {
		return err
	}
	if size == int64(realSize) {
		return nil
	}

	// Truncate can also be used to expand the file
	if size > int64(realSize) {
		return dm.expandSparse(int64(size) - realSize)
	}

	nnode, err := dm.dagTruncate(dm.ctx, dm.curNode, uint64(size))
	if err != nil {
		return err
	}

	err = dm.dagserv.Add(dm.ctx, nnode)
	if err != nil {
		return err
	}

	dm.curNode = nnode
	return nil
}

// dagTruncate truncates the given node to 'size' and returns the modified Node
func (dm *DagModifier) dagTruncate(ctx context.Context, n ipld.Node, size uint64) (ipld.Node, error) {
	if len(n.Links()) == 0 {
		switch nd := n.(type) {
		case *mdag.ProtoNode:
			// TODO: this can likely be done without marshaling and remarshaling
			fsn, err := ft.FSNodeFromBytes(nd.Data())
			if err != nil {
				return nil, err
			}
			nd.SetData(ft.WrapData(fsn.Data()[:size]))
			return nd, nil
		case *mdag.RawNode:
			return mdag.NewRawNodeWPrefix(nd.RawData()[:size], nd.Cid().Prefix())
		}
	}

	nd, ok := n.(*mdag.ProtoNode)
	if !ok {
		return nil, ErrNotUnixfs
	}

	var cur uint64
	end := 0
	var modified ipld.Node
	ndata, err := ft.FSNodeFromBytes(nd.Data())
	if err != nil {
		return nil, err
	}
	// Reset the block sizes of the node to adjust them
	// with the new values of the truncated children.
	ndata.RemoveAllBlockSizes()
	for i, lnk := range nd.Links() {
		child, err := lnk.GetNode(ctx, dm.dagserv)
		if err != nil {
			return nil, err
		}

		childsize, err := FileSize(child)
		if err != nil {
			return nil, err
		}

		// found the child we want to cut
		if size < cur+childsize {
			nchild, err := dm.dagTruncate(ctx, child, size-cur)
			if err != nil {
				return nil, err
			}

			ndata.AddBlockSize(size - cur)

			modified = nchild
			end = i
			break
		}
		cur += childsize
		ndata.AddBlockSize(childsize)
	}

	err = dm.dagserv.Add(ctx, modified)
	if err != nil {
		return nil, err
	}

	nd.SetLinks(nd.Links()[:end])
	err = nd.AddNodeLink("", modified)
	if err != nil {
		return nil, err
	}

	d, err := ndata.GetBytes()
	if err != nil {
		return nil, err
	}
	// Save the new block sizes to the original node.
	nd.SetData(d)

	// invalidate cache and recompute serialized data
	_, err = nd.EncodeProtobuf(true)
	if err != nil {
		return nil, err
	}

	return nd, nil
}

func (dm *DagModifier) GetCtx() context.Context {
	return dm.ctx
}

func (dm *DagModifier) GetDserv() ipld.DAGService {
	return dm.dagserv
}

func (mdm *MetaDagModifier) GetDb() *help.DagBuilderHelper {
	return mdm.db
}

// AddMetadata puts the given `metadata` items to the given `root`
// that is the top node of a BTFS file DAG and returns a new root for the BTFS file DAG.
//
// There are three scenarios possible:
// Scenario #1. No existing metadata for the `root`. So this scenario is
//    to create a new metadata sub-DAG.
// Scenario #2. There are existing keys for the given `metadata` items.
//    Then this is to update the metadata sub-DAG with the given items.
// Scenario #3. There are no pre-existing metadata items for the given `metadata` items.
//    Then this scenario is to append given items to the metadata sub-DAG.
// TODO: trickle format
// Preconditions include that first the given `metadata` is in compact JSON format.
func (mdm *MetaDagModifier) AddMetadata(root ipld.Node, metadata []byte) (ipld.Node, error) {
	// Read the existing metadata map.
	b, err := mdm.readMetadataBytes(root)
	if err != nil {
		return nil, err
	}

	// Determine the specific scenario.
	// Scenario #1:
	var newMetaNode ipld.Node
	var children *ft.DagMetaNodes
	if b == nil {
		// TODO: Get SuperMeta items and add to metadata.
		// Create a metadata sub-DAG
		newMetaNode, err = mdm.buildNewMetaDataDag(metadata)
		if err != nil {
			return nil, err
		}
	} else {
		children, err := ft.GetChildrenForDagWithMeta(mdm.ctx, root, mdm.dagserv)
		if err != nil {
			return nil, err
		}

		// Create existing map and input metadata map and check.
		m := make(map[string]interface{})
		err = json.Unmarshal(b, &m)
		if err != nil {
			return nil, err
		}

		inputM := make(map[string]interface{})
		err = json.Unmarshal(metadata, &inputM)
		if err != nil {
			return nil, err
		}

		exists := intersects(m, inputM)
		// Scenario #2: Update case.
		if exists {
			// iterate the inputM to put its (k, v) pairs to existing map.
			for k, v := range inputM {
				m[k] = v
			}
			b, err = json.Marshal(m)
			if err != nil {
				return nil, err
			}
			// Create a metadata sub-DAG
			newMetaNode, err = mdm.buildNewMetaDataDag(b)
			if err != nil {
				return nil, err
			}
		} else
		// Scenario #3: Append case.
		{
			// Append the given metadata items to the metadata sub-DAG.
			mdm.curNode = children.MetaNode
			fileSize, err := FileSize(mdm.curNode)
			if err != nil {
				return nil, err
			}

			// Combine two JSON format byte arrays. E.g.,
			// `{"price":12.22} + `{"number":1234}` -> `{"price":12.22,"number":1234}`
			metadata[0] = ','
			nmod, err := mdm.WriteAt(metadata, int64(fileSize)-1)
			if err != nil {
				return nil, err
			}

			if nmod != int(len(metadata)) {
				return nil, errors.New("Modified length not correct!")
			}

			newMetaNode, err = mdm.GetNode()
			if err != nil {
				return nil, err
			}
		}
	}

	// Attach the modified metadata sub-DAG to a new root for the BTFS file DAG.
	var dnode ipld.Node
	if children == nil {
		dnode = root
	} else {
		dnode = children.DataNode
	}

	fileSize, err := FileSize(dnode)
	if err != nil {
		return nil, err
	}

	newRoot, err := mdm.GetDb().AttachMetadataDag(dnode, fileSize, newMetaNode)
	if err != nil {
		return nil, err
	}
	mdm.GetDb().Add(newRoot)

	return newRoot, nil
}

// RemoveMetadata first truncate the metadata dag from the given `root`,
// update the metadata map with the given `metadata`,
// then calls meta-dag builder to create a metadata subDag and
// attach the metadata dag to a new root with the given `dataroot` being
// a child of the new root.
// The preconditions include that first the given `metadata` is a string with keys separated by
// commas, second`mdm.curNode` has the root of the metadata sub-DAG.
func (mdm *MetaDagModifier) RemoveMetadata(root ipld.Node, metakeys []byte) (ipld.Node, error) {
	// Read the existing metadata map.
	b, err := mdm.readMetadataBytes(root)
	if err != nil {
		return nil, err
	}

	// Determine the specific scenario.
	// Scenario #1:
	var newMetaNode ipld.Node
	var children *ft.DagMetaNodes
	var modificationRequired bool
	var clear bool
	children, err = ft.GetChildrenForDagWithMeta(mdm.ctx, root, mdm.dagserv)
	if err != nil {
		return nil, err
	}
	if children == nil {
		return nil, errors.New("expected DAG node with metadata child node")
	}

	if b != nil {

		// Create existing map and input metadata map and check.
		m := make(map[string]interface{})
		err = json.Unmarshal(b, &m)
		if err != nil {
			return nil, err
		}

		inputKeys := strings.Split(string(metakeys), ",")

		exists := keyIntersects(m, inputKeys)

		if exists {
			modificationRequired = true
			// Check if inputM and m have the same key set
			if equalKeySets(m, inputKeys) {
				// Scenario #1: clear metadata.
				clear = true
			} else {
				// Scenario #2: delete a subset of the metadata map.
				// iterate the inputKeys to delete each key from the existing map.
				for _, k := range inputKeys {
					delete(m, k)
				}

				b, err = json.Marshal(m)
				if err != nil {
					return nil, err
				}
				// Create a metadata sub-DAG
				newMetaNode, err = mdm.buildNewMetaDataDag(b)
				if err != nil {
					return nil, err
				}
			}
		}
	}

	if !modificationRequired {
		return root, nil
	}

	dnode := children.DataNode
	// if clear is true, return dnode as root.
	if clear {
		return dnode, nil
	}

	// Attach the modified metadata sub-DAG to a new root for the BTFS file DAG.
	fileSize, err := FileSize(dnode)
	if err != nil {
		return nil, err
	}

	newRoot, err := mdm.GetDb().AttachMetadataDag(dnode, fileSize, newMetaNode)
	if err != nil {
		return nil, err
	}
	mdm.GetDb().Add(newRoot)

	return newRoot, nil
}

func (mdm *MetaDagModifier) buildNewMetaDataDag(metaBytes []byte) (ipld.Node, error) {
	var superMeta help.SuperMeta
	err := json.Unmarshal(metaBytes, &superMeta)
	if err != nil {
		return nil, err
	}

	r := bytes.NewReader(metaBytes)
	if superMeta.ChunkSize == 0 {
		superMeta.ChunkSize = 256 * 1024
	}
	if superMeta.MaxLinks == 0 {
		superMeta.MaxLinks = uint64(help.DefaultLinksPerBlock)
	}
	// TODO: trickle format input should be set somewhere..

	metaSpl := chunker.NewMetaSplitter(r, superMeta.ChunkSize)
	metaDb := help.NewMetaDagBuilderHelper(*mdm.GetDb(), metaSpl, nil)
	var mnode ipld.Node
	if superMeta.TrickleFormat {
		mnode, err = trickle.BuildNewMetaDataDag(metaDb)
	} else {
		mnode, err = balanced.BuildNewMetaDataDag(metaDb)
	}
	if err != nil {
		return nil, err
	}
	return mnode, nil
}

func (mdm *MetaDagModifier) readMetadataBytes(root ipld.Node) ([]byte, error) {
	nd, ok := root.(*mdag.ProtoNode)
	if !ok {
		return nil, errors.New("Expected protobuf Merkle DAG node")
	}
	fsn, err := ft.FSNodeFromBytes(nd.Data())
	if err != nil {
		return nil, err
	}
	if fsn.Type() != ft.TFile {
		return nil, errors.New("Expected file type node")
	}

	_, mnode, err := ufile.CheckAndSplitMetadata(mdm.ctx, root, mdm.dagserv, false)
	if err != nil {
		return nil, err
	}
	if mnode == nil {
		return nil, nil
	}

	r, err := uio.NewDagReader(mdm.ctx, mnode, mdm.dagserv)
	if err != nil {
		return nil, err
	}

	b := make([]byte, r.Size())
	_, err = r.CtxReadFull(mdm.ctx, b)
	if err != nil {
		return nil, err
	}

	return b, nil
}

func intersects(m map[string]interface{}, inputM map[string]interface{}) bool {
	for k, _ := range inputM {
		_, isPresent := m[k]
		if isPresent {
			return true
		}
	}
	return false
}

func keyIntersects(m map[string]interface{}, inputKeys []string) bool {
	for _, k := range inputKeys {
		_, isPresent := m[k]
		if isPresent {
			return true
		}
	}
	return false
}

func equalKeySets(m map[string]interface{}, inputKeys []string) bool {
	if len(m) != len(inputKeys) {
		return false
	}

	for _, ik := range inputKeys {
		_, isPresent := m[ik]
		if !isPresent {
			return false
		}
	}

	return true
}
