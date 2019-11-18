package helpers

import (
	chunker "github.com/TRON-US/go-btfs-chunker"
	ft "github.com/TRON-US/go-unixfs"
	pb "github.com/TRON-US/go-unixfs/pb"
	ipld "github.com/ipfs/go-ipld-format"
)

// SuperMeta contains the common metadata fields for a BTFS object
// to be used for metadata readers and modifiers
type SuperMeta struct {
	ChunkSize     uint64
	MaxLinks      uint64
	TrickleFormat bool
}

type MetaDagBuilderHelper struct {
	db          DagBuilderHelper
	metaSpl     chunker.Splitter
	metaDagRoot ipld.Node // Metadata Dag root node
}

func NewMetaDagBuilderHelper(idb DagBuilderHelper, spl chunker.Splitter, mroot ipld.Node) *MetaDagBuilderHelper {
	return &MetaDagBuilderHelper{
		db:          idb,
		metaSpl:     spl,
		metaDagRoot: mroot,
	}
}

func GetSuperMeta(chunkSize uint64, maxLinks uint64, trickleFormat bool) interface{} {
	return &SuperMeta{
		ChunkSize:     chunkSize,
		MaxLinks:      maxLinks,
		TrickleFormat: trickleFormat,
	}
}

func (mdb *MetaDagBuilderHelper) SetSpl() {
	mdb.db.spl = mdb.metaSpl
}

func (mdb *MetaDagBuilderHelper) Done() bool {
	return mdb.db.Done()
}

func (mdb *MetaDagBuilderHelper) Next() ([]byte, error) {
	return mdb.db.Next()
}

func (mdb *MetaDagBuilderHelper) NewLeafNode(data []byte, fsNodeType pb.Data_DataType) (ipld.Node, error) {
	return mdb.db.NewLeafNode(data, fsNodeType)
}

func (mdb *MetaDagBuilderHelper) FillNodeLayer(node *FSNodeOverDag, fsNodeType pb.Data_DataType) error {
	return mdb.db.FillNodeLayer(node, fsNodeType)
}

func (mdb *MetaDagBuilderHelper) AttachMetadataDag(root ipld.Node, fileSize uint64, mRoot ipld.Node) (ipld.Node, error) {
	return mdb.db.AttachMetadataDag(root, fileSize, mRoot)
}

// NewMetaLeafDataNode builds a metadata `node` with the meta data
// obtained from the Metadata Splitter.
// It returns `ipld.Node` with the `dataSize`.
func (mdb *MetaDagBuilderHelper) NewLeafDataNode(fsNodeType pb.Data_DataType) (node ipld.Node, dataSize uint64, err error) {
	metaDataChunk, err := mdb.Next()
	if err != nil {
		return nil, 0, err
	}
	dataSize = uint64(len(metaDataChunk))

	// Create a new leaf node with token metadata.
	node, err = mdb.NewLeafNode(metaDataChunk, ft.TTokenMeta)
	if err != nil {
		return nil, 0, err
	}

	return node, dataSize, nil
}

func (mdb *MetaDagBuilderHelper) Add(node ipld.Node) error {
	return mdb.db.Add(node)
}

func (mdb *MetaDagBuilderHelper) Maxlinks() int {
	return mdb.db.Maxlinks()
}

func (mdb *MetaDagBuilderHelper) NewFSNodeOverDag(dataType pb.Data_DataType) *FSNodeOverDag {
	return mdb.db.NewFSNodeOverDag(dataType)
}

// SetMetaDagRoot sets metadata DAG root
func (mdb *MetaDagBuilderHelper) SetMetaDagRoot(root ipld.Node) {
	mdb.metaDagRoot = root
}

// GetMetaDagRoot returns the root of the token metadata DAG
func (mdb *MetaDagBuilderHelper) GetMetaDagRoot() ipld.Node {
	return mdb.metaDagRoot
}

// SetDb sets metadata DAG root
func (mdb *MetaDagBuilderHelper) SetDb(db *DagBuilderHelper) {
	mdb.db = *db
}

// GetDb returns the DagbuilderHelper
func (mdb *MetaDagBuilderHelper) GetDb() DagBuilderHelper {
	return mdb.db
}
