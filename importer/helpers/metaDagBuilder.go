package helpers

import (
	"io"

	chunker "github.com/TRON-US/go-btfs-chunker"
	ft "github.com/TRON-US/go-unixfs"
	pb "github.com/TRON-US/go-unixfs/pb"
	ipld "github.com/ipfs/go-ipld-format"
)

type MetaSplitter struct {
	r    io.Reader
	size uint64
	err  error
}

// NextBytes produces a new chunk.
func (ms *MetaSplitter) NextBytes() ([]byte, error) {
	if ms.err != nil {
		return nil, ms.err
	}

	// Return a new metadata chunk
	buf := make([]byte, ms.size)
	n, err := io.ReadFull(ms.r, buf)
	switch err {
	case io.ErrUnexpectedEOF:
		ms.err = io.EOF
		small := make([]byte, n)
		copy(small, buf)
		buf = nil
		return small, nil
	case nil:
		return buf, nil
	default:
		buf = nil
		return nil, err
	}
}

// Reader returns the io.Reader associated to this Splitter.
func (ms *MetaSplitter) Reader() io.Reader {
	return ms.r
}

func NewMetaSplitter(r io.Reader, size int64) chunker.Splitter {
	return &MetaSplitter{
		r:    r,
		size: uint64(size),
	}
}

type DagBuilderHelperInterface interface {
	NewFSNodeOverDag(pb.Data_DataType) *FSNodeOverDag
	Maxlinks() int
	Done() bool
	Add(ipld.Node) error
	NewLeafDataNode(pb.Data_DataType) (ipld.Node, uint64, error)
}

type MetaDagBuilderHelper struct {
	db          *DagBuilderHelper
	metaSpl     MetaSplitter
	recvdMErr   error
	nextMData   []byte    // next metadata chunk to return
	metaDagRoot ipld.Node // Metadata Dag root node
}

func (mdb *MetaDagBuilderHelper) prepareNext() {
	// if we already have data waiting to be consumed, we're ready
	if mdb.nextMData != nil || mdb.recvdMErr != nil {
		return
	}

	mdb.nextMData, mdb.recvdMErr = mdb.metaSpl.NextBytes()
	if mdb.recvdMErr == io.EOF {
		mdb.recvdMErr = nil
	}
}

// MetaDone returns whether or not we're done consuming the token meta data.
func (mdb *MetaDagBuilderHelper) Done() bool {
	mdb.prepareNext() // idempotent
	if mdb.recvdMErr != nil {
		return false
	}
	return mdb.nextMData == nil
}

// Next returns the next chunk of token meta data.
// reset the meta chunk read state and returns nil if EOF.
// In this case, the DAG build should finish.
func (mdb *MetaDagBuilderHelper) Next() ([]byte, error) {
	mdb.prepareNext() // idempotent
	md := mdb.nextMData
	// Reset the DagBuilder chunk read state.
	mdb.nextMData = nil
	if mdb.recvdMErr != nil {
		return nil, mdb.recvdMErr
	}
	return md, nil
}

func (mdb *MetaDagBuilderHelper) NewLeafNode(data []byte, fsNodeType pb.Data_DataType) (ipld.Node, error) {
	return mdb.db.NewLeafNode(data, fsNodeType)
}

func (mdb *MetaDagBuilderHelper) FillNodeLayer(node *FSNodeOverDag) error {
	return mdb.db.FillNodeLayer(node)
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
	mdb.db = db
}

// GetDb returns the DagbuilderHelper
func (mdb *MetaDagBuilderHelper) GetDb() *DagBuilderHelper {
	return mdb.db
}
