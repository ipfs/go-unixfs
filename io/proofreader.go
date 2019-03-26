package io

import (
	"context"
	"fmt"
	"io"

	ipld "github.com/ipfs/go-ipld-format"
	dag "github.com/ipfs/go-merkledag"
	mdag "github.com/ipfs/go-merkledag"
	unixfs "github.com/ipfs/go-unixfs"
	pb "github.com/ipfs/go-unixfs/pb"
	coreiface "github.com/ipfs/interface-go-ipfs-core"
)

// NewDagReaderWithProof creates a new proof reader object that reads the data
// represented by the given node, using the passed in DAGService for data
// retrieval.
func NewDagReaderWithProof(ctx context.Context, n ipld.Node, serv ipld.NodeGetter) (coreiface.ProofReader, error) {
	switch n := n.(type) {
	case *mdag.RawNode:
	case *mdag.ProtoNode:
		fsNode, err := unixfs.FSNodeFromBytes(n.Data())
		if err != nil {
			return nil, err
		}

		switch fsNode.Type() {
		case unixfs.TFile, unixfs.TRaw:
		case unixfs.TDirectory, unixfs.THAMTShard:
			return nil, ErrIsDir
		case unixfs.TSymlink:
			return nil, ErrCantReadSymlinks
		default:
			return nil, unixfs.ErrUnrecognizedType
		}
	default:
		return nil, ErrUnkownNodeType
	}

	ctxWithCancel, cancel := context.WithCancel(ctx)

	return &proofReader{
		cancel:    cancel,
		dagWalker: ipld.NewWalker(ctxWithCancel, ipld.NewNavigableIPLDNode(n, serv)),
	}, nil
}

// proofReader provides a way to easily read the data contained in a dag, in a
// way that it can be presented to an untrusting client.
type proofReader struct {
	dagWalker *ipld.Walker
	cancel    func()
}

func (pr *proofReader) ReadChunk() ([]byte, error) {
	var out []byte

	err := pr.dagWalker.Iterate(func(visitedNode ipld.NavigableNode) error {
		node := ipld.ExtractIPLDNode(visitedNode)

		switch node := node.(type) {
		case *dag.RawNode:
			out = append([]byte{0}, node.RawData()...)

		case *dag.ProtoNode:
			fsNode, err := unixfs.FSNodeFromBytes(node.Data())
			if err != nil {
				return fmt.Errorf("incorrectly formatted protobuf: %s", err)
			}

			switch fsNode.Type() {
			case pb.Data_File, pb.Data_Raw:
				out = append([]byte{1}, node.RawData()...)
			default:
				return fmt.Errorf("found %s node in unexpected place",
					fsNode.Type().String())
			}

		default:
			return unixfs.ErrUnrecognizedType
		}

		pr.dagWalker.Pause()
		return nil
	})

	if err == ipld.EndOfDag {
		return nil, io.EOF
	} else if err != nil {
		return nil, err
	}
	return out, nil
}

func (pr *proofReader) Close() error {
	pr.cancel()
	return nil
}
