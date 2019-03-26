package io

import (
	"context"

	dag "github.com/ipfs/go-merkledag"
	ft "github.com/ipfs/go-unixfs"
	hamt "github.com/ipfs/go-unixfs/hamt"

	ipld "github.com/ipfs/go-ipld-format"
	coreiface "github.com/ipfs/interface-go-ipfs-core"
)

// ResolveUnixfsOnce resolves a single hop of a path through a graph in a
// unixfs context. This includes handling traversing sharded directories.
func ResolveUnixfsOnce(ctx context.Context, ds ipld.NodeGetter, nd ipld.Node, names []string) (*ipld.Link, []string, error) {
	pn, ok := nd.(*dag.ProtoNode)
	if ok {
		fsn, err := ft.FSNodeFromBytes(pn.Data())
		if err == nil && fsn.Type() == ft.THAMTShard {
			rods := dag.NewReadOnlyDagService(ds)
			s, err := hamt.NewHamtFromDag(rods, nd)
			if err != nil {
				return nil, nil, err
			}

			out, err := s.Find(ctx, names[0])
			if err != nil {
				return nil, nil, err
			}

			return out, names[1:], nil
		}
	}

	if pw, ok := ctx.Value("proxy-preamble").(coreiface.ProofWriter); ok {
		pw.WriteChunk(append([]byte{0}, nd.RawData()...))
	}
	return nd.ResolveLink(names)
}
