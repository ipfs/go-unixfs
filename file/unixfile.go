package unixfile

import (
	"context"
	"encoding/json"
	"errors"

	ft "github.com/TRON-US/go-unixfs"
	uio "github.com/TRON-US/go-unixfs/io"

	chunker "github.com/TRON-US/go-btfs-chunker"
	files "github.com/TRON-US/go-btfs-files"
	ipld "github.com/ipfs/go-ipld-format"
	dag "github.com/ipfs/go-merkledag"
)

// Number to file to prefetch in directories
// TODO: should we allow setting this via context hint?
const prefetchFiles = 4

type ufsDirectory struct {
	ctx   context.Context
	dserv ipld.DAGService
	dir   uio.Directory
	size  int64
}

type ufsIterator struct {
	ctx   context.Context
	files chan *ipld.Link
	dserv ipld.DAGService

	curName string
	curFile files.Node

	err   error
	errCh chan error
}

func (it *ufsIterator) Name() string {
	return it.curName
}

func (it *ufsIterator) Node() files.Node {
	return it.curFile
}

func (it *ufsIterator) Next() bool {
	if it.err != nil {
		return false
	}

	var l *ipld.Link
	var ok bool
	for !ok {
		if it.files == nil && it.errCh == nil {
			return false
		}
		select {
		case l, ok = <-it.files:
			if !ok {
				it.files = nil
			}
		case err := <-it.errCh:
			it.errCh = nil
			it.err = err

			if err != nil {
				return false
			}
		}
	}

	it.curFile = nil

	nd, err := l.GetNode(it.ctx, it.dserv)
	if err != nil {
		it.err = err
		return false
	}

	it.curName = l.Name
	it.curFile, it.err = NewUnixfsFile(it.ctx, it.dserv, nd, false)
	return it.err == nil
}

func (it *ufsIterator) Err() error {
	return it.err
}

func (d *ufsDirectory) Close() error {
	return nil
}

func (d *ufsDirectory) Entries() files.DirIterator {
	fileCh := make(chan *ipld.Link, prefetchFiles)
	errCh := make(chan error, 1)
	go func() {
		errCh <- d.dir.ForEachLink(d.ctx, func(link *ipld.Link) error {
			if d.ctx.Err() != nil {
				return d.ctx.Err()
			}
			select {
			case fileCh <- link:
			case <-d.ctx.Done():
				return d.ctx.Err()
			}
			return nil
		})

		close(errCh)
		close(fileCh)
	}()

	return &ufsIterator{
		ctx:   d.ctx,
		files: fileCh,
		errCh: errCh,
		dserv: d.dserv,
	}
}

func (d *ufsDirectory) Size() (int64, error) {
	return d.size, nil
}

type ufsFile struct {
	uio.DagReader
}

func (f *ufsFile) Size() (int64, error) {
	return int64(f.DagReader.Size()), nil
}

func newUnixfsDir(ctx context.Context, dserv ipld.DAGService, nd *dag.ProtoNode) (files.Directory, error) {
	dir, err := uio.NewDirectoryFromNode(dserv, nd)
	if err != nil {
		return nil, err
	}

	size, err := nd.Size()
	if err != nil {
		return nil, err
	}

	return &ufsDirectory{
		ctx:   ctx,
		dserv: dserv,

		dir:  dir,
		size: int64(size),
	}, nil
}

// NewUnixFsFile returns a DagReader for the 'nd' root node.
// If meta = true, only return a valid metadata node if it exists. If not, return error.
// If meta = false, return only the data contents.
func NewUnixfsFile(ctx context.Context, dserv ipld.DAGService, nd ipld.Node, meta bool) (files.Node, error) {
	rawNode := false
	switch dn := nd.(type) {
	case *dag.ProtoNode:
		fsn, err := ft.FSNodeFromBytes(dn.Data())
		if err != nil {
			return nil, err
		}
		if fsn.IsDir() {
			return newUnixfsDir(ctx, dserv, dn)
		}
		if fsn.Type() == ft.TSymlink {
			return files.NewLinkFile(string(fsn.Data()), nil), nil
		}

	case *dag.RawNode:
		rawNode = true
	default:
		return nil, errors.New("unknown node type")
	}

	var dr uio.DagReader
	// Keep 'nd' if raw node
	if !rawNode {
		// Split metadata node and data node if available
		dataNode, metaNode, err := checkAndSplitMetadata(ctx, nd, dserv)
		if err != nil {
			return nil, err
		}

		// Return just metadata if available
		if meta {
			if metaNode == nil {
				return nil, errors.New("no metadata is available")
			}
			nd = metaNode
		} else {
			// Select DagReader based on metadata information
			if metaNode != nil {
				mdr, err := uio.NewDagReader(ctx, metaNode, dserv)
				if err != nil {
					return nil, err
				}
				// Read all metadata
				buf := make([]byte, mdr.Size())
				_, err = mdr.CtxReadFull(ctx, buf)
				if err != nil {
					return nil, err
				}
				var rsMeta chunker.RsMetaMap
				err = json.Unmarshal(buf, &rsMeta)
				if err != nil {
					return nil, err
				}
				if rsMeta.NumData > 0 && rsMeta.NumParity > 0 && rsMeta.FileSize > 0 {
					// Always read from the actual dag root for reed solomon
					dr, err = uio.NewReedSolomonDagReader(ctx, dataNode, dserv,
						rsMeta.NumData, rsMeta.NumParity, rsMeta.FileSize)
					if err != nil {
						return nil, err
					}
				}
			}
			nd = dataNode
		}
	}

	// Use default dag reader if not a special type reader
	if dr == nil {
		var err error
		dr, err = uio.NewDagReader(ctx, nd, dserv)
		if err != nil {
			return nil, err
		}
	}

	return &ufsFile{
		DagReader: dr,
	}, nil
}

// checkAndSplitMetadata returns both data root node and metadata root node if exists from
// the DAG topped by the given 'nd'.
// Case #1: if 'nd' is dummy root with metadata root node and user data root node being children
//    return the second child node that is the root of user data sub-DAG.
// Case #2: if 'nd' is metadata, return none.
// Case #3: if 'nd' is user data, return 'nd'.
func checkAndSplitMetadata(ctx context.Context, nd ipld.Node, ds ipld.DAGService) (ipld.Node, ipld.Node, error) {
	n := nd.(*dag.ProtoNode)

	fsType, err := ft.GetFSType(n)
	if err != nil {
		return nil, nil, err
	}

	if ft.TTokenMeta == fsType {
		return nil, nil, ft.ErrMetadataAccessDenied
	}

	// Return user data and metadata if first child is of type TTokenMeta.
	if nd.Links() != nil && len(nd.Links()) >= 2 {
		children, err := ft.GetChildrenForDagWithMeta(ctx, nd, ds)
		if err != nil {
			return nil, nil, err
		}
		if children == nil {
			return nd, nil, nil
		}
		return children.DataNode, children.MetaNode, nil
	}

	return nd, nil, nil
}

var _ files.Directory = &ufsDirectory{}
var _ files.File = &ufsFile{}
