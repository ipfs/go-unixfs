package io

import (
	"bytes"
	"context"
	"fmt"
	"io"

	cid "github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
	rs "github.com/klauspost/reedsolomon"
)

// reedSolomonDagReader reads a dag by concurrently merging N shards
// in a N/M encoded UnixFS dag file.
// Due to reed solomon requiring N shards to exist, we cannot perform
// stream Read or Seek on this reader.
// Everything will be pre-filled in memory before supporting DagReader
// operations from a []byte reader.
type reedSolomonDagReader struct {
	*bytes.Reader // for Reader, Seeker, and WriteTo
}

type nodeBufIndex struct {
	b   *bytes.Buffer
	i   int
	err error
}

// A ReedSolomonDagReader wraps M DagReaders and reads N (data) out of
// M (data + parity) concurrently to decode the original file shards for
// the returned DagReader to use.
func NewReedSolomonDagReader(ctx context.Context, n ipld.Node, serv ipld.NodeGetter,
	numData, numParity, size uint64) (DagReader, error) {
	// TODO: Read data, parity and size from metadata when implemented

	totalShards := int(numData + numParity)
	if totalShards != len(n.Links()) {
		return nil, fmt.Errorf("number of links under node [%d] does not match set data + parity [%d]",
			len(n.Links()), totalShards)
	}

	// Grab at least N nodes then we are ready for re-construction
	// Timeout is set at upper caller level through context.Context
	nodeBufChan := make(chan nodeBufIndex)
	ctxWithCancel, cancel := context.WithCancel(ctx)
	for i, link := range n.Links() {
		go func(ctx context.Context, shardCID cid.Cid, index int) {
			node, err := serv.Get(ctx, shardCID)
			if err != nil {
				nodeBufChan <- nodeBufIndex{nil, index, err}
				return
			}
			dr, err := NewDagReader(ctx, node, serv)
			if err != nil {
				nodeBufChan <- nodeBufIndex{nil, index, err}
				return
			}
			var b bytes.Buffer
			_, err = io.Copy(&b, dr)
			if err != nil {
				nodeBufChan <- nodeBufIndex{nil, index, err}
				return
			}
			nodeBufChan <- nodeBufIndex{&b, index, nil}
		}(ctxWithCancel, link.Cid, i)
	}

	// Context deadline is set so it should exit eventually
	bufs := make([]*bytes.Buffer, totalShards)
	valid := 0
	for nbi := range nodeBufChan {
		if nbi.err != nil {
			continue
		}
		bufs[nbi.i] = nbi.b
		valid += 1
		if valid == int(numData) {
			// No need to get more nodes
			cancel()
			break
		}
	}
	if valid < int(numData) {
		return nil, fmt.Errorf("unable to obtain at least [%d] shards to join original file", numData)
	}

	// Check if we already have everything
	dataValid := true
	for i := 0; i < int(numData); i++ {
		if bufs[i] == nil {
			dataValid = false
			break
		}
	}

	// Create rs stream
	rss, err := rs.NewStreamC(int(numData), int(numParity), true, true)
	if err != nil {
		return nil, err
	}

	// Reconstruct if missing some data shards
	if !dataValid {
		valid := make([]io.Reader, totalShards)
		fill := make([]io.Writer, totalShards)
		// Make all valid shards
		// Only fill the missing data shards
		for i, b := range bufs {
			if b != nil {
				valid[i] = bytes.NewReader(b.Bytes())
			} else if i < int(numData) {
				b = &bytes.Buffer{}
				bufs[i] = b
				fill[i] = b
			}
		}
		err = rss.Reconstruct(valid, fill)
		if err != nil {
			return nil, err
		}
	}

	// Now join to have the final combined file reader
	shards := make([]io.Reader, totalShards)
	for i := 0; i < int(numData); i++ {
		shards[i] = bytes.NewReader(bufs[i].Bytes())
	}
	var dataBuf bytes.Buffer
	err = rss.Join(&dataBuf, shards, int64(size))
	if err != nil {
		return nil, err
	}

	return &reedSolomonDagReader{Reader: bytes.NewReader(dataBuf.Bytes())}, nil
}

// Size returns the total size of the data from the decoded DAG structured file
// using reed solomon algorithm.
func (rsdr *reedSolomonDagReader) Size() uint64 {
	return uint64(rsdr.Len())
}

// Close has no effect since the underlying reader is a buffer.
func (rsdr *reedSolomonDagReader) Close() error {
	return nil
}

// CtxReadFull is just a Read since there is no context for buffer.
func (rsdr *reedSolomonDagReader) CtxReadFull(ctx context.Context, out []byte) (int, error) {
	return rsdr.Read(out)
}
