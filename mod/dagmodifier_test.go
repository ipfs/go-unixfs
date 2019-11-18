package mod

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"testing"

	"github.com/TRON-US/go-unixfs/importer/trickle"
	uio "github.com/TRON-US/go-unixfs/io"
	testu "github.com/TRON-US/go-unixfs/test"
	ipld "github.com/ipfs/go-ipld-format"
	dag "github.com/ipfs/go-merkledag"

	chunker "github.com/TRON-US/go-btfs-chunker"
	"github.com/TRON-US/go-unixfs"
	"github.com/TRON-US/go-unixfs/importer/balanced"
	"github.com/TRON-US/go-unixfs/importer/helpers"
	u "github.com/ipfs/go-ipfs-util"
)

func testModWrite(t *testing.T, beg, modSize uint64, orig []byte, dm *DagModifier, opts testu.NodeOpts, meta bool) []byte {
	return testModWriteAndVerify(t, beg, modSize, orig, nil, dm, opts, meta)
}

func testModWriteAndVerify(t *testing.T, beg, modSize uint64, orig []byte, newdata []byte, dm *DagModifier, opts testu.NodeOpts, meta bool) []byte {
	if newdata == nil && modSize == 0 {
		t.Fatalf("Invalid input arguments! modSize 0 but newData is nil")
	}

	if newdata == nil {
		newdata = make([]byte, modSize)
		r := u.NewTimeSeededRand()
		r.Read(newdata)

		if modSize+beg > uint64(len(orig)) {
			orig = append(orig, make([]byte, (modSize+beg)-uint64(len(orig)))...)
		}
		copy(orig[beg:], newdata)
	}

	nmod, err := dm.WriteAt(newdata, int64(beg))
	if err != nil {
		t.Fatal(err)
	}

	if nmod != int(modSize) {
		t.Fatalf("Mod length not correct! %d != %d", nmod, modSize)
	}

	verifyNode(t, orig, dm, opts, meta)

	return orig
}

func verifyNode(t *testing.T, orig []byte, dm *DagModifier, opts testu.NodeOpts, meta bool) {
	nd, err := dm.GetNode()
	if err != nil {
		t.Fatal(err)
	}

	maxLinks := helpers.DefaultLinksPerBlock
	if opts.MaxLinks > 0 {
		maxLinks = opts.MaxLinks
	}
	if opts.Balanced {
		baseD, ok := nd.(*dag.ProtoNode)
		if !ok {
			t.Fatal(dag.ErrNotProtobuf)
		}

		tBase, err := helpers.NewFSNFromDag(baseD)
		if err != nil {
			t.Fatal(err)
		}

		treeDepth, err := balanced.BalancedDagDepth(dm.GetCtx(), tBase, dm.GetDserv())
		if err != nil {
			t.Fatal(err)
		}
		err = balanced.VerifyBalancedDagStructure(nd, balanced.VerifyParamsForBalanced{
			Getter:    dm.dagserv,
			Ctx:       dm.ctx,
			MaxLinks:  maxLinks,
			TreeDepth: treeDepth,
			Prefix:    &opts.Prefix,
			RawLeaves: opts.RawLeavesUsed,
			Metadata:  meta,
		})
	} else {
		err = trickle.VerifyTrickleDagStructure(nd, trickle.VerifyParams{
			Getter:      dm.dagserv,
			Direct:      maxLinks,
			LayerRepeat: 4,
			Prefix:      &opts.Prefix,
			RawLeaves:   opts.RawLeavesUsed,
			Metadata:    meta,
		})
	}
	if err != nil {
		t.Fatal(err)
	}

	rd, err := uio.NewDagReader(context.Background(), nd, dm.dagserv)
	if err != nil {
		t.Fatal(err)
	}

	after, err := ioutil.ReadAll(rd)
	if err != nil {
		t.Fatal(err)
	}

	err = testu.ArrComp(after, orig)
	if err != nil {
		t.Fatal(err)
	}
}

func runAllSubtests(t *testing.T, tfunc func(*testing.T, testu.NodeOpts)) {
	t.Run("opts=ProtoBufLeaves", func(t *testing.T) { tfunc(t, testu.UseProtoBufLeaves) })
	t.Run("opts=RawLeaves", func(t *testing.T) { tfunc(t, testu.UseRawLeaves) })
	t.Run("opts=CidV1", func(t *testing.T) { tfunc(t, testu.UseCidV1) })
	t.Run("opts=Blake2b256", func(t *testing.T) { tfunc(t, testu.UseBlake2b256) })
}

func TestDagModifierBasic(t *testing.T) {
	runAllSubtests(t, testDagModifierBasic)
}
func testDagModifierBasic(t *testing.T, opts testu.NodeOpts) {
	dserv := testu.GetDAGServ()
	b, n := testu.GetRandomNode(t, dserv, 50000, opts)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dagmod, err := NewDagModifier(ctx, n, dserv, testu.SizeSplitterGen(512))
	if err != nil {
		t.Fatal(err)
	}
	if opts.ForceRawLeaves {
		dagmod.RawLeaves = true
	}

	// Within zero block
	beg := uint64(15)
	length := uint64(60)

	t.Log("Testing mod within zero block")
	b = testModWrite(t, beg, length, b, dagmod, opts, false)

	// Within bounds of existing file
	beg = 1000
	length = 4000
	t.Log("Testing mod within bounds of existing multiblock file.")
	b = testModWrite(t, beg, length, b, dagmod, opts, false)

	// Extend bounds
	beg = 49500
	length = 4000

	t.Log("Testing mod that extends file.")
	b = testModWrite(t, beg, length, b, dagmod, opts, false)

	// "Append"
	beg = uint64(len(b))
	length = 3000
	t.Log("Testing pure append")
	_ = testModWrite(t, beg, length, b, dagmod, opts, false)

	// Verify reported length
	node, err := dagmod.GetNode()
	if err != nil {
		t.Fatal(err)
	}

	size, err := FileSize(node)
	if err != nil {
		t.Fatal(err)
	}

	expected := uint64(50000 + 3500 + 3000)
	if size != expected {
		t.Fatalf("Final reported size is incorrect [%d != %d]", size, expected)
	}
}

func TestMultiWrite(t *testing.T) {
	runAllSubtests(t, testMultiWrite)
}
func testMultiWrite(t *testing.T, opts testu.NodeOpts) {
	dserv := testu.GetDAGServ()
	n := testu.GetEmptyNode(t, dserv, opts)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dagmod, err := NewDagModifier(ctx, n, dserv, testu.SizeSplitterGen(512))
	if err != nil {
		t.Fatal(err)
	}
	if opts.ForceRawLeaves {
		dagmod.RawLeaves = true
	}

	data := make([]byte, 4000)
	u.NewTimeSeededRand().Read(data)

	for i := 0; i < len(data); i++ {
		n, err := dagmod.WriteAt(data[i:i+1], int64(i))
		if err != nil {
			t.Fatal(err)
		}
		if n != 1 {
			t.Fatal("Somehow wrote the wrong number of bytes! (n != 1)")
		}

		size, err := dagmod.Size()
		if err != nil {
			t.Fatal(err)
		}

		if size != int64(i+1) {
			t.Fatal("Size was reported incorrectly")
		}
	}

	verifyNode(t, data, dagmod, opts, false)
}

func TestMultiWriteAndFlush(t *testing.T) {
	runAllSubtests(t, testMultiWriteAndFlush)
}
func testMultiWriteAndFlush(t *testing.T, opts testu.NodeOpts) {
	dserv := testu.GetDAGServ()
	n := testu.GetEmptyNode(t, dserv, opts)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dagmod, err := NewDagModifier(ctx, n, dserv, testu.SizeSplitterGen(512))
	if err != nil {
		t.Fatal(err)
	}
	if opts.ForceRawLeaves {
		dagmod.RawLeaves = true
	}

	data := make([]byte, 20)
	u.NewTimeSeededRand().Read(data)

	for i := 0; i < len(data); i++ {
		n, err := dagmod.WriteAt(data[i:i+1], int64(i))
		if err != nil {
			t.Fatal(err)
		}
		if n != 1 {
			t.Fatal("Somehow wrote the wrong number of bytes! (n != 1)")
		}
		err = dagmod.Sync()
		if err != nil {
			t.Fatal(err)
		}
	}

	verifyNode(t, data, dagmod, opts, false)
}

func TestWriteNewFile(t *testing.T) {
	runAllSubtests(t, testWriteNewFile)
}
func testWriteNewFile(t *testing.T, opts testu.NodeOpts) {
	dserv := testu.GetDAGServ()
	n := testu.GetEmptyNode(t, dserv, opts)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dagmod, err := NewDagModifier(ctx, n, dserv, testu.SizeSplitterGen(512))
	if err != nil {
		t.Fatal(err)
	}
	if opts.ForceRawLeaves {
		dagmod.RawLeaves = true
	}

	towrite := make([]byte, 2000)
	u.NewTimeSeededRand().Read(towrite)

	nw, err := dagmod.Write(towrite)
	if err != nil {
		t.Fatal(err)
	}
	if nw != len(towrite) {
		t.Fatal("Wrote wrong amount")
	}

	verifyNode(t, towrite, dagmod, opts, false)
}

func TestMultiWriteCoal(t *testing.T) {
	runAllSubtests(t, testMultiWriteCoal)
}
func testMultiWriteCoal(t *testing.T, opts testu.NodeOpts) {
	dserv := testu.GetDAGServ()
	n := testu.GetEmptyNode(t, dserv, opts)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dagmod, err := NewDagModifier(ctx, n, dserv, testu.SizeSplitterGen(512))
	if err != nil {
		t.Fatal(err)
	}
	if opts.ForceRawLeaves {
		dagmod.RawLeaves = true
	}

	data := make([]byte, 1000)
	u.NewTimeSeededRand().Read(data)

	for i := 0; i < len(data); i++ {
		n, err := dagmod.WriteAt(data[:i+1], 0)
		if err != nil {
			fmt.Println("FAIL AT ", i)
			t.Fatal(err)
		}
		if n != i+1 {
			t.Fatal("Somehow wrote the wrong number of bytes! (n != 1)")
		}

	}

	verifyNode(t, data, dagmod, opts, false)
}

func TestLargeWriteChunks(t *testing.T) {
	runAllSubtests(t, testLargeWriteChunks)
}
func testLargeWriteChunks(t *testing.T, opts testu.NodeOpts) {
	dserv := testu.GetDAGServ()
	n := testu.GetEmptyNode(t, dserv, opts)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dagmod, err := NewDagModifier(ctx, n, dserv, testu.SizeSplitterGen(512))
	if err != nil {
		t.Fatal(err)
	}
	if opts.ForceRawLeaves {
		dagmod.RawLeaves = true
	}

	wrsize := 1000
	datasize := 10000000
	data := make([]byte, datasize)

	u.NewTimeSeededRand().Read(data)

	for i := 0; i < datasize/wrsize; i++ {
		n, err := dagmod.WriteAt(data[i*wrsize:(i+1)*wrsize], int64(i*wrsize))
		if err != nil {
			t.Fatal(err)
		}
		if n != wrsize {
			t.Fatal("failed to write buffer")
		}
	}

	_, err = dagmod.Seek(0, io.SeekStart)
	if err != nil {
		t.Fatal(err)
	}

	out, err := ioutil.ReadAll(dagmod)
	if err != nil {
		t.Fatal(err)
	}

	if err = testu.ArrComp(out, data); err != nil {
		t.Fatal(err)
	}
}

func TestDagTruncate(t *testing.T) {
	runAllSubtests(t, testDagTruncate)
}
func testDagTruncate(t *testing.T, opts testu.NodeOpts) {
	dserv := testu.GetDAGServ()
	b, n := testu.GetRandomNode(t, dserv, 50000, opts)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dagmod, err := NewDagModifier(ctx, n, dserv, testu.SizeSplitterGen(512))
	if err != nil {
		t.Fatal(err)
	}
	if opts.ForceRawLeaves {
		dagmod.RawLeaves = true
	}

	err = dagmod.Truncate(12345)
	if err != nil {
		t.Fatal(err)
	}
	size, err := dagmod.Size()
	if err != nil {
		t.Fatal(err)
	}

	if size != 12345 {
		t.Fatal("size was incorrect!")
	}

	_, err = dagmod.Seek(0, io.SeekStart)
	if err != nil {
		t.Fatal(err)
	}

	out, err := ioutil.ReadAll(dagmod)
	if err != nil {
		t.Fatal(err)
	}

	if err = testu.ArrComp(out, b[:12345]); err != nil {
		t.Fatal(err)
	}

	err = dagmod.Truncate(10)
	if err != nil {
		t.Fatal(err)
	}

	size, err = dagmod.Size()
	if err != nil {
		t.Fatal(err)
	}

	if size != 10 {
		t.Fatal("size was incorrect!")
	}

	err = dagmod.Truncate(0)
	if err != nil {
		t.Fatal(err)
	}

	size, err = dagmod.Size()
	if err != nil {
		t.Fatal(err)
	}

	if size != 0 {
		t.Fatal("size was incorrect!")
	}
}

// TestDagSync tests that a DAG will expand sparse during sync
// if offset > curNode's size.
func TestDagSync(t *testing.T) {
	dserv := testu.GetDAGServ()
	nd := dag.NodeWithData(unixfs.FilePBData(nil, 0))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dagmod, err := NewDagModifier(ctx, nd, dserv, testu.SizeSplitterGen(512))
	if err != nil {
		t.Fatal(err)
	}

	_, err = dagmod.Write([]byte("test1"))
	if err != nil {
		t.Fatal(err)
	}

	err = dagmod.Sync()
	if err != nil {
		t.Fatal(err)
	}

	// Truncate leave the offset at 5 and filesize at 0
	err = dagmod.Truncate(0)
	if err != nil {
		t.Fatal(err)
	}

	_, err = dagmod.Write([]byte("test2"))
	if err != nil {
		t.Fatal(err)
	}

	// When Offset > filesize , Sync will call enpandSparse
	err = dagmod.Sync()
	if err != nil {
		t.Fatal(err)
	}

	_, err = dagmod.Seek(0, io.SeekStart)
	if err != nil {
		t.Fatal(err)
	}

	out, err := ioutil.ReadAll(dagmod)
	if err != nil {
		t.Fatal(err)
	}

	if err = testu.ArrComp(out[5:], []byte("test2")); err != nil {
		t.Fatal(err)
	}
}

// TestDagTruncateSameSize tests that a DAG truncated
// to the same size (i.e., doing nothing) doesn't modify
// the DAG (its hash).
func TestDagTruncateSameSize(t *testing.T) {
	runAllSubtests(t, testDagTruncateSameSize)
}
func testDagTruncateSameSize(t *testing.T, opts testu.NodeOpts) {
	dserv := testu.GetDAGServ()
	_, n := testu.GetRandomNode(t, dserv, 50000, opts)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dagmod, err := NewDagModifier(ctx, n, dserv, testu.SizeSplitterGen(512))
	if err != nil {
		t.Fatal(err)
	}
	// Copied from `TestDagTruncate`.

	size, err := dagmod.Size()
	if err != nil {
		t.Fatal(err)
	}

	err = dagmod.Truncate(size)
	if err != nil {
		t.Fatal(err)
	}

	modifiedNode, err := dagmod.GetNode()
	if err != nil {
		t.Fatal(err)
	}

	if modifiedNode.Cid().Equals(n.Cid()) == false {
		t.Fatal("the node has been modified!")
	}
}

func TestSparseWrite(t *testing.T) {
	runAllSubtests(t, testSparseWrite)
}
func testSparseWrite(t *testing.T, opts testu.NodeOpts) {
	dserv := testu.GetDAGServ()
	n := testu.GetEmptyNode(t, dserv, opts)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dagmod, err := NewDagModifier(ctx, n, dserv, testu.SizeSplitterGen(512))
	if err != nil {
		t.Fatal(err)
	}
	if opts.ForceRawLeaves {
		dagmod.RawLeaves = true
	}

	buf := make([]byte, 5000)
	u.NewTimeSeededRand().Read(buf[2500:])

	wrote, err := dagmod.WriteAt(buf[2500:], 2500)
	if err != nil {
		t.Fatal(err)
	}

	if wrote != 2500 {
		t.Fatal("incorrect write amount")
	}

	_, err = dagmod.Seek(0, io.SeekStart)
	if err != nil {
		t.Fatal(err)
	}

	out, err := ioutil.ReadAll(dagmod)
	if err != nil {
		t.Fatal(err)
	}

	if err = testu.ArrComp(out, buf); err != nil {
		t.Fatal(err)
	}
}

func TestSeekPastEndWrite(t *testing.T) {
	runAllSubtests(t, testSeekPastEndWrite)
}
func testSeekPastEndWrite(t *testing.T, opts testu.NodeOpts) {
	dserv := testu.GetDAGServ()
	n := testu.GetEmptyNode(t, dserv, opts)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dagmod, err := NewDagModifier(ctx, n, dserv, testu.SizeSplitterGen(512))
	if err != nil {
		t.Fatal(err)
	}
	if opts.ForceRawLeaves {
		dagmod.RawLeaves = true
	}

	buf := make([]byte, 5000)
	u.NewTimeSeededRand().Read(buf[2500:])

	nseek, err := dagmod.Seek(2500, io.SeekStart)
	if err != nil {
		t.Fatal(err)
	}

	if nseek != 2500 {
		t.Fatal("failed to seek")
	}

	wrote, err := dagmod.Write(buf[2500:])
	if err != nil {
		t.Fatal(err)
	}

	if wrote != 2500 {
		t.Fatal("incorrect write amount")
	}

	_, err = dagmod.Seek(0, io.SeekStart)
	if err != nil {
		t.Fatal(err)
	}

	out, err := ioutil.ReadAll(dagmod)
	if err != nil {
		t.Fatal(err)
	}

	if err = testu.ArrComp(out, buf); err != nil {
		t.Fatal(err)
	}
}

func TestRelativeSeek(t *testing.T) {
	runAllSubtests(t, testRelativeSeek)
}
func testRelativeSeek(t *testing.T, opts testu.NodeOpts) {
	dserv := testu.GetDAGServ()
	n := testu.GetEmptyNode(t, dserv, opts)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dagmod, err := NewDagModifier(ctx, n, dserv, testu.SizeSplitterGen(512))
	if err != nil {
		t.Fatal(err)
	}
	if opts.ForceRawLeaves {
		dagmod.RawLeaves = true
	}

	for i := 0; i < 64; i++ {
		dagmod.Write([]byte{byte(i)})
		if _, err := dagmod.Seek(1, io.SeekCurrent); err != nil {
			t.Fatal(err)
		}
	}

	out, err := ioutil.ReadAll(dagmod)
	if err != nil {
		t.Fatal(err)
	}

	for i, v := range out {
		if v != 0 && i/2 != int(v) {
			t.Errorf("expected %d, at index %d, got %d", i/2, i, v)
		}
	}
}

func TestInvalidSeek(t *testing.T) {
	runAllSubtests(t, testInvalidSeek)
}
func testInvalidSeek(t *testing.T, opts testu.NodeOpts) {
	dserv := testu.GetDAGServ()
	n := testu.GetEmptyNode(t, dserv, opts)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dagmod, err := NewDagModifier(ctx, n, dserv, testu.SizeSplitterGen(512))
	if err != nil {
		t.Fatal(err)
	}
	if opts.ForceRawLeaves {
		dagmod.RawLeaves = true
	}

	_, err = dagmod.Seek(10, -10)

	if err != ErrUnrecognizedWhence {
		t.Fatal(err)
	}
}

func TestEndSeek(t *testing.T) {
	runAllSubtests(t, testEndSeek)
}
func testEndSeek(t *testing.T, opts testu.NodeOpts) {
	dserv := testu.GetDAGServ()

	n := testu.GetEmptyNode(t, dserv, opts)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dagmod, err := NewDagModifier(ctx, n, dserv, testu.SizeSplitterGen(512))
	if err != nil {
		t.Fatal(err)
	}
	if opts.ForceRawLeaves {
		dagmod.RawLeaves = true
	}

	_, err = dagmod.Write(make([]byte, 100))
	if err != nil {
		t.Fatal(err)
	}

	offset, err := dagmod.Seek(0, io.SeekCurrent)
	if err != nil {
		t.Fatal(err)
	}
	if offset != 100 {
		t.Fatal("expected the relative seek 0 to return current location")
	}

	offset, err = dagmod.Seek(0, io.SeekStart)
	if err != nil {
		t.Fatal(err)
	}
	if offset != 0 {
		t.Fatal("expected the absolute seek to set offset at 0")
	}

	offset, err = dagmod.Seek(0, io.SeekEnd)
	if err != nil {
		t.Fatal(err)
	}
	if offset != 100 {
		t.Fatal("expected the end seek to set offset at end")
	}
}

func TestReadAndSeek(t *testing.T) {
	runAllSubtests(t, testReadAndSeek)
}
func testReadAndSeek(t *testing.T, opts testu.NodeOpts) {
	dserv := testu.GetDAGServ()

	n := testu.GetEmptyNode(t, dserv, opts)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dagmod, err := NewDagModifier(ctx, n, dserv, testu.SizeSplitterGen(512))
	if err != nil {
		t.Fatal(err)
	}
	if opts.ForceRawLeaves {
		dagmod.RawLeaves = true
	}

	writeBuf := []byte{0, 1, 2, 3, 4, 5, 6, 7}
	dagmod.Write(writeBuf)

	if !dagmod.HasChanges() {
		t.Fatal("there are changes, this should be true")
	}

	readBuf := make([]byte, 4)
	offset, err := dagmod.Seek(0, io.SeekStart)
	if offset != 0 {
		t.Fatal("expected offset to be 0")
	}
	if err != nil {
		t.Fatal(err)
	}

	// read 0,1,2,3
	c, err := dagmod.Read(readBuf)
	if err != nil {
		t.Fatal(err)
	}
	if c != 4 {
		t.Fatalf("expected length of 4 got %d", c)
	}

	for i := byte(0); i < 4; i++ {
		if readBuf[i] != i {
			t.Fatalf("wrong value %d [at index %d]", readBuf[i], i)
		}
	}

	// skip 4
	_, err = dagmod.Seek(1, io.SeekCurrent)
	if err != nil {
		t.Fatalf("error: %s, offset %d, reader offset %d", err, dagmod.curWrOff, getOffset(dagmod.read))
	}

	//read 5,6,7
	readBuf = make([]byte, 3)
	c, err = dagmod.Read(readBuf)
	if err != nil {
		t.Fatal(err)
	}
	if c != 3 {
		t.Fatalf("expected length of 3 got %d", c)
	}

	for i := byte(0); i < 3; i++ {
		if readBuf[i] != i+5 {
			t.Fatalf("wrong value %d [at index %d]", readBuf[i], i)
		}

	}

}

func TestCtxRead(t *testing.T) {
	runAllSubtests(t, testCtxRead)
}
func testCtxRead(t *testing.T, opts testu.NodeOpts) {
	dserv := testu.GetDAGServ()

	n := testu.GetEmptyNode(t, dserv, opts)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dagmod, err := NewDagModifier(ctx, n, dserv, testu.SizeSplitterGen(512))
	if err != nil {
		t.Fatal(err)
	}
	if opts.ForceRawLeaves {
		dagmod.RawLeaves = true
	}

	_, err = dagmod.Write([]byte{0, 1, 2, 3, 4, 5, 6, 7})
	if err != nil {
		t.Fatal(err)
	}
	dagmod.Seek(0, io.SeekStart)

	readBuf := make([]byte, 4)
	_, err = dagmod.CtxReadFull(ctx, readBuf)
	if err != nil {
		t.Fatal(err)
	}
	err = testu.ArrComp(readBuf, []byte{0, 1, 2, 3})
	if err != nil {
		t.Fatal(err)
	}
	// TODO(Kubuxu): context cancel case, I will do it after I figure out dagreader tests,
	// because this is exacelly the same.
}

func BenchmarkDagmodWrite(b *testing.B) {
	b.StopTimer()
	dserv := testu.GetDAGServ()
	n := testu.GetEmptyNode(b, dserv, testu.UseProtoBufLeaves)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	wrsize := 4096

	dagmod, err := NewDagModifier(ctx, n, dserv, testu.SizeSplitterGen(512))
	if err != nil {
		b.Fatal(err)
	}

	buf := make([]byte, b.N*wrsize)
	u.NewTimeSeededRand().Read(buf)
	b.StartTimer()
	b.SetBytes(int64(wrsize))
	for i := 0; i < b.N; i++ {
		n, err := dagmod.Write(buf[i*wrsize : (i+1)*wrsize])
		if err != nil {
			b.Fatal(err)
		}
		if n != wrsize {
			b.Fatal("Wrote bad size")
		}
	}
}

func getOffset(reader uio.DagReader) int64 {
	offset, err := reader.Seek(0, io.SeekCurrent)
	if err != nil {
		panic("failed to retrieve offset: " + err.Error())
	}
	return offset
}

func TestTrickleMetaDagAppend(t *testing.T) {
	inputMdata := []byte(`{"nodeid":"QmURnhjU6b2Si4rqwfpD4FDGTzJH3hGRAWSQmXtagywwdz","Price":12.4}`)
	mDataToModify := []byte(`{"testitem":1234}`)
	t.Run("opts=UseTrickleWithMetadata-4", func(t *testing.T) {
		testTrickleMetaDagAppend(t, testu.UseTrickleWithMetadata(2, inputMdata, 32, mDataToModify), 4)
	})
	t.Run("opts=UseTrickleWithMetadata-16", func(t *testing.T) {
		testTrickleMetaDagAppend(t, testu.UseTrickleWithMetadata(2, inputMdata, 8, mDataToModify), 16)
	})
}

func testTrickleMetaDagAppend(t *testing.T, opts testu.NodeOpts, dataSize uint64) {
	dserv := testu.GetDAGServ()
	inputMdata := opts.Metadata

	_, node := testu.GetRandomNode(t, dserv, int64(dataSize), opts)
	ctx, closer := context.WithCancel(context.Background())
	defer closer()

	mnode, err := unixfs.GetMetaSubdagRoot(ctx, node, dserv)
	if err != nil {
		t.Fatal(err)
	}

	dagmod, err := NewDagModifier(ctx, mnode, dserv, testu.MetaSplitterGen((32)))
	if err != nil {
		t.Fatal(err)
	}

	// "Append"
	beg := uint64(len(inputMdata))
	length := uint64(dataSize)
	_ = testModWrite(t, beg, length, inputMdata, dagmod, opts, true)

	// Verify reported length
	node, err = dagmod.GetNode()
	if err != nil {
		t.Fatal(err)
	}

	size, err := FileSize(node)
	if err != nil {
		t.Fatal(err)
	}

	expected := uint64(beg + dataSize)
	if size != expected {
		t.Fatalf("Final reported size is incorrect [%d != %d]", size, expected)
	}
}

func TestBalancedMetaDagAppend(t *testing.T) {
	inMdataDepthZero := []byte(`{"Price":11.2}`)
	mDataToModify := []byte(`{"number":1234}`)
	inputMdata := []byte(`{"nodeid":"QmURnhjU6b2Si4rqwfpD4FDGTzJH3hGRAWSQmXtagywwdz","Price":12.4}`)
	userData := []byte("existing file contents")

	t.Run("opts=UseBalancedWithDepthZeroMetadata-32-staticData", func(t *testing.T) {
		cid := "QmaLy2PZK3PyiLjYLo7QPW4UDVT3igSEXUhwG6HN4tX2Ae"
		testBalancedMetadataAppendWithUserData(t, testu.UseBalancedWithMetadata(2, inMdataDepthZero, 32, mDataToModify), userData, cid)
	})
	t.Run("opts=UseBalancedWithDepthZeroMetadata-32", func(t *testing.T) {
		testBalancedMetadataAppend(t, testu.UseBalancedWithMetadata(2, inMdataDepthZero, 32, mDataToModify), 72, "")
	})
	t.Run("opts=UseBalancedWithDepthOneMetadata-32", func(t *testing.T) {
		testBalancedMetadataAppend(t, testu.UseBalancedWithMetadata(2, inputMdata, 32, mDataToModify), 72, "")
	})
	t.Run("opts=UseBalancedWithDepthTwoMetadata-8", func(t *testing.T) {
		testBalancedMetadataAppend(t, testu.UseBalancedWithMetadata(2, inputMdata, 8, mDataToModify), 72, "")
	})
}

func testBalancedMetadataAppend(t *testing.T, opts testu.NodeOpts, userDataSize int64, cid string) {
	testBalancedMetaDagAppend(t, opts, userDataSize, nil, true, cid)
}

func testBalancedMetadataAppendWithUserData(t *testing.T, opts testu.NodeOpts, userData []byte, cid string) {
	testBalancedMetaDagAppend(t, opts, 0, userData, false, cid)
}

func testBalancedMetaDagAppend(t *testing.T, opts testu.NodeOpts, userDataSize int64, userData []byte, randomData bool, ecid string) {
	dserv := testu.GetDAGServ()
	mDataToModifyWith := opts.MetadataToMdify
	var nd ipld.Node
	if randomData {
		_, nd = testu.GetRandomNode(t, dserv, userDataSize, opts)
	} else {
		nd = testu.GetNodeWithGivenData(t, dserv, userData, opts)
	}
	ctx, closer := context.WithCancel(context.Background())
	defer closer()

	children, err := unixfs.GetChildrenForDagWithMeta(ctx, nd, dserv)
	if err != nil {
		t.Fatal(err)
	}
	mnode := children.MetaNode
	dnode := children.DataNode

	dr := bytes.NewReader(mDataToModifyWith)
	dbp := testu.GetDagBuilderParams(dserv, opts)
	db, err := dbp.New(chunker.NewMetaSplitter(dr, uint64(opts.ChunkSize)))
	if err != nil {
		t.Fatal(err)
	}
	dagmod, err := NewDagModifierBalanced(ctx, mnode, dserv, testu.MetaSplitterGen((int64(opts.ChunkSize))), opts.MaxLinks)
	if err != nil {
		t.Fatal(err)
	}
	mdagmod := NewMetaDagModifierBalanced(dagmod, db)

	// Append metadata.
	beg := uint64(len(opts.Metadata))
	length := uint64(len(mDataToModifyWith))
	orig := make([]byte, beg+length)
	copy(orig, opts.Metadata)
	orig = append(orig[:beg], mDataToModifyWith...)
	_ = testModWriteAndVerify(t, beg, length, orig, mDataToModifyWith, dagmod, opts, true)

	mnode, err = mdagmod.GetNode()
	if err != nil {
		t.Fatal(err)
	}

	size, err := FileSize(mnode)
	if err != nil {
		t.Fatal(err)
	}

	expected := uint64(beg + length)
	if size != expected {
		t.Fatalf("Updated metadata reported size is incorrect [%d != %d]", size, expected)
	}

	fileSize, err := FileSize(dnode)
	if err != nil {
		t.Fatal(err)
	}

	// Attach metadata sub-DAG.
	root, err := mdagmod.db.AttachMetadataDag(dnode, fileSize, mnode)
	if err != nil {
		t.Fatal(err)
	}
	mdagmod.db.Add(root)
	size, err = FileSize(root)
	if err != nil {
		t.Fatal(err)
	}

	expected = uint64(beg+length) + uint64(userDataSize)
	if !randomData {
		expected = uint64(beg+length) + uint64(len(userData))
	}

	if size != expected {
		t.Fatalf("Final reported size is incorrect [%d != %d]", size, expected)
	}
	cs := root.Cid().String()
	if ecid != "" && ecid != cs {
		t.Fatalf("Returned cid value [%s] is not expected value [%s]", cs, ecid)
	}
}
