package unixfile

import (
	"context"
	//"fmt"
	"io/ioutil"
	"testing"

	testu "github.com/TRON-US/go-unixfs/test"

	files "github.com/TRON-US/go-btfs-files"
)

func TestUnixFsFileRead(t *testing.T) {
	dserv := testu.GetDAGServ()
	inbuf, node := testu.GetRandomNode(t, dserv, 1024, testu.UseProtoBufLeaves)
	ctx, closer := context.WithCancel(context.Background())
	defer closer()

	n, err := NewUnixfsFile(ctx, dserv, node, false)
	if err != nil {
		t.Fatal(err)
	}

	file := files.ToFile(n)

	outbuf, err := ioutil.ReadAll(file)
	if err != nil {
		t.Fatal(err)
	}

	err = testu.ArrComp(inbuf, outbuf)
	if err != nil {
		t.Fatal(err)
	}
}

func TestUnixFsFileReadWithMetadata(t *testing.T) {
	inputMeta := []byte(`{"hello":1,"world":["33","11","22"]}`)
	dserv := testu.GetDAGServ()
	inbuf, node := testu.GetRandomNode(t, dserv, 1024,
		testu.UseBalancedWithMetadata(inputMeta, 512))
	ctx, closer := context.WithCancel(context.Background())
	defer closer()

	// Read but do not display meta
	n, err := NewUnixfsFile(ctx, dserv, node, false)
	if err != nil {
		t.Fatal(err)
	}

	file := files.ToFile(n)

	outbuf, err := ioutil.ReadAll(file)
	if err != nil {
		t.Fatal(err)
	}

	err = testu.ArrComp(inbuf, outbuf)
	if err != nil {
		t.Fatal(err)
	}

	// Read both data and metadata
	n, err = NewUnixfsFile(ctx, dserv, node, true)
	if err != nil {
		t.Fatal(err)
	}

	file = files.ToFile(n)

	outbuf, err = ioutil.ReadAll(file)
	if err != nil {
		t.Fatal(err)
	}

	err = testu.ArrComp(append(inputMeta, inbuf...), outbuf)
	if err != nil {
		t.Fatal(err)
	}
}

func TestUnixFsFileReedSolomonRead(t *testing.T) {
	dserv := testu.GetDAGServ()
	inbuf, node := testu.GetRandomNode(t, dserv, 1024,
		testu.UseReedSolomon(testu.TestRsDefaultNumData, testu.TestRsDefaultNumParity, nil, 0))
	ctx, closer := context.WithCancel(context.Background())
	defer closer()

	// Pre-compute reed solomon metadata
	//rsMeta := []byte(fmt.Sprintf(`{"NumData":%d,"NumParity":%d,"FileSize":%d}`,
	//testu.TestRsDefaultNumData, testu.TestRsDefaultNumParity, uint64(len(inbuf))))

	// Read but do not display meta
	n, err := NewUnixfsFile(ctx, dserv, node, false)
	if err != nil {
		t.Fatal(err)
	}

	file := files.ToFile(n)

	outbuf, err := ioutil.ReadAll(file)
	if err != nil {
		t.Fatal(err)
	}

	err = testu.ArrComp(inbuf, outbuf)
	if err != nil {
		t.Fatal(err)
	}
}
