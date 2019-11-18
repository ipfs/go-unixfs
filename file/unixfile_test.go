package unixfile

import (
	"context"
	"github.com/TRON-US/go-unixfs/importer/helpers"

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

	// Should have no metadata
	_, err = NewUnixfsFile(ctx, dserv, node, true)
	if err == nil {
		t.Fatal("no metadata error should be returned")
	}
}

func TestUnixFsFileReadWithMetadata(t *testing.T) {
	inputMeta := []byte(`{"hello":1,"world":["33","11","22"]}`)
	dserv := testu.GetDAGServ()
	inbuf, node := testu.GetRandomNode(t, dserv, 1024,
		testu.UseBalancedWithMetadata(helpers.DefaultLinksPerBlock, inputMeta, 512, nil))
	ctx, closer := context.WithCancel(context.Background())
	defer closer()

	// Read only data
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

	// Read only metadata
	n, err = NewUnixfsFile(ctx, dserv, node, true)
	if err != nil {
		t.Fatal(err)
	}

	file = files.ToFile(n)

	outbuf, err = ioutil.ReadAll(file)
	if err != nil {
		t.Fatal(err)
	}

	err = testu.ArrComp(inputMeta, outbuf)
	if err != nil {
		t.Fatal(err)
	}
}

func TestUnixFsFileReedSolomonRead(t *testing.T) {
	dserv := testu.GetDAGServ()

	rsOpts, rsMeta := testu.UseReedSolomon(testu.TestRsDefaultNumData, testu.TestRsDefaultNumParity,
		1024, nil, 512)
	inbuf, node := testu.GetRandomNode(t, dserv, 1024, rsOpts)
	ctx, closer := context.WithCancel(context.Background())
	defer closer()

	// Read only joined data
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

	// Read only reed solomon fixed metadata
	n, err = NewUnixfsFile(ctx, dserv, node, true)
	if err != nil {
		t.Fatal(err)
	}

	file = files.ToFile(n)

	outbuf, err = ioutil.ReadAll(file)
	if err != nil {
		t.Fatal(err)
	}

	err = testu.ArrComp(rsMeta, outbuf)
	if err != nil {
		t.Fatal(err)
	}
}

func TestUnixFsFileReedSolomonMetadataRead(t *testing.T) {
	inputMeta := []byte(`{"hello":1,"world":["33","11","22"]}`)
	dserv := testu.GetDAGServ()

	rsOpts, rsMeta := testu.UseReedSolomon(testu.TestRsDefaultNumData, testu.TestRsDefaultNumParity,
		1024, inputMeta, 512)
	inbuf, node := testu.GetRandomNode(t, dserv, 1024, rsOpts)
	ctx, closer := context.WithCancel(context.Background())
	defer closer()

	// Read only joined data
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

	// Read reed solomon fixed metadata + custom metadata
	n, err = NewUnixfsFile(ctx, dserv, node, true)
	if err != nil {
		t.Fatal(err)
	}

	file = files.ToFile(n)

	outbuf, err = ioutil.ReadAll(file)
	if err != nil {
		t.Fatal(err)
	}

	err = testu.ArrComp(testu.ExtendMetaBytes(rsMeta, inputMeta), outbuf)
	if err != nil {
		t.Fatal(err)
	}
}
