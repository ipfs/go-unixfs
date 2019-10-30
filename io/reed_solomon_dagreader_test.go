package io

import (
	"context"
	"io/ioutil"
	"testing"

	testu "github.com/TRON-US/go-unixfs/test"
)

func TestReedSolomonRead(t *testing.T) {
	dserv := testu.GetDAGServ()
	inbuf, node := testu.GetRandomNode(t, dserv, 1024,
		testu.UseReedSolomon(testu.TestRsDefaultNumData, testu.TestRsDefaultNumParity, nil, 0))
	ctx, closer := context.WithCancel(context.Background())
	defer closer()

	reader, err := NewReedSolomonDagReader(ctx, node, dserv,
		testu.TestRsDefaultNumData, testu.TestRsDefaultNumParity, uint64(len(inbuf)))
	if err != nil {
		t.Fatal(err)
	}

	outbuf, err := ioutil.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}

	err = testu.ArrComp(inbuf, outbuf)
	if err != nil {
		t.Fatal(err)
	}
}

func TestReedSolomonWithMetadataRead(t *testing.T) {
	inputMdata := []byte(`{"nodeid":"QmURnhjU6b2Si4rqwfpD4FDGTzJH3hGRAWSQmXtagywwdz","Price":12.4}`)
	dserv := testu.GetDAGServ()

	inbuf, node := testu.GetRandomNode(t, dserv, 1024,
		testu.UseReedSolomon(testu.TestRsDefaultNumData, testu.TestRsDefaultNumParity, inputMdata, 512))
	ctx, closer := context.WithCancel(context.Background())
	defer closer()

	mnode, err := node.Links()[0].GetNode(ctx, dserv)
	if err != nil {
		t.Fatal(err)
	}
	rsnode, err := node.Links()[1].GetNode(ctx, dserv)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := NewReedSolomonDagReader(ctx, rsnode, dserv,
		testu.TestRsDefaultNumData, testu.TestRsDefaultNumParity, uint64(len(inbuf)))
	if err != nil {
		t.Fatal(err)
	}

	mreader, err := NewDagReader(context.Background(), mnode, dserv)
	if err != nil {
		t.Fatal(err)
	}

	outbuf, err := ioutil.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}

	moutbuf, err := ioutil.ReadAll(mreader)
	if err != nil {
		t.Fatal(err)
	}

	err = testu.ArrComp(inbuf, outbuf)
	if err != nil {
		t.Fatal(err)
	}
	err = testu.ArrComp(inputMdata, moutbuf)
	if err != nil {
		t.Fatal(err)
	}
}

// TODO: Currently we don't test the rest of the functions since everything
// is performed against a standard bytes.Buffer (implementation).
// Eventually, for completeness, we should.
