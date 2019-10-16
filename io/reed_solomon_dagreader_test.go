package io

import (
	"context"
	"io/ioutil"
	"testing"

	testu "github.com/TRON-US/go-unixfs/test"
)

const (
	testRsDefaultNumData   = 10
	testRsDefaultNumParity = 20
)

func TestReedSolomonRead(t *testing.T) {
	dserv := testu.GetDAGServ()
	inbuf, node := testu.GetRandomNode(t, dserv, 1024,
		testu.UseReedSolomon(testRsDefaultNumData, testRsDefaultNumParity))
	ctx, closer := context.WithCancel(context.Background())
	defer closer()

	reader, err := NewReedSolomonDagReader(ctx, node, dserv,
		testRsDefaultNumData, testRsDefaultNumParity, uint64(len(inbuf)))
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

// TODO: Currently we don't test the rest of the functions since everything
// is performed against a standard bytes.Buffer (implementation).
// Eventually, for completeness, we should.
