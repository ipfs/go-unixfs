package internal

import "github.com/ipfs/go-cid"

var HAMTHashFunction func(val []byte) []byte

var LinkSizeFunction func(linkName string, linkCid cid.Cid) int
