package linksize

import "github.com/ipfs/go-cid"

// Deprecated: use github.com/ipfs/boxo/ipld/unixfs/private/linksize.LinkSizeFunction
var LinkSizeFunction func(linkName string, linkCid cid.Cid) int
