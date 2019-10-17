// Package importer implements utilities used to create IPFS DAGs from files
// and readers.
package importer

import (
	bal "github.com/TRON-US/go-unixfs/importer/balanced"
	h "github.com/TRON-US/go-unixfs/importer/helpers"
	trickle "github.com/TRON-US/go-unixfs/importer/trickle"

	chunker "github.com/TRON-US/go-btfs-chunker"
	ipld "github.com/ipfs/go-ipld-format"
)

// BuildDagFromReader creates a DAG given a DAGService and a Splitter
// implementation (Splitters are io.Readers), using a Balanced layout.
func BuildDagFromReader(ds ipld.DAGService, spl chunker.Splitter) (ipld.Node, error) {
	dbp := h.DagBuilderParams{
		Dagserv:  ds,
		Maxlinks: h.DefaultLinksPerBlock,
	}
	db, err := dbp.New(spl)
	if err != nil {
		return nil, err
	}
	if db.IsThereMetaData() && !db.IsMetaDagBuilt() {
		err := bal.BuildMetadataDag(db)
		if err != nil {
			return nil, err
		}
		db.SetMetaDagBuilt(true)
	}
	return bal.Layout(db)
}

// BuildTrickleDagFromReader creates a DAG given a DAGService and a Splitter
// implementation (Splitters are io.Readers), using a Trickle Layout.
func BuildTrickleDagFromReader(ds ipld.DAGService, spl chunker.Splitter) (ipld.Node, error) {
	dbp := h.DagBuilderParams{
		Dagserv:  ds,
		Maxlinks: h.DefaultLinksPerBlock,
	}

	db, err := dbp.New(spl)
	if err != nil {
		return nil, err
	}
	return trickle.Layout(db)
}
