# Partial enumeration

This a proposal to implement on top of https://github.com/ipfs/go-unixfs/pull/94 to allow the performance optimization of not having to enumerate *all* links in the HAMT directory when evaluating a possible downgrade/switch to the basic single-node directory.

Note on terminology: "partial" may not be the most apt term here. It is just used as a contrast with the *full* enumeration of `ForEachLink` and `EnumLinksAsync`. The exact nature of the enumeration proposed needs to be further elucidated in the proposal here.

*tl;dr*: keep fetched nodes from the enumeration in the `Shard` structure (as if they had been fetched as a consequence of an `AddChild`/`RemoveChild` call, which are retained throughout the lifetime of the structure/directory).

## Trade memory for bandwidth

The key insight of this proposal is the trade-off between memory (MEM) consumed by the `HAMTDirectory`/`Shard` and the bandwidth (BW) used to fetch the different nodes comprising the full `HAMTDirectory` DAG during enumeration.

We want to minimize BW in the sense of not fetching *all* nodes at the same time but only as many needed to check if we are still above the transition threshold (`HAMTShardingSize`). It is not so much the *aggregated* BW we want to minimize (in the long run we may end up traversing the whole DAG) but the "burst" consumed in a single `Directory` API call when evaluating the transition (ideally we need only a number of nodes whose entries aggregated add up to `HAMTShardingSize`). This burst of enumeration then need not be full (all nodes) and hence we refer to partial enumeration. For this partial enumeration to be meaningful it needs to *not* overlap: we can't count the same node/entry twice toward the aggregated directory size. This means we need to track what was enumerated before: this is where we need to expense MEM.

## HAMT/Shard structure

The most meaningful way to organize this MEM is to leverage the `hamt.Shard` implementation which already stores the fetched nodes from the network (`loadChild`) in its internal `childer` structure. When we modify a shard/node we fetch it, modify it and keep it in memory (for future edits). Independent of the previous, when we enumerate the links of the `HAMTDirectory` we walk the DAG fetching the same nodes but discard them afterward (using them transiently as the path to follow to get to the leaves with the directory entries/links). (Both codes are flagged as part of this PR.)

## Implementation

The proposed implementation change is rather straightforward then: modify `makeAsyncTrieGetLinks` to store the fetched nodes (likely unifying some of its logic with `loadChild`).

We can then create a low-level internal function that fetches only a part of the DAG (canceling the context after a certain number of nodes are loaded; we don't really care which). After that we enumerate only loaded nodes (so only a partial DAG, but enough to check against `HAMTDirectory`). The "partial"term of this proposal is more crucially applied not to the enumeration itself but to the fetching process: we only fetch part of the DAG and then enumerate those nodes and compute their size.

(Technically the enumeration/traversal of the loaded DAG is full: we always start it from scratch. We could cache some traversed paths but at this iteration of the proposal the cost of CPU/MEM is negligent against the cost of BW we are trying to reduce.)

### Concurrency

For now the enumeration is blocking, as it has been before. While we fetch, enumerate and aggregate size no other operation is allowed on the `HAMTDirectory`. This is to make sure the transition is always in sync with the current size of the directory (and not with some old enumeration that happened before). To be more performant we should be fetching in parallel with other directory operations since the start of the directory load (this assumes *every* time the user loads a directory it will eventually request its root node/CID to store a modified copy in the DAG service). This alternative would require more careful design and the addition of a locking system that is beyond the scope of this proposal.
