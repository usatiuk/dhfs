package com.usatiuk.kleppmanntree;

public record OpMove<TimestampT extends Comparable<TimestampT>, PeerIdT extends Comparable<PeerIdT>, MetaT extends NodeMeta, NodeIdT>
        (CombinedTimestamp<TimestampT, PeerIdT> timestamp, NodeIdT newParentId, MetaT newMeta,
         NodeIdT childId) {}
