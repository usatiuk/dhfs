package com.usatiuk.kleppmanntree;

import java.io.Serializable;

public record OpMove<TimestampT extends Comparable<TimestampT>, PeerIdT extends Comparable<PeerIdT>, MetaT extends NodeMeta, NodeIdT>
        (CombinedTimestamp<TimestampT, PeerIdT> timestamp, NodeIdT newParentId, MetaT newMeta,
         NodeIdT childId) implements Serializable {
    public String newName() {
        if (newMeta != null)
            return newMeta.getName();
        return childId.toString();
    }
}
