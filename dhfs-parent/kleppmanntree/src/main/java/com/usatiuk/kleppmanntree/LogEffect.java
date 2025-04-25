package com.usatiuk.kleppmanntree;

import java.io.Serializable;

public record LogEffect<TimestampT extends Comparable<TimestampT>, PeerIdT extends Comparable<PeerIdT>, MetaT extends NodeMeta, NodeIdT>(
        LogEffectOld<TimestampT, PeerIdT, MetaT, NodeIdT> oldInfo,
        OpMove<TimestampT, PeerIdT, MetaT, NodeIdT> effectiveOp,
        NodeIdT newParentId,
        MetaT newMeta,
        NodeIdT childId) implements Serializable {
    public String oldName() {
        if (oldInfo.oldMeta() != null) {
            return oldInfo.oldMeta().name();
        }
        return childId.toString();
    }

    public String newName() {
        if (newMeta != null) {
            return newMeta.name();
        }
        return childId.toString();
    }
}
