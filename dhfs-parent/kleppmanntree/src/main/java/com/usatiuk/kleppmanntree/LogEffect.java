package com.usatiuk.kleppmanntree;

public record LogEffect<TimestampT extends Comparable<TimestampT>, PeerIdT extends Comparable<PeerIdT>, MetaT extends NodeMeta, NodeIdT>(
        LogEffectOld<TimestampT, PeerIdT, MetaT, NodeIdT> oldInfo,
        OpMove<TimestampT, PeerIdT, MetaT, NodeIdT> effectiveOp,
        NodeIdT newParentId,
        MetaT newMeta,
        NodeIdT childId) {
}
