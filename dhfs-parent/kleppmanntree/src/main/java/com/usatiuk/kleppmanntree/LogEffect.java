package com.usatiuk.kleppmanntree;

import java.io.Serializable;

public record LogEffect<TimestampT extends Comparable<TimestampT>, PeerIdT extends Comparable<PeerIdT>, MetaT extends NodeMeta, NodeIdT>(
        LogEffectOld<TimestampT, PeerIdT, MetaT, NodeIdT> oldInfo,
        NodeIdT newParentId,
        MetaT newMeta,
        NodeIdT childId) implements Serializable {
}
