package com.usatiuk.kleppmanntree;

public record LogEffectOld<TimestampT extends Comparable<TimestampT>, PeerIdT extends Comparable<PeerIdT>, MetaT extends NodeMeta, NodeIdT>
        (OpMove<TimestampT, PeerIdT, MetaT, NodeIdT> oldEffectiveMove,
         NodeIdT oldParent,
         MetaT oldMeta) {}
