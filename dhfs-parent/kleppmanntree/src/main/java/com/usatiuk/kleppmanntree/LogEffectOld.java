package com.usatiuk.kleppmanntree;

import java.io.Serializable;

public record LogEffectOld<TimestampT extends Comparable<TimestampT>, PeerIdT extends Comparable<PeerIdT>, MetaT extends NodeMeta, NodeIdT>
        (CombinedTimestamp<TimestampT, PeerIdT> oldTimestamp,
         NodeIdT oldParent,
         MetaT oldMeta) implements Serializable {}
