package com.usatiuk.kleppmanntree;

import java.io.Serializable;

public record LogOpMove<TimestampT extends Comparable<TimestampT>, PeerIdT extends Comparable<PeerIdT>, MetaT extends NodeMeta, NodeIdT>
        (LogOpMoveOld<MetaT, NodeIdT> oldInfo,
         OpMove<TimestampT, PeerIdT, MetaT, NodeIdT> op) implements Serializable {}
