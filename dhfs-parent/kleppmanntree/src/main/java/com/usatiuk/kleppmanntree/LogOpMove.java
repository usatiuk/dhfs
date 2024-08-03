package com.usatiuk.kleppmanntree;

import java.io.Serializable;

public record LogOpMove<TimestampT extends Comparable<TimestampT>, PeerIdT extends Comparable<PeerIdT>, NameT, MetaT extends NodeMeta<NameT>, NodeIdT>
        (LogOpMoveOld<NameT, MetaT, NodeIdT> oldInfo,
         OpMove<TimestampT, PeerIdT, NameT, MetaT, NodeIdT> op) implements Serializable {}
