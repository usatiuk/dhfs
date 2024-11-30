package com.usatiuk.kleppmanntree;

import java.util.List;

public record LogRecord<TimestampT extends Comparable<TimestampT>, PeerIdT extends Comparable<PeerIdT>, MetaT extends NodeMeta, NodeIdT>
        (OpMove<TimestampT, PeerIdT, MetaT, NodeIdT> op,
         List<LogEffect<TimestampT, PeerIdT, MetaT, NodeIdT>> effects) {}
