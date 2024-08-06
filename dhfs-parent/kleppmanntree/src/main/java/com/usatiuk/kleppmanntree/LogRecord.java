package com.usatiuk.kleppmanntree;

import java.io.Serializable;
import java.util.List;

public record LogRecord<TimestampT extends Comparable<TimestampT>, PeerIdT extends Comparable<PeerIdT>, MetaT extends NodeMeta, NodeIdT>
        (OpMove<TimestampT, PeerIdT, ? extends MetaT, NodeIdT> op,
         List<LogEffect<TimestampT, PeerIdT, ? extends MetaT, NodeIdT>> effects) implements Serializable {}
