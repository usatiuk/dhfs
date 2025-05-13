package com.usatiuk.kleppmanntree;

import java.io.Serializable;
import java.util.List;

/**
 * Represents a log record in the Kleppmann tree.
 * @param op the operation that is stored in this log record
 * @param effects the effects of the operation (resulting moves)
 * @param <TimestampT> the type of the timestamp
 * @param <PeerIdT> the type of the peer ID
 * @param <MetaT> the type of the node metadata
 * @param <NodeIdT> the type of the node ID
 */
public record LogRecord<TimestampT extends Comparable<TimestampT>, PeerIdT extends Comparable<PeerIdT>, MetaT extends NodeMeta, NodeIdT>
        (OpMove<TimestampT, PeerIdT, MetaT, NodeIdT> op,
         List<LogEffect<TimestampT, PeerIdT, MetaT, NodeIdT>> effects) implements Serializable {
}
