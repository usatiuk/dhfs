package com.usatiuk.kleppmanntree;

/**
 * Interface to provide recording operations to be sent to peers asynchronously.
 * @param <TimestampT> the type of the timestamp
 * @param <PeerIdT> the type of the peer ID
 * @param <MetaT> the type of the node metadata
 * @param <NodeIdT> the type of the node ID
 */
public interface OpRecorder<TimestampT extends Comparable<TimestampT>, PeerIdT extends Comparable<PeerIdT>, MetaT extends NodeMeta, NodeIdT> {
    /**
     * Records an operation to be sent to peers asynchronously.
     * The operation will be sent to all known peers in the system.
     *
     * @param op the operation to be recorded
     */
    void recordOp(OpMove<TimestampT, PeerIdT, MetaT, NodeIdT> op);

    /**
     * Records an operation to be sent to a specific peer asynchronously.
     *
     * @param peer the ID of the peer to send the operation to
     * @param op   the operation to be recorded
     */
    void recordOpForPeer(PeerIdT peer, OpMove<TimestampT, PeerIdT, MetaT, NodeIdT> op);
}
