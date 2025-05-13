package com.usatiuk.kleppmanntree;

/**
 * Interface providing a map of newest received timestamps for each peer. (causality thresholds)
 * If a peer has some timestamp recorded in this map,
 * it means that all messages coming from this peer will have a newer timestamp.
 * @param <TimestampT>
 * @param <PeerIdT>
 */
public interface PeerTimestampLogInterface<
        TimestampT extends Comparable<TimestampT>,
        PeerIdT extends Comparable<PeerIdT>> {

    /**
     * Get the timestamp for a specific peer.
     * @param peerId the ID of the peer
     * @return the timestamp for the peer
     */
    TimestampT getForPeer(PeerIdT peerId);

    /**
     * Get the timestamp for the current peer.
     */
    void putForPeer(PeerIdT peerId, TimestampT timestamp);

}
