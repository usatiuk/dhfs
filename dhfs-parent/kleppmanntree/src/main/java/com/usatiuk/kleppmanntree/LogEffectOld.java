package com.usatiuk.kleppmanntree;

import java.io.Serializable;

/**
 * Represents the old information about a node before it was moved.
 * @param oldEffectiveMove the old effective move that had caused this effect to be applied
 * @param oldParent the ID of the old parent node
 * @param oldMeta the old metadata of the node
 * @param <TimestampT> the type of the timestamp
 * @param <PeerIdT> the type of the peer ID
 * @param <MetaT> the type of the node metadata
 * @param <NodeIdT> the type of the node ID
 */
public record LogEffectOld<TimestampT extends Comparable<TimestampT>, PeerIdT extends Comparable<PeerIdT>, MetaT extends NodeMeta, NodeIdT>
        (OpMove<TimestampT, PeerIdT, MetaT, NodeIdT> oldEffectiveMove,
         NodeIdT oldParent,
         MetaT oldMeta) implements Serializable {
}
