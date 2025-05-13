package com.usatiuk.kleppmanntree;

import java.io.Serializable;

/**
 * LogEffect is a record that represents the effect of a log entry on a tree node.
 * @param oldInfo the old information about the node, before it was moved. Null if the node did not exist before
 * @param effectiveOp the operation that had caused this effect to be applied
 * @param newParentId the ID of the new parent node
 * @param newMeta the new metadata of the node
 * @param childId the ID of the child node
 * @param <TimestampT> the type of the timestamp
 * @param <PeerIdT> the type of the peer ID
 * @param <MetaT> the type of the node metadata
 * @param <NodeIdT> the type of the node ID
 */
public record LogEffect<TimestampT extends Comparable<TimestampT>, PeerIdT extends Comparable<PeerIdT>, MetaT extends NodeMeta, NodeIdT>(
        LogEffectOld<TimestampT, PeerIdT, MetaT, NodeIdT> oldInfo,
        OpMove<TimestampT, PeerIdT, MetaT, NodeIdT> effectiveOp,
        NodeIdT newParentId,
        MetaT newMeta,
        NodeIdT childId) implements Serializable {
    public String oldName() {
        if (oldInfo.oldMeta() != null) {
            return oldInfo.oldMeta().name();
        }
        return childId.toString();
    }

    public String newName() {
        if (newMeta != null) {
            return newMeta.name();
        }
        return childId.toString();
    }
}
