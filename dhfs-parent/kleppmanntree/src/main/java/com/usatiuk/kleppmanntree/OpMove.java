package com.usatiuk.kleppmanntree;

import java.io.Serializable;

/**
 * Operation that moves a child node to a new parent node.
 *
 * @param timestamp    the timestamp of the operation
 * @param newParentId  the ID of the new parent node
 * @param newMeta      the new metadata of the node, can be null
 * @param childId      the ID of the child node (the node that is being moved)
 * @param <TimestampT> the type of the timestamp
 * @param <PeerIdT>    the type of the peer ID
 * @param <MetaT>      the type of the node metadata
 * @param <NodeIdT>    the type of the node ID
 */
public record OpMove<TimestampT extends Comparable<TimestampT>, PeerIdT extends Comparable<PeerIdT>, MetaT extends NodeMeta, NodeIdT>
        (CombinedTimestamp<TimestampT, PeerIdT> timestamp, NodeIdT newParentId, MetaT newMeta,
         NodeIdT childId) implements Serializable {
    /**
     * Returns the new name of the node: name extracted from the new metadata if available,
     * otherwise the child ID converted to string.
     *
     * @return the new name of the node
     */
    public String newName() {
        if (newMeta != null)
            return newMeta.name();
        return childId.toString();
    }
}
