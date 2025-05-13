package com.usatiuk.kleppmanntree;

import jakarta.annotation.Nullable;
import org.pcollections.PMap;

import java.io.Serializable;

/**
 * Represents a node in the Kleppmann tree.
 *
 * @param <TimestampT> the type of the timestamp
 * @param <PeerIdT>    the type of the peer ID
 * @param <MetaT>      the type of the node metadata
 * @param <NodeIdT>    the type of the node ID
 */
public interface TreeNode<TimestampT extends Comparable<TimestampT>, PeerIdT extends Comparable<PeerIdT>, MetaT extends NodeMeta, NodeIdT> extends Serializable {
    /**
     * Get the ID of the node.
     *
     * @return the ID of the node
     */
    NodeIdT key();

    /**
     * Get the ID of the parent node.
     *
     * @return the ID of the parent node
     */
    NodeIdT parent();

    /**
     * Get the last effective operation that moved this node.
     *
     * @return the last effective operation
     */
    OpMove<TimestampT, PeerIdT, MetaT, NodeIdT> lastEffectiveOp();

    /**
     * Get the metadata stored in this node.
     *
     * @return the metadata of the node
     */
    @Nullable
    MetaT meta();

    /**
     * Get the name of the node.
     * If the node has metadata, the name is extracted from it, otherwise the key is converted to string.
     *
     * @return the name of the node
     */
    default String name() {
        var meta = meta();
        if (meta != null) return meta.name();
        return key().toString();
    }

    /**
     * Get the children of this node.
     *
     * @return a map of child IDs to their respective nodes
     */
    PMap<String, NodeIdT> children();

    /**
     * Make a copy of this node with a new parent.
     *
     * @param parent the ID of the new parent node
     * @return a new TreeNode instance with the updated parent
     */
    TreeNode<TimestampT, PeerIdT, MetaT, NodeIdT> withParent(NodeIdT parent);

    /**
     * Make a copy of this node with a new last effective operation.
     *
     * @param lastEffectiveOp the new last effective operation
     * @return a new TreeNode instance with the updated last effective operation
     */
    TreeNode<TimestampT, PeerIdT, MetaT, NodeIdT> withLastEffectiveOp(OpMove<TimestampT, PeerIdT, MetaT, NodeIdT> lastEffectiveOp);

    /**
     * Make a copy of this node with new metadata.
     *
     * @param meta the new metadata
     * @return a new TreeNode instance with the updated metadata
     */
    TreeNode<TimestampT, PeerIdT, MetaT, NodeIdT> withMeta(MetaT meta);

    /**
     * Make a copy of this node with new children.
     *
     * @param children the new children
     * @return a new TreeNode instance with the updated children
     */
    TreeNode<TimestampT, PeerIdT, MetaT, NodeIdT> withChildren(PMap<String, NodeIdT> children);
}
