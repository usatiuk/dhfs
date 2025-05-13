package com.usatiuk.kleppmanntree;

/**
 * Storage interface for the Kleppmann tree.
 *
 * @param <TimestampT> the type of the timestamp
 * @param <PeerIdT>    the type of the peer ID
 * @param <MetaT>      the type of the node metadata
 * @param <NodeIdT>    the type of the node ID
 */
public interface StorageInterface<
        TimestampT extends Comparable<TimestampT>,
        PeerIdT extends Comparable<PeerIdT>,
        MetaT extends NodeMeta,
        NodeIdT> {
    /**
     * Get the root node ID.
     *
     * @return the root node IDx
     */
    NodeIdT getRootId();

    /**
     * Get the trash node ID.
     *
     * @return the trash node ID
     */
    NodeIdT getTrashId();

    /**
     * Get the lost and found node ID.
     *
     * @return the lost and found node ID
     */
    NodeIdT getLostFoundId();

    /**
     * Get the new node ID.
     *
     * @return the new node ID
     */
    NodeIdT getNewNodeId();

    /**
     * Get the node by its ID.
     *
     * @param id the ID of the node
     * @return the node with the specified ID, or null if not found
     */
    TreeNode<TimestampT, PeerIdT, MetaT, NodeIdT> getById(NodeIdT id);

    /**
     * Create a new node with the specified key, parent, and metadata.
     *
     * @param key    the ID of the new node
     * @param parent the ID of the parent node
     * @param meta   the metadata of the new node
     * @return the new node
     */
    TreeNode<TimestampT, PeerIdT, MetaT, NodeIdT> createNewNode(NodeIdT key, NodeIdT parent, MetaT meta);

    /**
     * Put a node into the storage.
     *
     * @param node the node to put into the storage
     */
    void putNode(TreeNode<TimestampT, PeerIdT, MetaT, NodeIdT> node);

    /**
     * Remove a node from the storage.
     *
     * @param id the ID of the node to remove
     */
    void removeNode(NodeIdT id);

    /**
     * Get the log interface.
     *
     * @return the log interface
     */
    LogInterface<TimestampT, PeerIdT, MetaT, NodeIdT> getLog();

    /**
     * Get the peer timestamp log interface.
     *
     * @return the peer timestamp log interface
     */
    PeerTimestampLogInterface<TimestampT, PeerIdT> getPeerTimestampLog();
}
