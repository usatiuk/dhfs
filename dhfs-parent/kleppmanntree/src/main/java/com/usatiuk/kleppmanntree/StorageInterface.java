package com.usatiuk.kleppmanntree;

public interface StorageInterface<
        TimestampT extends Comparable<TimestampT>,
        PeerIdT extends Comparable<PeerIdT>,
        MetaT extends NodeMeta,
        NodeIdT> {
    NodeIdT getRootId();

    NodeIdT getTrashId();

    NodeIdT getNewNodeId();

    TreeNode<TimestampT, PeerIdT, MetaT, NodeIdT> getById(NodeIdT id);

    // Creates a node, returned wrapper is RW-locked
    TreeNode<TimestampT, PeerIdT, MetaT, NodeIdT> createNewNode(NodeIdT key, NodeIdT parent, MetaT meta);

    void putNode(TreeNode<TimestampT, PeerIdT, MetaT, NodeIdT> node);

    void removeNode(NodeIdT id);

    LogInterface<TimestampT, PeerIdT, MetaT, NodeIdT> getLog();

    PeerTimestampLogInterface<TimestampT, PeerIdT> getPeerTimestampLog();
}
