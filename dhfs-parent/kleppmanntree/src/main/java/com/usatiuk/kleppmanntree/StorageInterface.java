package com.usatiuk.kleppmanntree;

public interface StorageInterface<
        TimestampT extends Comparable<TimestampT>,
        PeerIdT extends Comparable<PeerIdT>,
        MetaT extends NodeMeta,
        NodeIdT,
        WrapperT extends TreeNodeWrapper<TimestampT, PeerIdT, MetaT, NodeIdT>> {
    NodeIdT getRootId();

    NodeIdT getTrashId();

    NodeIdT getNewNodeId();

    WrapperT getById(NodeIdT id);

    // Creates a node, returned wrapper is RW-locked
    WrapperT createNewNode(TreeNode<TimestampT, PeerIdT, MetaT, NodeIdT> node);

    void removeNode(NodeIdT id);

    LogInterface<TimestampT, PeerIdT, MetaT, NodeIdT> getLog();

    PeerTimestampLogInterface<TimestampT, PeerIdT> getPeerTimestampLog();
}
