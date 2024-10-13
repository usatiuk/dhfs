package com.usatiuk.kleppmanntree;

public interface TreeNodeWrapper<TimestampT extends Comparable<TimestampT>, PeerIdT extends Comparable<PeerIdT>, MetaT extends NodeMeta, NodeIdT> {
    void rLock();

    void rUnlock();

    void rwLock();

    void rwUnlock();

    void freeze();

    void unfreeze();

    void notifyRef(NodeIdT id);

    void notifyRmRef(NodeIdT id);

    TreeNode<TimestampT, PeerIdT, MetaT, NodeIdT> getNode();
}
