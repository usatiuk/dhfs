package com.usatiuk.kleppmanntree;

public interface TreeNodeWrapper<MetaT extends NodeMeta, NodeIdT> {
    void rLock();

    void rUnlock();

    void rwLock();

    void rwUnlock();

    void notifyRef(NodeIdT id);

    void notifyRmRef(NodeIdT id);

    TreeNode<MetaT, NodeIdT> getNode();
}
