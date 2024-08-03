package com.usatiuk.kleppmanntree;

public interface TreeNodeWrapper<NameT, MetaT extends NodeMeta<NameT>, NodeIdT> {
    void rLock();

    void rUnlock();

    void rwLock();

    void rwUnlock();

    void notifyRef(NodeIdT id);

    void notifyRmRef(NodeIdT id);

    TreeNode<NameT, MetaT, NodeIdT> getNode();
}
