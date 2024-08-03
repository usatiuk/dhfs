package com.usatiuk.kleppmanntree;

public interface TreeNodeWrapper<NameT, MetaT extends NodeMeta<NameT>, NodeIdT> {
    void rLock();

    void rUnlock();

    void rwLock();

    void rwUnlock();

    TreeNode<NameT, MetaT, NodeIdT> getNode();
}
