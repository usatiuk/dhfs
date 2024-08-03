package com.usatiuk.kleppmanntree;

public class TestNodeWrapper implements TreeNodeWrapper<String, TestNodeMeta, Long> {
    private final TreeNode<String, TestNodeMeta, Long> _backingNode;

    public TestNodeWrapper(TreeNode<String, TestNodeMeta, Long> backingNode) {_backingNode = backingNode;}

    @Override
    public void rLock() {

    }

    @Override
    public void rUnlock() {

    }

    @Override
    public void rwLock() {

    }

    @Override
    public void rwUnlock() {

    }

    @Override
    public TreeNode<String, TestNodeMeta, Long> getNode() {
        return _backingNode;
    }
}
