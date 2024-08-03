package com.usatiuk.kleppmanntree;

public class TestNodeWrapper implements TreeNodeWrapper<TestNodeMeta, Long> {
    private final TreeNode<TestNodeMeta, Long> _backingNode;

    public TestNodeWrapper(TreeNode<TestNodeMeta, Long> backingNode) {_backingNode = backingNode;}

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
    public void notifyRef(Long id) {

    }

    @Override
    public void notifyRmRef(Long id) {

    }

    @Override
    public TreeNode<TestNodeMeta, Long> getNode() {
        return _backingNode;
    }
}
