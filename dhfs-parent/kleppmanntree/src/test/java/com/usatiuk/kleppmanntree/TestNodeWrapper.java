package com.usatiuk.kleppmanntree;

public class TestNodeWrapper implements TreeNodeWrapper<Long, Long, TestNodeMeta, Long> {
    private final TreeNode<Long, Long, TestNodeMeta, Long> _backingNode;

    public TestNodeWrapper(TreeNode<Long, Long, TestNodeMeta, Long> backingNode) {_backingNode = backingNode;}

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
    public void freeze() {

    }

    @Override
    public void unfreeze() {

    }

    @Override
    public void notifyRef(Long id) {

    }

    @Override
    public void notifyRmRef(Long id) {

    }

    @Override
    public TreeNode<Long, Long, TestNodeMeta, Long> getNode() {
        return _backingNode;
    }
}
