package com.usatiuk.kleppmanntree;

public class TestNodeWrapper implements TreeNodeWrapper<String, TestNodeMetaDir, Long> {
    private final TreeNode<String, TestNodeMetaDir, Long> _backingNode;

    public TestNodeWrapper(TreeNode<String, TestNodeMetaDir, Long> backingNode) {_backingNode = backingNode;}

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
    public TreeNode<String, TestNodeMetaDir, Long> getNode() {
        return _backingNode;
    }
}
