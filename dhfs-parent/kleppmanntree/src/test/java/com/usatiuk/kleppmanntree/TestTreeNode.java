package com.usatiuk.kleppmanntree;

import org.pcollections.HashTreePMap;
import org.pcollections.PMap;

public record TestTreeNode(Long key, Long parent, OpMove<Long, Long, TestNodeMeta, Long> lastEffectiveOp,
                           TestNodeMeta meta,
                           PMap<String, Long> children) implements TreeNode<Long, Long, TestNodeMeta, Long> {

    public TestTreeNode(Long id, Long parent, TestNodeMeta meta) {
        this(id, parent, null, meta, HashTreePMap.empty());
    }

    @Override
    public TreeNode<Long, Long, TestNodeMeta, Long> withParent(Long parent) {
        return new TestTreeNode(key, parent, lastEffectiveOp, meta, children);
    }

    @Override
    public TreeNode<Long, Long, TestNodeMeta, Long> withLastEffectiveOp(OpMove<Long, Long, TestNodeMeta, Long> lastEffectiveOp) {
        return new TestTreeNode(key, parent, lastEffectiveOp, meta, children);
    }

    @Override
    public TreeNode<Long, Long, TestNodeMeta, Long> withMeta(TestNodeMeta meta) {
        return new TestTreeNode(key, parent, lastEffectiveOp, meta, children);
    }

    @Override
    public TreeNode<Long, Long, TestNodeMeta, Long> withChildren(PMap<String, Long> children) {
        return new TestTreeNode(key, parent, lastEffectiveOp, meta, children);
    }
}
