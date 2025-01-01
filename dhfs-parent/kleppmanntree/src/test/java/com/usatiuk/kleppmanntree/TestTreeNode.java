package com.usatiuk.kleppmanntree;

import lombok.Builder;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

@Builder(toBuilder = true)
public record TestTreeNode(Long key, Long parent, OpMove<Long, Long, TestNodeMeta, Long> lastEffectiveOp,
                           TestNodeMeta meta,
                           Map<String, Long> children) implements TreeNode<Long, Long, TestNodeMeta, Long> {

    public TestTreeNode(Long id, Long parent, TestNodeMeta meta) {
        this(id, parent, null, meta, Collections.emptyMap());
    }

    @Override
    public TreeNode<Long, Long, TestNodeMeta, Long> withParent(Long parent) {
        return this.toBuilder().parent(parent).build();
    }

    @Override
    public TreeNode<Long, Long, TestNodeMeta, Long> withLastEffectiveOp(OpMove<Long, Long, TestNodeMeta, Long> lastEffectiveOp) {
        return this.toBuilder().lastEffectiveOp(lastEffectiveOp).build();
    }

    @Override
    public TreeNode<Long, Long, TestNodeMeta, Long> withMeta(TestNodeMeta meta) {
        return this.toBuilder().meta(meta).build();
    }

    @Override
    public TreeNode<Long, Long, TestNodeMeta, Long> withChildren(Map<String, Long> children) {
        return this.toBuilder().children(children).build();
    }
}
