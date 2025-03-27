package com.usatiuk.kleppmanntree;

import jakarta.annotation.Nullable;
import org.pcollections.PMap;

import java.io.Serializable;

public interface TreeNode<TimestampT extends Comparable<TimestampT>, PeerIdT extends Comparable<PeerIdT>, MetaT extends NodeMeta, NodeIdT> extends Serializable {
    NodeIdT key();

    NodeIdT parent();

    OpMove<TimestampT, PeerIdT, MetaT, NodeIdT> lastEffectiveOp();

    @Nullable
    MetaT meta();

    default String name() {
        var meta = meta();
        if (meta != null) return meta.getName();
        return key().toString();
    }

    PMap<String, NodeIdT> children();

    TreeNode<TimestampT, PeerIdT, MetaT, NodeIdT> withParent(NodeIdT parent);

    TreeNode<TimestampT, PeerIdT, MetaT, NodeIdT> withLastEffectiveOp(OpMove<TimestampT, PeerIdT, MetaT, NodeIdT> lastEffectiveOp);

    TreeNode<TimestampT, PeerIdT, MetaT, NodeIdT> withMeta(MetaT meta);

    TreeNode<TimestampT, PeerIdT, MetaT, NodeIdT> withChildren(PMap<String, NodeIdT> children);
}
