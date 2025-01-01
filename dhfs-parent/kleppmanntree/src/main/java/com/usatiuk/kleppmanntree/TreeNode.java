package com.usatiuk.kleppmanntree;

import java.io.Serializable;
import java.util.Map;

public interface TreeNode<TimestampT extends Comparable<TimestampT>, PeerIdT extends Comparable<PeerIdT>, MetaT extends NodeMeta, NodeIdT> extends Serializable {
    NodeIdT key();

    NodeIdT parent();

    OpMove<TimestampT, PeerIdT, MetaT, NodeIdT> lastEffectiveOp();

    MetaT meta();

    Map<String, NodeIdT> children();

    TreeNode<TimestampT, PeerIdT, MetaT, NodeIdT> withParent(NodeIdT parent);

    TreeNode<TimestampT, PeerIdT, MetaT, NodeIdT> withLastEffectiveOp(OpMove<TimestampT, PeerIdT, MetaT, NodeIdT> lastEffectiveOp);

    TreeNode<TimestampT, PeerIdT, MetaT, NodeIdT> withMeta(MetaT meta);

    TreeNode<TimestampT, PeerIdT, MetaT, NodeIdT> withChildren(Map<String, NodeIdT> children);
}
