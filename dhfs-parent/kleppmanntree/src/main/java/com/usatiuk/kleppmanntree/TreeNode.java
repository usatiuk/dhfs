package com.usatiuk.kleppmanntree;

import lombok.Getter;
import lombok.Setter;

import java.util.HashMap;
import java.util.Map;

@Getter
@Setter
public class TreeNode<TimestampT extends Comparable<TimestampT>, PeerIdT extends Comparable<PeerIdT>, MetaT extends NodeMeta, NodeIdT> {
    private final NodeIdT _id;
    private NodeIdT _parent = null;
    private OpMove<TimestampT, PeerIdT, MetaT, NodeIdT> _lastEffectiveOp = null;
    private MetaT _meta = null;
    private Map<String, NodeIdT> _children = new HashMap<>();

    public TreeNode(NodeIdT id, NodeIdT parent, MetaT meta) {
        _id = id;
        _meta = meta;
        _parent = parent;
    }

    public TreeNode(NodeIdT id, NodeIdT parent, MetaT meta, Map<String, NodeIdT> children) {
        _id = id;
        _meta = meta;
        _parent = parent;
        _children = children;
    }

}
