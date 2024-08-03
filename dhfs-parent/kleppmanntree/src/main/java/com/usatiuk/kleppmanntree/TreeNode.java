package com.usatiuk.kleppmanntree;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

@Getter
@Setter
public class TreeNode<NameT, MetaT extends NodeMeta<NameT>, NodeIdT> implements Serializable {
    private NodeIdT _parent = null;
    private final NodeIdT _id;
    private MetaT _meta = null;
    private Map<NameT, NodeIdT> _children = new HashMap<>();

    public TreeNode(NodeIdT id, NodeIdT parent, MetaT meta) {
        _id = id;
        _meta = meta;
        _parent = parent;
    }
}
