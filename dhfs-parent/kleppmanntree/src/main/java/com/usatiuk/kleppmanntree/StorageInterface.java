package com.usatiuk.kleppmanntree;

import java.util.Collection;
import java.util.NavigableMap;

public interface StorageInterface<
        TimestampT extends Comparable<TimestampT>,
        PeerIdT extends Comparable<PeerIdT>,
        NameT,
        MetaT extends NodeMeta<NameT>,
        NodeIdT,
        WrapperT extends TreeNodeWrapper<NameT, MetaT, NodeIdT>> {
    NodeIdT getRootId();

    NodeIdT getTrashId();

    NodeIdT getNewNodeId();

    WrapperT getById(NodeIdT id);

    // Creates a node, returned wrapper is RW-locked
    WrapperT createNewNode(TreeNode<NameT, MetaT, NodeIdT> node);

    void removeNode(NodeIdT id);

    void lockSet(Collection<WrapperT> nodes);

    // It is expected that the map allows concurrent additions at the end
    NavigableMap<CombinedTimestamp<TimestampT, PeerIdT>, LogOpMove<TimestampT, PeerIdT, NameT, ? extends MetaT, NodeIdT>> getLog();

    // Locks all the objects from being changed
    void globalRwLock();

    void globalRwUnlock();

    // Locks all the objects from being changed
    void globalRLock();

    void globalRUnlock();
}
