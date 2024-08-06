package com.usatiuk.kleppmanntree;

import java.util.Collection;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.atomic.AtomicReference;

public interface StorageInterface<
        TimestampT extends Comparable<TimestampT>,
        PeerIdT extends Comparable<PeerIdT>,
        MetaT extends NodeMeta,
        NodeIdT,
        WrapperT extends TreeNodeWrapper<TimestampT, PeerIdT, MetaT, NodeIdT>> {
    NodeIdT getRootId();

    NodeIdT getTrashId();

    NodeIdT getNewNodeId();

    WrapperT getById(NodeIdT id);

    // Creates a node, returned wrapper is RW-locked
    WrapperT createNewNode(TreeNode<TimestampT, PeerIdT, MetaT, NodeIdT> node);

    void removeNode(NodeIdT id);

    void lockSet(Collection<WrapperT> nodes);

    // It is expected that the map allows concurrent additions at the end
    NavigableMap<CombinedTimestamp<TimestampT, PeerIdT>, LogRecord<TimestampT, PeerIdT, ? extends MetaT, NodeIdT>> getLog();

    Map<PeerIdT, AtomicReference<TimestampT>> getPeerTimestampLog();
}
