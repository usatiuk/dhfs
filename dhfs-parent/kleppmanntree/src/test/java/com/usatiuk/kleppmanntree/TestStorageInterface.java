package com.usatiuk.kleppmanntree;

import java.util.HashMap;
import java.util.Map;

public class TestStorageInterface implements StorageInterface<Long, Long, TestNodeMeta, Long> {
    private final long _peerId;
    private final Map<Long, TestTreeNode> _nodes = new HashMap<>();
    private final TestLog _log = new TestLog();
    private final TestPeerLog _peerLog = new TestPeerLog();
    private long _curId = 1;

    public TestStorageInterface(long peerId) {
        _peerId = peerId;
        _nodes.put(getRootId(), new TestTreeNode(getRootId(), null, null));
        _nodes.put(getTrashId(), new TestTreeNode(getTrashId(), null, null));
    }

    @Override
    public Long getRootId() {
        return 0L;
    }

    @Override
    public Long getTrashId() {
        return -1L;
    }

    @Override
    public Long getNewNodeId() {
        return _curId++ | _peerId << 32;
    }

    @Override
    public TestTreeNode getById(Long id) {
        return _nodes.get(id);
    }

    @Override
    public TestTreeNode createNewNode(Long key, Long parent, TestNodeMeta meta) {
        return new TestTreeNode(key, parent, meta);
    }

    @Override
    public void putNode(TreeNode<Long, Long, TestNodeMeta, Long> node) {
        _nodes.put(node.key(), (TestTreeNode) node);
    }

    @Override
    public void removeNode(Long id) {
        if (!_nodes.containsKey(id))
            throw new IllegalStateException("Node with id " + id + " doesn't exist");
        _nodes.remove(id);
    }

    @Override
    public LogInterface<Long, Long, TestNodeMeta, Long> getLog() {
        return _log;
    }


    @Override
    public PeerTimestampLogInterface<Long, Long> getPeerTimestampLog() {
        return _peerLog;
    }
}
