package com.usatiuk.kleppmanntree;

import java.util.HashMap;
import java.util.Map;

public class TestStorageInterface implements StorageInterface<Long, Long, TestNodeMeta, Long, TestNodeWrapper> {
    private final long _peerId;
    private final Map<Long, TreeNode<Long, Long, TestNodeMeta, Long>> _nodes = new HashMap<>();
    private final TestLog _log = new TestLog();
    private final TestPeerLog _peerLog = new TestPeerLog();
    private long _curId = 1;

    public TestStorageInterface(long peerId) {
        _peerId = peerId;
        _nodes.put(getRootId(), new TreeNode<>(getRootId(), null, null));
        _nodes.put(getTrashId(), new TreeNode<>(getTrashId(), null, null));
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
    public TestNodeWrapper getById(Long id) {
        var node = _nodes.get(id);
        return node == null ? null : new TestNodeWrapper(node);
    }

    @Override
    public TestNodeWrapper createNewNode(TreeNode<Long, Long, TestNodeMeta, Long> node) {
        if (!_nodes.containsKey(node.getId())) {
            _nodes.put(node.getId(), node);
            return new TestNodeWrapper(node);
        }
        throw new IllegalStateException("Node with id " + node.getId() + " already exists");
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
    public void assertRwLock() {

    }
}
