package com.usatiuk.kleppmanntree;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

public class TestStorageInterface implements StorageInterface<Long, Long, TestNodeMeta, Long, TestNodeWrapper> {
    private long _curId = 1;
    private final long _peerId;

    private final Map<Long, TreeNode<Long, Long, TestNodeMeta, Long>> _nodes = new HashMap<>();
    private final NavigableMap<CombinedTimestamp<Long, Long>, LogRecord<Long, Long, TestNodeMeta, Long>> _log = new TreeMap<>();
    private final Map<Long, AtomicReference<Long>> _peerTimestampLog = new HashMap<>();

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
    public void lockSet(Collection<TestNodeWrapper> nodes) {

    }

    @Override
    public NavigableMap<CombinedTimestamp<Long, Long>, LogRecord<Long, Long, TestNodeMeta, Long>> getLog() {
        return _log;
    }

    @Override
    public Map<Long, AtomicReference<Long>> getPeerTimestampLog() {
        return _peerTimestampLog;
    }
}
