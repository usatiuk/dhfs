package com.usatiuk.kleppmanntree;

import java.util.*;

public class TestStorageInterface implements StorageInterface<Long, Long, String, TestNodeMetaDir, Long, TestNodeWrapper> {
    private long _curId = 1;
    private final long _peerId;

    private final Map<Long, TreeNode<String, TestNodeMetaDir, Long>> _nodes = new HashMap<>();
    private final NavigableMap<CombinedTimestamp<Long, Long>, LogOpMove<Long, Long, String, TestNodeMetaDir, Long>> _log = new TreeMap<>();

    public TestStorageInterface(long peerId) {
        _peerId = peerId;
        _nodes.put(getRootId(), new TreeNode<>(getRootId()));
        _nodes.put(getTrashId(), new TreeNode<>(getTrashId()));
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
    public TestNodeWrapper createNewNode(Long id) {
        if (!_nodes.containsKey(id)) {
            var newNode = new TreeNode<String, TestNodeMetaDir, Long>(id);
            _nodes.put(id, newNode);
            return new TestNodeWrapper(newNode);
        }
        throw new IllegalStateException("Node with id " + id + " already exists");
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
    public NavigableMap<CombinedTimestamp<Long, Long>, LogOpMove<Long, Long, String, TestNodeMetaDir, Long>> getLog() {
        return _log;
    }

    @Override
    public void globalLock() {

    }

    @Override
    public void globalUnlock() {

    }
}
