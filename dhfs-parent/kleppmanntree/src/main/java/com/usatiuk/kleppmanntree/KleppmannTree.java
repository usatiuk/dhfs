package com.usatiuk.kleppmanntree;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class KleppmannTree<TimestampT extends Comparable<TimestampT>, PeerIdT extends Comparable<PeerIdT>, MetaT extends NodeMeta, NodeIdT, WrapperT extends TreeNodeWrapper<MetaT, NodeIdT>> {
    private final StorageInterface<TimestampT, PeerIdT, MetaT, NodeIdT, WrapperT> _storage;
    private final PeerInterface<PeerIdT> _peers;
    private final Clock<TimestampT> _clock;
    private final OpRecorder<TimestampT, PeerIdT, MetaT, NodeIdT> _opRecorder;
    private final ReentrantReadWriteLock _lock = new ReentrantReadWriteLock();

    public KleppmannTree(StorageInterface<TimestampT, PeerIdT, MetaT, NodeIdT, WrapperT> storage,
                         PeerInterface<PeerIdT> peers,
                         Clock<TimestampT> clock,
                         OpRecorder<TimestampT, PeerIdT, MetaT, NodeIdT> opRecorder) {
        _storage = storage;
        _peers = peers;
        _clock = clock;
        _opRecorder = opRecorder;
    }


    public NodeIdT traverse(NodeIdT fromId, List<String> names) {
        if (names.isEmpty()) return fromId;

        var from = _storage.getById(fromId);
        from.rLock();
        NodeIdT childId;
        try {
            childId = from.getNode().getChildren().get(names.getFirst());
        } finally {
            from.rUnlock();
        }

        if (childId == null)
            return null;

        return traverse(childId, names.subList(1, names.size()));
    }

    public NodeIdT traverse(List<String> names) {
        return traverse(_storage.getRootId(), names);
    }

    private void assertRwLock() {
        if (!_lock.writeLock().isHeldByCurrentThread())
            throw new IllegalStateException("Expected to be write-locked here!");
    }

    private void undoOp(LogOpMove<TimestampT, PeerIdT, ? extends MetaT, NodeIdT> op) {
        assertRwLock();
        if (op.oldInfo() != null) {
            var node = _storage.getById(op.op().childId());
            var oldParent = _storage.getById(op.oldInfo().oldParent());
            var curParent = _storage.getById(op.op().newParentId());
            curParent.rwLock();
            oldParent.rwLock();
            node.rwLock();
            try {
                curParent.getNode().getChildren().remove(node.getNode().getMeta().getName());
                if (!node.getNode().getMeta().getClass().equals(op.oldInfo().oldMeta().getClass()))
                    throw new IllegalArgumentException("Class mismatch for meta for node " + node.getNode().getId());
                node.getNode().setMeta(op.oldInfo().oldMeta());
                node.getNode().setParent(oldParent.getNode().getId());
                oldParent.getNode().getChildren().put(node.getNode().getMeta().getName(), node.getNode().getId());
                node.notifyRmRef(curParent.getNode().getId());
                node.notifyRef(oldParent.getNode().getId());
            } finally {
                node.rwUnlock();
                oldParent.rwUnlock();
                curParent.rwUnlock();
            }
        } else {
            var node = _storage.getById(op.op().childId());
            var curParent = _storage.getById(op.op().newParentId());
            curParent.rwLock();
            node.rwLock();
            try {
                curParent.getNode().getChildren().remove(node.getNode().getMeta().getName());
                node.notifyRmRef(curParent.getNode().getId());
                _storage.removeNode(node.getNode().getId());
            } finally {
                node.rwUnlock();
                curParent.rwUnlock();
            }
        }
    }

    private void redoOp(Map.Entry<CombinedTimestamp<TimestampT, PeerIdT>, LogOpMove<TimestampT, PeerIdT, ? extends MetaT, NodeIdT>> entry) {
        assertRwLock();
        entry.setValue(doOp(entry.getValue().op(), false));
    }

    private <LocalMetaT extends MetaT> void doAndPut(OpMove<TimestampT, PeerIdT, LocalMetaT, NodeIdT> op) {
        var res = doOp(op, true);
        var log = _storage.getLog();
        log.put(res.op().timestamp(), res);
    }

    public <LocalMetaT extends MetaT> void applyOp(OpMove<TimestampT, PeerIdT, LocalMetaT, NodeIdT> op) {
        _clock.updateTimestamp(op.timestamp().timestamp());
        var log = _storage.getLog();

        int cmp;

        _lock.readLock().lock();
        try {
            if (log.isEmpty()) {
                // doOp can't be a move here, otherwise we deadlock
                doAndPut(op);
                return;
            }
            cmp = op.timestamp().compareTo(log.lastEntry().getKey());
        } finally {
            _lock.readLock().unlock();
        }

        if (log.containsKey(op.timestamp())) return;
        assert cmp != 0;
        if (cmp < 0) {
            _lock.writeLock().lock();
            try {
                if (log.containsKey(op.timestamp())) return;
                var toUndo = log.tailMap(op.timestamp(), false);
                for (var entry : toUndo.reversed().entrySet()) {
                    undoOp(entry.getValue());
                }
                doAndPut(op);
                for (var entry : toUndo.entrySet()) {
                    redoOp(entry);
                }
            } finally {
                _lock.writeLock().unlock();
            }
        } else {
            doAndPut(op);
        }
    }

    public CombinedTimestamp<TimestampT, PeerIdT> getTimestamp() {
        return new CombinedTimestamp<>(_clock.getTimestamp(), _peers.getSelfId());
    }

    public <LocalMetaT extends MetaT> OpMove<TimestampT, PeerIdT, LocalMetaT, NodeIdT> createMove(NodeIdT newParent, LocalMetaT newMeta, NodeIdT node) {
        return new OpMove<>(getTimestamp(), newParent, newMeta, node);
    }

    private <LocalMetaT extends MetaT> LogOpMove<TimestampT, PeerIdT, LocalMetaT, NodeIdT> doOp(OpMove<TimestampT, PeerIdT, LocalMetaT, NodeIdT> op, boolean record) {
        var node = _storage.getById(op.childId());

        var oldParent = (node != null && node.getNode().getParent() != null) ? _storage.getById(node.getNode().getParent()) : null;
        var newParent = _storage.getById(op.newParentId());

        if (newParent == null) {
            throw new IllegalArgumentException("New parent not found");
        }

        if (oldParent == null) {
            newParent.rwLock();
            try {
                node = _storage.createNewNode(new TreeNode<>(op.childId(), op.newParentId(), op.newMeta()));
                try {
                    // TODO: Handle conflicts
                    newParent.getNode().getChildren().put(node.getNode().getMeta().getName(), node.getNode().getId());
                    node.notifyRef(newParent.getNode().getId());
                    _opRecorder.recordOp(op);
                    return new LogOpMove<>(null, op);
                } finally {
                    node.rwUnlock();
                }
            } finally {
                newParent.rwUnlock();
            }
        }

        // FIXME:
        _lock.writeLock().lock();
        try {
            if (op.childId() == op.newParentId() || isAncestor(op.childId(), op.newParentId())) {
                _opRecorder.recordOp(op);
                return new LogOpMove<>(null, op);
            }

            var trash = _storage.getById(_storage.getTrashId());
            trash.rwLock();
            newParent.rwLock();
            oldParent.rwLock();
            node.rwLock();

            try {
                oldParent.getNode().getChildren().remove(node.getNode().getMeta().getName());
                var oldMeta = node.getNode().getMeta();
                if (!node.getNode().getMeta().getClass().equals(op.newMeta().getClass()))
                    throw new IllegalArgumentException("Class mismatch for meta for node " + node.getNode().getId());
                node.getNode().setMeta(op.newMeta());
                node.getNode().setParent(newParent.getNode().getId());
                var old = newParent.getNode().getChildren().get(op.newMeta().getName());
                if (old != null) {
                    var oldNode = _storage.getById(old);
                    try {
                        oldNode.rwLock();
                        trash.getNode().getChildren().put(oldNode.getNode().getId().toString(), oldNode.getNode().getId());
                        oldNode.notifyRmRef(newParent.getNode().getId());
                        oldNode.notifyRef(trash.getNode().getId());
                    } finally {
                        oldNode.rwUnlock();
                    }
                }
                newParent.getNode().getChildren().put(op.newMeta().getName(), node.getNode().getId());
                node.notifyRmRef(oldParent.getNode().getId());
                node.notifyRef(newParent.getNode().getId());
                _opRecorder.recordOp(op);
                return new LogOpMove<>(new LogOpMoveOld<>(oldParent.getNode().getId(), (LocalMetaT) oldMeta), op);
            } finally {
                node.rwUnlock();
                oldParent.rwUnlock();
                newParent.rwUnlock();
                trash.rwUnlock();
            }
        } finally {
            _lock.writeLock().unlock();
        }
    }

    private boolean isAncestor(NodeIdT child, NodeIdT parent) {
        var node = _storage.getById(parent);
        NodeIdT curParent = null;
        while ((curParent = node.getNode().getParent()) != null) {
            if (Objects.equals(child, curParent)) return true;
            node = _storage.getById(curParent);
        }
        return false;
    }
}
