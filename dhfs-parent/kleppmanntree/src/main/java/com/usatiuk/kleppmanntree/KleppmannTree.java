package com.usatiuk.kleppmanntree;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;

public class KleppmannTree<TimestampT extends Comparable<TimestampT>, PeerIdT extends Comparable<PeerIdT>, MetaT extends NodeMeta, NodeIdT, WrapperT extends TreeNodeWrapper<TimestampT, PeerIdT, MetaT, NodeIdT>> {
    private final StorageInterface<TimestampT, PeerIdT, MetaT, NodeIdT, WrapperT> _storage;
    private final PeerInterface<PeerIdT> _peers;
    private final Clock<TimestampT> _clock;
    private final OpRecorder<TimestampT, PeerIdT, MetaT, NodeIdT> _opRecorder;
    private static final Logger LOGGER = Logger.getLogger(KleppmannTree.class.getName());

    private final ReentrantReadWriteLock _lock = new ReentrantReadWriteLock();

    private void assertRwLock() {
        assert _lock.isWriteLockedByCurrentThread();
    }

    public KleppmannTree(StorageInterface<TimestampT, PeerIdT, MetaT, NodeIdT, WrapperT> storage,
                         PeerInterface<PeerIdT> peers,
                         Clock<TimestampT> clock,
                         OpRecorder<TimestampT, PeerIdT, MetaT, NodeIdT> opRecorder) {
        _storage = storage;
        _peers = peers;
        _clock = clock;
        _opRecorder = opRecorder;
    }

    private NodeIdT traverseImpl(NodeIdT fromId, List<String> names) {
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

        return traverseImpl(childId, names.subList(1, names.size()));
    }

    public NodeIdT traverse(NodeIdT fromId, List<String> names) {
        _lock.readLock().lock();
        try {
            return traverseImpl(fromId, names.subList(1, names.size()));
        } finally {
            _lock.readLock().unlock();
        }
    }

    public NodeIdT traverse(List<String> names) {
        _lock.readLock().lock();
        try {
            return traverseImpl(_storage.getRootId(), names);
        } finally {
            _lock.readLock().unlock();
        }
    }

    private HashMap<NodeIdT, WrapperT> _undoCtx = null;

    private void undoEffect(LogEffect<TimestampT, PeerIdT, ? extends MetaT, NodeIdT> effect) {
        assertRwLock();
        if (effect.oldInfo() != null) {
            var node = _storage.getById(effect.childId());
            var oldParent = _storage.getById(effect.oldInfo().oldParent());
            var curParent = _storage.getById(effect.newParentId());
            curParent.rwLock();
            oldParent.rwLock();
            node.rwLock();
            try {
                curParent.getNode().getChildren().remove(node.getNode().getMeta().getName());
                if (!node.getNode().getMeta().getClass().equals(effect.oldInfo().oldMeta().getClass()))
                    throw new IllegalArgumentException("Class mismatch for meta for node " + node.getNode().getId());
                node.getNode().setMeta(effect.oldInfo().oldMeta());
                node.getNode().setParent(oldParent.getNode().getId());
                oldParent.getNode().getChildren().put(node.getNode().getMeta().getName(), node.getNode().getId());
                node.notifyRmRef(curParent.getNode().getId());
                node.notifyRef(oldParent.getNode().getId());
                node.getNode().setLastMoveTimestamp(effect.oldInfo().oldTimestamp());
            } finally {
                node.rwUnlock();
                oldParent.rwUnlock();
                curParent.rwUnlock();
            }
        } else {
            var node = _storage.getById(effect.childId());
            var curParent = _storage.getById(effect.newParentId());
            curParent.rwLock();
            node.rwLock();
            try {
                curParent.getNode().getChildren().remove(node.getNode().getMeta().getName());
                node.lock();
                node.getNode().setParent(null);
                node.getNode().setLastMoveTimestamp(null);
                node.notifyRmRef(curParent.getNode().getId());
                _undoCtx.put(node.getNode().getId(), node);
            } finally {
                node.rwUnlock();
                curParent.rwUnlock();
            }
        }
    }

    private void undoOp(LogRecord<TimestampT, PeerIdT, ? extends MetaT, NodeIdT> op) {
        for (var e : op.effects().reversed())
            undoEffect(e);
    }

    private void redoOp(Map.Entry<CombinedTimestamp<TimestampT, PeerIdT>, LogRecord<TimestampT, PeerIdT, ? extends MetaT, NodeIdT>> entry) {
        entry.setValue(doOp(entry.getValue().op(), false));
    }

    private <LocalMetaT extends MetaT> void doAndPut(OpMove<TimestampT, PeerIdT, LocalMetaT, NodeIdT> op, boolean failIfExists) {
        assertRwLock();
        var res = doOp(op, failIfExists);
        _storage.getLog().put(res.op().timestamp(), res);
    }

    private void tryTrimLog() {
        var log = _storage.getLog();
        var timeLog = _storage.getPeerTimestampLog();
        TimestampT min = null;
        for (var e : _peers.getAllPeers()) {
            var got = timeLog.get(e);
            if (got == null || got.get() == null) return;
            var gotNum = got.get();
            if (min == null) {
                min = gotNum;
                continue;
            }
            if (gotNum.compareTo(min) < 0)
                min = gotNum;
        }
        if (min == null) return;

        var canTrim = log.headMap(new CombinedTimestamp<>(min, null), true);
        if (!canTrim.isEmpty()) {
            canTrim = log.headMap(new CombinedTimestamp<>(min, null), true);

            Set<NodeIdT> inTrash = new HashSet<>();

            for (var le : canTrim.values()) {
                for (var e : le.effects()) {
                    if (Objects.equals(e.newParentId(), _storage.getTrashId())) {
                        inTrash.add(e.childId());
                    } else {
                        inTrash.remove(e.childId());
                    }
                }
            }

            canTrim.clear();

            if (!inTrash.isEmpty()) {
                var trash = _storage.getById(_storage.getTrashId());
                trash.rwLock();
                try {
                    for (var n : inTrash) {
                        var node = _storage.getById(n);
                        node.rwLock();
                        try {
                            trash.getNode().getChildren().remove(node.getNode().getMeta().getName());
                            node.notifyRmRef(trash.getNode().getId());
                        } finally {
                            node.rwUnlock();
                        }
                        _storage.removeNode(n);
                    }
                } finally {
                    trash.rwUnlock();
                }
            }
        }
    }

    public <LocalMetaT extends MetaT> void move(NodeIdT newParent, LocalMetaT newMeta, NodeIdT child) {
        move(newParent, newMeta, child, true);
    }

    public <LocalMetaT extends MetaT> void move(NodeIdT newParent, LocalMetaT newMeta, NodeIdT child, boolean failIfExists) {
        _lock.writeLock().lock();
        try {
            var createdMove = createMove(newParent, newMeta, child);
            _opRecorder.recordOp(createdMove);
            applyOp(_peers.getSelfId(), createdMove, failIfExists);
        } finally {
            _lock.writeLock().unlock();
        }
    }

    public void applyExternalOp(PeerIdT from, OpMove<TimestampT, PeerIdT, ? extends MetaT, NodeIdT> op) {
        _lock.writeLock().lock();
        try {
            _clock.updateTimestamp(op.timestamp().timestamp());
            applyOp(from, op, false);
        } finally {
            _lock.writeLock().unlock();
        }
    }

    private void applyOp(PeerIdT from, OpMove<TimestampT, PeerIdT, ? extends MetaT, NodeIdT> op, boolean failIfExists) {
        assertRwLock();
        synchronized (this) {
            var ref = _storage.getPeerTimestampLog().computeIfAbsent(from, f -> new AtomicReference<>());

            // TODO: I guess it's not actually needed since one peer can't handle concurrent updates?
            TimestampT oldRef;
            TimestampT newRef;
            do {
                oldRef = ref.get();
                if (oldRef != null && oldRef.compareTo(op.timestamp().timestamp()) >= 0)
                    throw new IllegalArgumentException("Wrong op order: received older than known from " + from.toString());
                newRef = op.timestamp().timestamp();
            } while (!ref.compareAndSet(oldRef, newRef));

            var log = _storage.getLog();

            // FIXME: hack?
            int cmp = log.isEmpty() ? 1 : op.timestamp().compareTo(log.lastEntry().getKey());

            if (log.containsKey(op.timestamp())) {
                tryTrimLog();
                return;
            }
            assert cmp != 0;
            if (cmp < 0) {
                try {
                    if (log.containsKey(op.timestamp())) return;
                    var toUndo = log.tailMap(op.timestamp(), false);
                    _undoCtx = new HashMap<>();
                    for (var entry : toUndo.reversed().entrySet()) {
                        undoOp(entry.getValue());
                    }
                    try {
                        doAndPut(op, failIfExists);
                    } finally {
                        for (var entry : toUndo.entrySet()) {
                            redoOp(entry);
                        }

                        if (!_undoCtx.isEmpty()) {
                            for (var e : _undoCtx.entrySet()) {
                                LOGGER.log(Level.FINE, "Dropping node " + e.getKey());
                                e.getValue().unlock();
                                _storage.removeNode(e.getKey());
                            }
                        }
                        _undoCtx = null;
                    }
                } finally {
                    tryTrimLog();
                }
            } else {
                doAndPut(op, failIfExists);
                tryTrimLog();
            }
        }
    }

    private CombinedTimestamp<TimestampT, PeerIdT> getTimestamp() {
        assertRwLock();
        return new CombinedTimestamp<>(_clock.getTimestamp(), _peers.getSelfId());
    }

    private <LocalMetaT extends MetaT> OpMove<TimestampT, PeerIdT, LocalMetaT, NodeIdT> createMove(NodeIdT newParent, LocalMetaT newMeta, NodeIdT node) {
        assertRwLock();
        return new OpMove<>(getTimestamp(), newParent, newMeta, node);
    }

    private LogRecord<TimestampT, PeerIdT, MetaT, NodeIdT> doOp(OpMove<TimestampT, PeerIdT, ? extends MetaT, NodeIdT> op, boolean failIfExists) {
        LogRecord<TimestampT, PeerIdT, MetaT, NodeIdT> computed;
        try {
            computed = computeEffects(op, failIfExists);
        } catch (AlreadyExistsException aex) {
            throw aex;
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Error computing effects for op" + op.toString(), e);
            computed = new LogRecord<>(op, null);
        }

        if (computed.effects() != null)
            applyEffects(op.timestamp(), computed.effects());
        return computed;
    }

    private WrapperT getNewNode(TreeNode<TimestampT, PeerIdT, MetaT, NodeIdT> desired) {
        assertRwLock();
        if (_undoCtx != null) {
            var node = _undoCtx.get(desired.getId());
            if (node != null) {
                node.rwLock();
                try {
                    if (!node.getNode().getChildren().isEmpty()) {
                        LOGGER.log(Level.WARNING, "Not empty children for undone node " + desired.getId());
                        assert node.getNode().getChildren().isEmpty();
                        node.getNode().getChildren().clear();
                    }
                    node.getNode().setParent(desired.getParent());
                    node.notifyRef(desired.getParent());
                    node.getNode().setMeta(desired.getMeta());
                    node.unlock();
                } catch (Exception e) {
                    LOGGER.log(Level.SEVERE, "Error while fixing up node " + desired.getId(), e);
                    node.rwUnlock();
                    node = null;
                }
            }
            if (node != null) {
                _undoCtx.remove(desired.getId());
                return node;
            }
        }
        return _storage.createNewNode(desired);
    }

    private void applyEffects(CombinedTimestamp<TimestampT, PeerIdT> opTimestamp, List<LogEffect<TimestampT, PeerIdT, ? extends MetaT, NodeIdT>> effects) {
        assertRwLock();
        for (var effect : effects) {
            WrapperT oldParentNode = null;
            WrapperT newParentNode;
            WrapperT node;

            newParentNode = _storage.getById(effect.newParentId());
            newParentNode.rwLock();
            try {
                if (effect.oldInfo() != null) {
                    oldParentNode = _storage.getById(effect.oldInfo().oldParent());
                    oldParentNode.rwLock();
                }
                try {
                    if (oldParentNode == null) {
                        node = getNewNode(new TreeNode<>(effect.childId(), effect.newParentId(), effect.newMeta()));
                    } else {
                        node = _storage.getById(effect.childId());
                        node.rwLock();
                    }
                    try {

                        if (oldParentNode != null) {
                            oldParentNode.getNode().getChildren().remove(effect.oldInfo().oldMeta().getName());
                            node.notifyRmRef(effect.oldInfo().oldParent());
                        }

                        newParentNode.getNode().getChildren().put(effect.newMeta().getName(), effect.childId());
                        node.getNode().setParent(effect.newParentId());
                        node.getNode().setMeta(effect.newMeta());
                        node.getNode().setLastMoveTimestamp(opTimestamp);
                        node.notifyRef(effect.newParentId());

                    } finally {
                        node.rwUnlock();
                    }
                } finally {
                    if (oldParentNode != null)
                        oldParentNode.rwUnlock();
                }
            } finally {
                newParentNode.rwUnlock();
            }
        }
    }

    private <LocalMetaT extends MetaT> LogRecord<TimestampT, PeerIdT, MetaT, NodeIdT> computeEffects(OpMove<TimestampT, PeerIdT, LocalMetaT, NodeIdT> op, boolean failIfExists) {
        assertRwLock();
        var node = _storage.getById(op.childId());

        NodeIdT oldParentId = (node != null && node.getNode().getParent() != null) ? node.getNode().getParent() : null;
        NodeIdT newParentId = op.newParentId();
        WrapperT newParent = _storage.getById(newParentId);

        if (newParent == null) {
            LOGGER.log(Level.SEVERE, "New parent not found " + op.newMeta().getName() + " " + op.childId());
            return new LogRecord<>(op, null);
        }

        if (oldParentId == null) {
            newParent.rLock();
            try {
                var conflictNodeId = newParent.getNode().getChildren().get(op.newMeta().getName());

                if (conflictNodeId != null) {
                    if (failIfExists)
                        throw new AlreadyExistsException("Already exists: " + op.newMeta().getName() + ": " + conflictNodeId);

                    var conflictNode = _storage.getById(conflictNodeId);
                    conflictNode.rLock();
                    try {
                        MetaT conflictNodeMeta = conflictNode.getNode().getMeta();
                        String newConflictNodeName = conflictNodeMeta.getName() + ".conflict." + conflictNode.getNode().getId();
                        String newOursName = op.newMeta().getName() + ".conflict." + op.childId();
                        return new LogRecord<>(op, List.of(
                                new LogEffect<>(new LogEffectOld<>(conflictNode.getNode().getLastMoveTimestamp(), newParentId, conflictNodeMeta), newParentId, (MetaT) conflictNodeMeta.withName(newConflictNodeName), conflictNodeId),
                                new LogEffect<>(null, op.newParentId(), (MetaT) op.newMeta().withName(newOursName), op.childId())
                                                          ));
                    } finally {
                        conflictNode.rUnlock();
                    }
                } else {
                    return new LogRecord<>(op, List.of(
                            new LogEffect<>(null, newParentId, op.newMeta(), op.childId())
                                                      ));
                }
            } finally {
                newParent.rUnlock();
            }
        }

        if (Objects.equals(op.childId(), op.newParentId()) || isAncestor(op.childId(), op.newParentId())) {
            return new LogRecord<>(op, null);
        }

        node.rLock();
        newParent.rLock();
        try {
            MetaT oldMeta = node.getNode().getMeta();
            if (!oldMeta.getClass().equals(op.newMeta().getClass())) {
                LOGGER.log(Level.SEVERE, "Class mismatch for meta for node " + node.getNode().getId());
                return new LogRecord<>(op, null);
            }
            var replaceNodeId = newParent.getNode().getChildren().get(op.newMeta().getName());
            if (replaceNodeId != null) {
                var replaceNode = _storage.getById(replaceNodeId);
                try {
                    replaceNode.rLock();
                    var replaceNodeMeta = replaceNode.getNode().getMeta();
                    return new LogRecord<>(op, List.of(
                            new LogEffect<>(new LogEffectOld<>(replaceNode.getNode().getLastMoveTimestamp(), newParentId, replaceNodeMeta), _storage.getTrashId(), (MetaT) replaceNodeMeta.withName(replaceNodeId.toString()), replaceNodeId),
                            new LogEffect<>(new LogEffectOld<>(node.getNode().getLastMoveTimestamp(), oldParentId, oldMeta), op.newParentId(), op.newMeta(), op.childId())
                                                      ));
                } finally {
                    replaceNode.rUnlock();
                }
            }
            return new LogRecord<>(op, List.of(
                    new LogEffect<>(new LogEffectOld<>(node.getNode().getLastMoveTimestamp(), oldParentId, oldMeta), op.newParentId(), op.newMeta(), op.childId())
                                              ));
        } finally {
            newParent.rUnlock();
            node.rUnlock();
        }
    }

    private boolean isAncestor(NodeIdT child, NodeIdT parent) {
        var node = _storage.getById(parent);
        NodeIdT curParent;
        while ((curParent = node.getNode().getParent()) != null) {
            if (Objects.equals(child, curParent)) return true;
            node = _storage.getById(curParent);
        }
        return false;
    }
}
