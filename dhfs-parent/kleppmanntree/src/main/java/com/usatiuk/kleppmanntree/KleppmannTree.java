package com.usatiuk.kleppmanntree;

import org.apache.commons.lang3.tuple.Pair;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;

public class KleppmannTree<TimestampT extends Comparable<TimestampT>, PeerIdT extends Comparable<PeerIdT>, MetaT extends NodeMeta, NodeIdT, WrapperT extends TreeNodeWrapper<TimestampT, PeerIdT, MetaT, NodeIdT>> {
    private static final Logger LOGGER = Logger.getLogger(KleppmannTree.class.getName());
    private final StorageInterface<TimestampT, PeerIdT, MetaT, NodeIdT, WrapperT> _storage;
    private final PeerInterface<PeerIdT> _peers;
    private final Clock<TimestampT> _clock;
    private final OpRecorder<TimestampT, PeerIdT, MetaT, NodeIdT> _opRecorder;
    private HashMap<NodeIdT, WrapperT> _undoCtx = null;

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
        _storage.rLock();
        try {
            return traverseImpl(fromId, names.subList(1, names.size()));
        } finally {
            _storage.rUnlock();
        }
    }

    public NodeIdT traverse(List<String> names) {
        _storage.rLock();
        try {
            return traverseImpl(_storage.getRootId(), names);
        } finally {
            _storage.rUnlock();
        }
    }

    private void undoEffect(LogEffect<TimestampT, PeerIdT, MetaT, NodeIdT> effect) {
        _storage.assertRwLock();
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
                node.getNode().setLastEffectiveOp(effect.oldInfo().oldEffectiveMove());
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
                node.getNode().setLastEffectiveOp(null);
                node.notifyRmRef(curParent.getNode().getId());
                _undoCtx.put(node.getNode().getId(), node);
            } finally {
                node.rwUnlock();
                curParent.rwUnlock();
            }
        }
    }

    private void undoOp(LogRecord<TimestampT, PeerIdT, MetaT, NodeIdT> op) {
        for (var e : op.effects().reversed())
            undoEffect(e);
    }

    private void redoOp(Map.Entry<CombinedTimestamp<TimestampT, PeerIdT>, LogRecord<TimestampT, PeerIdT, MetaT, NodeIdT>> entry) {
        var newEffects = doOp(entry.getValue().op(), false);
        _storage.getLog().replace(entry.getKey(), newEffects);
    }

    private void doAndPut(OpMove<TimestampT, PeerIdT, MetaT, NodeIdT> op, boolean failCreatingIfExists) {
        _storage.assertRwLock();
        var res = doOp(op, failCreatingIfExists);
        _storage.getLog().put(res.op().timestamp(), res);
    }

    private void tryTrimLog() {
        var log = _storage.getLog();
        var timeLog = _storage.getPeerTimestampLog();
        TimestampT min = null;
        for (var e : _peers.getAllPeers()) {
            var got = timeLog.getForPeer(e);
            if (got == null) return;
            if (min == null) {
                min = got;
                continue;
            }
            if (got.compareTo(min) < 0)
                min = got;
        }
        if (min == null) return;

        var threshold = new CombinedTimestamp<TimestampT, PeerIdT>(min, null);

        if (!log.isEmpty() && log.peekOldest().getLeft().compareTo(threshold) <= 0) {
            Set<NodeIdT> inTrash = new HashSet<>();

            {
                Pair<CombinedTimestamp<TimestampT, PeerIdT>, LogRecord<TimestampT, PeerIdT, MetaT, NodeIdT>> entry = null;
                while ((entry = log.peekOldest()) != null
                        && entry.getLeft().compareTo(threshold) <= 0) {
                    log.takeOldest();
                    if (entry.getRight().effects() != null)
                        for (var e : entry.getRight().effects()) {
                            if (Objects.equals(e.newParentId(), _storage.getTrashId())) {
                                inTrash.add(e.childId());
                            } else {
                                inTrash.remove(e.childId());
                            }
                        }
                }

            }
            if (!inTrash.isEmpty()) {
                var trash = _storage.getById(_storage.getTrashId());
                trash.rwLock();
                try {
                    for (var n : inTrash) {
                        var node = _storage.getById(n);
                        node.rwLock();
                        try {
                            if (trash.getNode().getChildren().remove(n.toString()) == null)
                                LOGGER.severe("Node " + node.getNode().getId() + " not found in trash but should be there");
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
        } else {
            LOGGER.fine("Nothing to trim");
        }
    }

    public <LocalMetaT extends MetaT> void move(NodeIdT newParent, LocalMetaT newMeta, NodeIdT child) {
        move(newParent, newMeta, child, true);
    }

    public void move(NodeIdT newParent, MetaT newMeta, NodeIdT child, boolean failCreatingIfExists) {
        _storage.rwLock();
        try {
            var createdMove = createMove(newParent, newMeta, child);
            _opRecorder.recordOp(createdMove);
            applyOp(_peers.getSelfId(), createdMove, failCreatingIfExists);
        } finally {
            _storage.rwUnlock();
        }
    }

    public void applyExternalOp(PeerIdT from, OpMove<TimestampT, PeerIdT, MetaT, NodeIdT> op) {
        _storage.rwLock();
        try {
            _clock.updateTimestamp(op.timestamp().timestamp());
            applyOp(from, op, false);
        } finally {
            _storage.rwUnlock();
        }
    }

    // Returns true if the timestamp is newer than what's seen, false otherwise
    private boolean updateTimestampImpl(PeerIdT from, TimestampT newTimestamp) {
        _storage.assertRwLock();
        TimestampT oldRef = _storage.getPeerTimestampLog().getForPeer(from);
        if (oldRef != null && oldRef.compareTo(newTimestamp) > 0) { // FIXME?
            LOGGER.warning("Wrong op order: received older than known from " + from.toString());
            return false;
        }
        _storage.getPeerTimestampLog().putForPeer(from, newTimestamp);
        return true;
    }

    public void updateExternalTimestamp(PeerIdT from, TimestampT timestamp) {
        _storage.rLock();
        try {
            // TODO: Ideally no point in this separate locking?
            var gotExt = _storage.getPeerTimestampLog().getForPeer(from);
            var gotSelf = _storage.getPeerTimestampLog().getForPeer(_peers.getSelfId());
            if ((gotExt != null && gotExt.compareTo(timestamp) >= 0)
                    && (gotSelf != null && gotSelf.compareTo(_clock.peekTimestamp()) >= 0)) return;
        } finally {
            _storage.rUnlock();
        }
        _storage.rwLock();
        try {
            updateTimestampImpl(_peers.getSelfId(), _clock.peekTimestamp()); // FIXME:? Kind of a hack?
            updateTimestampImpl(from, timestamp);
            tryTrimLog();
        } finally {
            _storage.rwUnlock();
        }
    }

    private void applyOp(PeerIdT from, OpMove<TimestampT, PeerIdT, MetaT, NodeIdT> op, boolean failCreatingIfExists) {
        _storage.assertRwLock();

        if (!updateTimestampImpl(from, op.timestamp().timestamp())) return;

        var log = _storage.getLog();

        // FIXME: hack?
        int cmp = log.isEmpty() ? 1 : op.timestamp().compareTo(log.peekNewest().getKey());

        if (log.containsKey(op.timestamp())) {
            tryTrimLog();
            return;
        }
        assert cmp != 0;
        if (cmp < 0) {
            try {
                if (log.containsKey(op.timestamp())) return;
                var toUndo = log.newestSlice(op.timestamp(), false);
                _undoCtx = new HashMap<>();
                for (var entry : toUndo.reversed()) {
                    undoOp(entry.getValue());
                }
                try {
                    doAndPut(op, failCreatingIfExists);
                } finally {
                    for (var entry : toUndo) {
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
            doAndPut(op, failCreatingIfExists);
            tryTrimLog();
        }
    }

    private CombinedTimestamp<TimestampT, PeerIdT> getTimestamp() {
        _storage.assertRwLock();
        return new CombinedTimestamp<>(_clock.getTimestamp(), _peers.getSelfId());
    }

    private <LocalMetaT extends MetaT> OpMove<TimestampT, PeerIdT, LocalMetaT, NodeIdT> createMove(NodeIdT newParent, LocalMetaT newMeta, NodeIdT node) {
        _storage.assertRwLock();
        return new OpMove<>(getTimestamp(), newParent, newMeta, node);
    }

    private LogRecord<TimestampT, PeerIdT, MetaT, NodeIdT> doOp(OpMove<TimestampT, PeerIdT, MetaT, NodeIdT> op, boolean failCreatingIfExists) {
        LogRecord<TimestampT, PeerIdT, MetaT, NodeIdT> computed;
        try {
            computed = computeEffects(op, failCreatingIfExists);
        } catch (AlreadyExistsException aex) {
            throw aex;
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Error computing effects for op" + op.toString(), e);
            computed = new LogRecord<>(op, null);
        }

        if (computed.effects() != null)
            applyEffects(op, computed.effects());
        return computed;
    }

    private WrapperT getNewNode(TreeNode<TimestampT, PeerIdT, MetaT, NodeIdT> desired) {
        _storage.assertRwLock();
        if (_undoCtx != null) {
            var node = _undoCtx.get(desired.getId());
            if (node != null) {
                node.rwLock();
                try {
                    if (!node.getNode().getChildren().isEmpty()) {
                        LOGGER.log(Level.WARNING, "Not empty children for undone node " + desired.getId());
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

    private void applyEffects(OpMove<TimestampT, PeerIdT, MetaT, NodeIdT> sourceOp, List<LogEffect<TimestampT, PeerIdT, MetaT, NodeIdT>> effects) {
        _storage.assertRwLock();
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
                        if (effect.newParentId().equals(_storage.getTrashId()) &&
                                !Objects.equals(effect.newMeta().getName(), effect.childId()))
                            throw new IllegalArgumentException("Move to trash should have id of node as name");
                        node.getNode().setParent(effect.newParentId());
                        node.getNode().setMeta(effect.newMeta());
                        node.getNode().setLastEffectiveOp(effect.effectiveOp());
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

    private LogRecord<TimestampT, PeerIdT, MetaT, NodeIdT> computeEffects(OpMove<TimestampT, PeerIdT, MetaT, NodeIdT> op, boolean failCreatingIfExists) {
        _storage.assertRwLock();
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
                    if (failCreatingIfExists)
                        throw new AlreadyExistsException("Already exists: " + op.newMeta().getName() + ": " + conflictNodeId);

                    var conflictNode = _storage.getById(conflictNodeId);
                    conflictNode.rLock();
                    try {
                        MetaT conflictNodeMeta = conflictNode.getNode().getMeta();
                        String newConflictNodeName = conflictNodeMeta.getName() + ".conflict." + conflictNode.getNode().getId();
                        String newOursName = op.newMeta().getName() + ".conflict." + op.childId();
                        return new LogRecord<>(op, List.of(
                                new LogEffect<>(new LogEffectOld<>(conflictNode.getNode().getLastEffectiveOp(), newParentId, conflictNodeMeta), conflictNode.getNode().getLastEffectiveOp(), newParentId, (MetaT) conflictNodeMeta.withName(newConflictNodeName), conflictNodeId),
                                new LogEffect<>(null, op, op.newParentId(), (MetaT) op.newMeta().withName(newOursName), op.childId())
                        ));
                    } finally {
                        conflictNode.rUnlock();
                    }
                } else {
                    return new LogRecord<>(op, List.of(
                            new LogEffect<>(null, op, newParentId, op.newMeta(), op.childId())
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
                            new LogEffect<>(new LogEffectOld<>(replaceNode.getNode().getLastEffectiveOp(), newParentId, replaceNodeMeta), replaceNode.getNode().getLastEffectiveOp(), _storage.getTrashId(), (MetaT) replaceNodeMeta.withName(replaceNodeId.toString()), replaceNodeId),
                            new LogEffect<>(new LogEffectOld<>(node.getNode().getLastEffectiveOp(), oldParentId, oldMeta), op, op.newParentId(), op.newMeta(), op.childId())
                    ));
                } finally {
                    replaceNode.rUnlock();
                }
            }
            return new LogRecord<>(op, List.of(
                    new LogEffect<>(new LogEffectOld<>(node.getNode().getLastEffectiveOp(), oldParentId, oldMeta), op, op.newParentId(), op.newMeta(), op.childId())
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

    public void walkTree(Consumer<WrapperT> consumer) {
        _storage.rLock();
        try {
            ArrayDeque<NodeIdT> queue = new ArrayDeque<>();
            queue.push(_storage.getRootId());

            while (!queue.isEmpty()) {
                var id = queue.pop();
                var node = _storage.getById(id);
                if (node == null) continue;
                node.rLock();
                try {
                    queue.addAll(node.getNode().getChildren().values());
                    consumer.accept(node);
                } finally {
                    node.rUnlock();
                }
            }
        } finally {
            _storage.rUnlock();
        }
    }

    public Pair<String, NodeIdT> findParent(Function<WrapperT, Boolean> kidPredicate) {
        _storage.rLock();
        try {
            ArrayDeque<NodeIdT> queue = new ArrayDeque<>();
            queue.push(_storage.getRootId());

            while (!queue.isEmpty()) {
                var id = queue.pop();
                var node = _storage.getById(id);
                if (node == null) continue;
                node.rLock();
                try {
                    var children = node.getNode().getChildren();
                    for (var childEntry : children.entrySet()) {
                        var child = _storage.getById(childEntry.getValue());
                        if (kidPredicate.apply(child)) {
                            return Pair.of(childEntry.getKey(), node.getNode().getId());
                        }
                    }
                    queue.addAll(children.values());
                } finally {
                    node.rUnlock();
                }
            }
        } finally {
            _storage.rUnlock();
        }
        return null;
    }

    public void recordBoostrapFor(PeerIdT host) {
        TreeMap<CombinedTimestamp<TimestampT, PeerIdT>, OpMove<TimestampT, PeerIdT, MetaT, NodeIdT>> result = new TreeMap<>();

        _storage.rwLock();
        try {
            walkTree(node -> {
                var op = node.getNode().getLastEffectiveOp();
                if (node.getNode().getLastEffectiveOp() == null) return;
                LOGGER.fine("visited bootstrap op for " + host + ": " + op.timestamp().toString() + " " + op.newMeta().getName() + " " + op.childId() + "->" + op.newParentId());
                result.put(node.getNode().getLastEffectiveOp().timestamp(), node.getNode().getLastEffectiveOp());
            });

            for (var le : _storage.getLog().getAll()) {
                var op = le.getValue().op();
                LOGGER.fine("bootstrap op from log for " + host + ": " + op.timestamp().toString() + " " + op.newMeta().getName() + " " + op.childId() + "->" + op.newParentId());
                result.put(le.getKey(), le.getValue().op());
            }

            for (var op : result.values()) {
                LOGGER.fine("Recording bootstrap op for " + host + ": " + op.timestamp().toString() + " " + op.newMeta().getName() + " " + op.childId() + "->" + op.newParentId());
                _opRecorder.recordOpForPeer(host, op);
            }
        } finally {
            _storage.rwUnlock();
        }
    }
}
