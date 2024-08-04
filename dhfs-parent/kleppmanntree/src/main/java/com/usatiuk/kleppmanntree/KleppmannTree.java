package com.usatiuk.kleppmanntree;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

public class KleppmannTree<TimestampT extends Comparable<TimestampT>, PeerIdT extends Comparable<PeerIdT>, MetaT extends NodeMeta, NodeIdT, WrapperT extends TreeNodeWrapper<MetaT, NodeIdT>> {
    private final StorageInterface<TimestampT, PeerIdT, MetaT, NodeIdT, WrapperT> _storage;
    private final PeerInterface<PeerIdT> _peers;
    private final Clock<TimestampT> _clock;
    private final OpRecorder<TimestampT, PeerIdT, MetaT, NodeIdT> _opRecorder;

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

    private void undoEffect(LogEffect<? extends MetaT, NodeIdT> effect) {
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
                node.notifyRmRef(curParent.getNode().getId());
                _storage.removeNode(node.getNode().getId());
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
        entry.setValue(doOp(null, entry.getValue().op(), false));
    }

    private <LocalMetaT extends MetaT> void doAndPut(PeerIdT from, OpMove<TimestampT, PeerIdT, LocalMetaT, NodeIdT> op) {
        var res = doOp(from, op, true);
        var log = _storage.getLog();
        log.put(res.op().timestamp(), res);
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

            for (var e : canTrim.values()) {
                if (Objects.equals(e.op().newParentId(), _storage.getTrashId())) {
                    inTrash.add(e.op().childId());
                } else {
                    inTrash.remove(e.op().childId());
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
                    }
                } finally {
                    trash.rwUnlock();
                }
            }
        }
    }

    private void maybeRecord(PeerIdT from, OpMove<TimestampT, PeerIdT, ? extends MetaT, NodeIdT> op) {
        if (Objects.equals(from, _peers.getSelfId())) {
            if (!_storage.getLog().containsKey(op.timestamp()))
                _opRecorder.recordOp(op);
        }
    }

    public <LocalMetaT extends MetaT> void move(NodeIdT newParent, LocalMetaT newMeta, NodeIdT child) {
        synchronized (this) {
            applyOp(_peers.getSelfId(), createMove(newParent, newMeta, child));
        }
    }

    public void applyExternalOp(PeerIdT from, OpMove<TimestampT, PeerIdT, ? extends MetaT, NodeIdT> op) {
        applyOp(from, op);
    }

    private void applyOp(PeerIdT from, OpMove<TimestampT, PeerIdT, ? extends MetaT, NodeIdT> op) {
        synchronized (this) {
            _clock.updateTimestamp(op.timestamp().timestamp());
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
                    for (var entry : toUndo.reversed().entrySet()) {
                        undoOp(entry.getValue());
                    }
                    doAndPut(from, op);
                    for (var entry : toUndo.entrySet()) {
                        redoOp(entry);
                    }
                } finally {
                    tryTrimLog();
                }
            } else {
                doAndPut(from, op);
                tryTrimLog();
            }
        }
    }

    private CombinedTimestamp<TimestampT, PeerIdT> getTimestamp() {
        return new CombinedTimestamp<>(_clock.getTimestamp(), _peers.getSelfId());
    }

    private <LocalMetaT extends MetaT> OpMove<TimestampT, PeerIdT, LocalMetaT, NodeIdT> createMove(NodeIdT newParent, LocalMetaT newMeta, NodeIdT node) {
        return new OpMove<>(getTimestamp(), newParent, newMeta, node);
    }

    private <LocalMetaT extends MetaT> LogRecord<TimestampT, PeerIdT, LocalMetaT, NodeIdT> doOp(PeerIdT from, OpMove<TimestampT, PeerIdT, LocalMetaT, NodeIdT> op, boolean record) {
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
                    var old = newParent.getNode().getChildren().get(node.getNode().getMeta().getName());

                    var effects = new ArrayList<LogEffect<LocalMetaT, NodeIdT>>(2);

                    if (old != null) {
                        var oldN = _storage.getById(old);
                        oldN.rwLock();
                        try {
                            var oldMeta = oldN.getNode().getMeta();
                            oldN.getNode().setMeta((MetaT) oldN.getNode().getMeta().withName(oldN.getNode().getMeta().getName() + ".conflict." + oldN.getNode().getId()));
                            node.getNode().setMeta((MetaT) node.getNode().getMeta().withName(node.getNode().getMeta().getName() + ".conflict." + node.getNode().getId()));
                            newParent.getNode().getChildren().remove(node.getNode().getMeta().getName());
                            newParent.getNode().getChildren().put(oldN.getNode().getMeta().getName(), oldN.getNode().getId());
                            effects.add(new LogEffect<>(new LogEffectOld<>(newParent.getNode().getId(), (LocalMetaT) oldMeta), op.newParentId(), (LocalMetaT) oldN.getNode().getMeta(), oldN.getNode().getId()));
                            effects.add(new LogEffect<>(null, op.newParentId(), (LocalMetaT) node.getNode().getMeta(), op.childId()));
                        } finally {
                            oldN.rwUnlock();
                        }
                    } else {
                        effects.add(new LogEffect<>(null, op.newParentId(), (LocalMetaT) node.getNode().getMeta(), op.childId()));
                    }

                    newParent.getNode().getChildren().put(node.getNode().getMeta().getName(), node.getNode().getId());
                    node.notifyRef(newParent.getNode().getId());
                    if (record)
                        maybeRecord(from, op);
                    return new LogRecord<>(op, Collections.unmodifiableList(effects));
                } finally {
                    node.rwUnlock();
                }
            } finally {
                newParent.rwUnlock();
            }
        }

        if (Objects.equals(op.childId(), op.newParentId()) || isAncestor(op.childId(), op.newParentId())) {
            if (record)
                maybeRecord(from, op);
            return new LogRecord<>(op, null);
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
            // TODO: somehow detect when this might be a conflict? (2 devices move two different files into one)
            var effects = new ArrayList<LogEffect<LocalMetaT, NodeIdT>>(2);
            if (old != null) {
                var oldNode = _storage.getById(old);
                try {
                    oldNode.rwLock();
                    trash.getNode().getChildren().put(oldNode.getNode().getId().toString(), oldNode.getNode().getId());
                    var oldOldMeta = oldNode.getNode().getMeta();
                    oldNode.notifyRmRef(newParent.getNode().getId());
                    oldNode.notifyRef(trash.getNode().getId());
                    oldNode.getNode().setMeta((MetaT) oldNode.getNode().getMeta().withName(oldNode.getNode().getId().toString()));
                    effects.add(new LogEffect<>(new LogEffectOld<>(newParent.getNode().getId(), (LocalMetaT) oldOldMeta), trash.getNode().getId(), (LocalMetaT) oldNode.getNode().getMeta(), oldNode.getNode().getId()));
                } finally {
                    oldNode.rwUnlock();
                }
            }
            newParent.getNode().getChildren().put(op.newMeta().getName(), node.getNode().getId());
            node.notifyRmRef(oldParent.getNode().getId());
            node.notifyRef(newParent.getNode().getId());
            if (record)
                maybeRecord(from, op);
            effects.add(new LogEffect<>(new LogEffectOld<>(oldParent.getNode().getId(), (LocalMetaT) oldMeta), op.newParentId(), (LocalMetaT) node.getNode().getMeta(), node.getNode().getId()));
            return new LogRecord<>(op, Collections.unmodifiableList(effects));
        } finally {
            node.rwUnlock();
            oldParent.rwUnlock();
            newParent.rwUnlock();
            trash.rwUnlock();
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
