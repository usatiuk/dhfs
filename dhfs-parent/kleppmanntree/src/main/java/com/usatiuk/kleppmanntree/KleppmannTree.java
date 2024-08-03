package com.usatiuk.kleppmanntree;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public class KleppmannTree<TimestampT extends Comparable<TimestampT>, PeerIdT extends Comparable<PeerIdT>, NameT, MetaT extends NodeMeta<NameT>, NodeIdT, WrapperT extends TreeNodeWrapper<NameT, MetaT, NodeIdT>> {
    private final StorageInterface<TimestampT, PeerIdT, NameT, MetaT, NodeIdT, WrapperT> _storage;
    private final PeerInterface<PeerIdT> _peers;
    private final Clock<TimestampT> _clock;

    public KleppmannTree(StorageInterface<TimestampT, PeerIdT, NameT, MetaT, NodeIdT, WrapperT> storage,
                         PeerInterface<PeerIdT> peers,
                         Clock<TimestampT> clock) {
        _storage = storage;
        _peers = peers;
        _clock = clock;
    }


    public NodeIdT traverse(NodeIdT fromId, List<NameT> names) {
        if (names.isEmpty()) return fromId;

        var from = _storage.getById(fromId);
        from.rLock();
        NodeIdT childId;
        try {
            childId = from.getNode().getChildren().get(names.getFirst());
        } finally {
            from.rwUnlock();
        }

        if (childId == null)
            return null;

        return traverse(childId, names.subList(1, names.size()));
    }

    public NodeIdT traverse(List<NameT> names) {
        return traverse(_storage.getRootId(), names);
    }

    private void undoOp(LogOpMove<TimestampT, PeerIdT, NameT, ? extends MetaT, NodeIdT> op) {
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
                _storage.removeNode(node.getNode().getId());
            } finally {
                node.rwUnlock();
                curParent.rwUnlock();
            }
        }
    }

    private void redoOp(Map.Entry<CombinedTimestamp<TimestampT, PeerIdT>, LogOpMove<TimestampT, PeerIdT, NameT, ? extends MetaT, NodeIdT>> entry) {
        entry.setValue(doOp(entry.getValue().op()));
    }

    public <LocalMetaT extends MetaT> void applyOp(OpMove<TimestampT, PeerIdT, NameT, LocalMetaT, NodeIdT> op) {
        _clock.updateTimestamp(op.timestamp().timestamp());
        var log = _storage.getLog();
        if (log.isEmpty()) {
            log.put(op.timestamp(), doOp(op));
            return;
        }
        var cmp = op.timestamp().compareTo(log.lastEntry().getKey());
        assert cmp != 0;
        if (cmp < 0) {
            _storage.globalLock();
            try {
                var toUndo = log.tailMap(op.timestamp(), false);
                for (var entry : toUndo.reversed().entrySet()) {
                    undoOp(entry.getValue());
                }
                log.put(op.timestamp(), doOp(op));
                for (var entry : toUndo.entrySet()) {
                    redoOp(entry);
                }
            } finally {
                _storage.globalUnlock();
            }
        } else {
            log.put(op.timestamp(), doOp(op));
        }
    }

    private <LocalMetaT extends MetaT> LogOpMove<TimestampT, PeerIdT, NameT, LocalMetaT, NodeIdT> doOp(OpMove<TimestampT, PeerIdT, NameT, LocalMetaT, NodeIdT> op) {
        var node = _storage.getById(op.childId());
        if (node == null) {
            node = _storage.createNewNode(op.childId());
        }

        var oldParent = node.getNode().getParent() != null ? _storage.getById(node.getNode().getParent()) : null;
        var newParent = _storage.getById(op.newParentId());

        if (newParent == null) {
            throw new IllegalArgumentException("New parent not found");
        }

        if (oldParent == null) {
            newParent.rwLock();
            try {
                node.rwLock();
                try {
                    node.getNode().setMeta(op.newMeta());
                    node.getNode().setParent(op.newParentId());
                    newParent.getNode().getChildren().put(node.getNode().getMeta().getName(), node.getNode().getId());
                    return new LogOpMove<>(null, op);
                } finally {
                    node.rwUnlock();
                }
            } finally {
                newParent.rwUnlock();
            }
        }

        // FIXME:
        _storage.globalLock();
        try {
            if (op.childId() == op.newParentId() || isAncestor(op.childId(), op.newParentId()))
                return new LogOpMove<>(null, op);

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
                newParent.getNode().getChildren().put(op.newMeta().getName(), node.getNode().getId());
                return new LogOpMove<>(new LogOpMoveOld<>(oldParent.getNode().getId(), (LocalMetaT) oldMeta), op);
            } finally {
                node.rwUnlock();
                oldParent.rwUnlock();
                newParent.rwUnlock();
            }
        } finally {
            _storage.globalUnlock();
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
