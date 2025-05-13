package com.usatiuk.kleppmanntree;

import org.apache.commons.lang3.tuple.Pair;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * An implementation of a tree as described in <a href="https://martin.kleppmann.com/papers/move-op.pdf">A highly-available move operation for replicated trees</a>
 *
 * @param <TimestampT> Type of the timestamp
 * @param <PeerIdT>    Type of the peer ID
 * @param <MetaT>      Type of the node metadata
 * @param <NodeIdT>    Type of the node ID
 */
public class KleppmannTree<TimestampT extends Comparable<TimestampT>, PeerIdT extends Comparable<PeerIdT>, MetaT extends NodeMeta, NodeIdT> {
    private static final Logger LOGGER = Logger.getLogger(KleppmannTree.class.getName());

    private final StorageInterface<TimestampT, PeerIdT, MetaT, NodeIdT> _storage;
    private final PeerInterface<PeerIdT> _peers;
    private final Clock<TimestampT> _clock;
    private final OpRecorder<TimestampT, PeerIdT, MetaT, NodeIdT> _opRecorder;

    /**
     * Constructor with all the dependencies
     *
     * @param storage    Storage interface
     * @param peers      Peer interface
     * @param clock      Clock interface
     * @param opRecorder Operation recorder interface
     */
    public KleppmannTree(StorageInterface<TimestampT, PeerIdT, MetaT, NodeIdT> storage,
                         PeerInterface<PeerIdT> peers,
                         Clock<TimestampT> clock,
                         OpRecorder<TimestampT, PeerIdT, MetaT, NodeIdT> opRecorder) {
        _storage = storage;
        _peers = peers;
        _clock = clock;
        _opRecorder = opRecorder;
    }

    /**
     * Traverse the tree from the given node ID using the given list of names
     *
     * @param fromId The starting node ID
     * @param names  The list of names to traverse
     * @return The resulting node ID or null if not found
     */
    private NodeIdT traverseImpl(NodeIdT fromId, List<String> names) {
        if (names.isEmpty()) return fromId;

        var from = _storage.getById(fromId);
        NodeIdT childId;
        childId = from.children().get(names.getFirst());

        if (childId == null)
            return null;

        return traverseImpl(childId, names.subList(1, names.size()));
    }

    /**
     * Traverse the tree from its root node using the given list of names
     *
     * @param names The list of names to traverse
     * @return The resulting node ID or null if not found
     */
    public NodeIdT traverse(List<String> names) {
        return traverseImpl(_storage.getRootId(), names);
    }

    /**
     * Undo the effect of a log effect
     *
     * @param effect The log effect to undo
     */
    private void undoEffect(LogEffect<TimestampT, PeerIdT, MetaT, NodeIdT> effect) {
        if (effect.oldInfo() != null) {
            var node = _storage.getById(effect.childId());
            var curParent = _storage.getById(effect.newParentId());
            {
                var newCurParentChildren = curParent.children().minus(node.name());
                curParent = curParent.withChildren(newCurParentChildren);
                _storage.putNode(curParent);
            }

            if (effect.oldInfo().oldMeta() != null
                    && node.meta() != null
                    && !node.meta().getClass().equals(effect.oldInfo().oldMeta().getClass()))
                throw new IllegalArgumentException("Class mismatch for meta for node " + node.key());

            // Needs to be read after changing curParent, as it might be the same node
            var oldParent = _storage.getById(effect.oldInfo().oldParent());
            {
                var newOldParentChildren = oldParent.children().plus(effect.oldName(), node.key());
                oldParent = oldParent.withChildren(newOldParentChildren);
                _storage.putNode(oldParent);
            }
            _storage.putNode(
                    node.withMeta(effect.oldInfo().oldMeta())
                            .withParent(effect.oldInfo().oldParent())
                            .withLastEffectiveOp(effect.oldInfo().oldEffectiveMove())
            );
        } else {
            var node = _storage.getById(effect.childId());
            var curParent = _storage.getById(effect.newParentId());
            {
                var newCurParentChildren = curParent.children().minus(node.name());
                curParent = curParent.withChildren(newCurParentChildren);
                _storage.putNode(curParent);
            }
            _storage.putNode(
                    node.withParent(null)
                            .withLastEffectiveOp(null)
            );
        }
    }

    /**
     * Undo the effects of a log record
     *
     * @param op The log record to undo
     */
    private void undoOp(LogRecord<TimestampT, PeerIdT, MetaT, NodeIdT> op) {
        LOGGER.finer(() -> "Will undo op: " + op);
        if (op.effects() != null)
            for (var e : op.effects().reversed())
                undoEffect(e);
    }

    /**
     * Redo the operation in a log record
     *
     * @param entry The log record to redo
     */
    private void redoOp(Map.Entry<CombinedTimestamp<TimestampT, PeerIdT>, LogRecord<TimestampT, PeerIdT, MetaT, NodeIdT>> entry) {
        var newEffects = doOp(entry.getValue().op(), false);
        _storage.getLog().replace(entry.getKey(), newEffects);
    }

    /**
     * Perform the operation and put it in the log
     *
     * @param op                   The operation to perform
     * @param failCreatingIfExists Whether to fail if there is a name conflict,
     *                             otherwise replace the existing node
     * @throws AlreadyExistsException If the node already exists and failCreatingIfExists is true
     */
    private void doAndPut(OpMove<TimestampT, PeerIdT, MetaT, NodeIdT> op, boolean failCreatingIfExists) {
        var res = doOp(op, failCreatingIfExists);
        _storage.getLog().put(res.op().timestamp(), res);
    }

    /**
     * Try to trim the log to the causality threshold
     */
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
                for (var n : inTrash) {
                    var node = _storage.getById(n);
                    {
                        if (!trash.children().containsKey(n.toString()))
                            LOGGER.severe("Node " + node.key() + " not found in trash but should be there");
                        trash = trash.withChildren(trash.children().minus(n.toString()));
                        _storage.putNode(trash);
                    }
                    _storage.removeNode(n);
                }
            }
        } else {
            LOGGER.fine("Nothing to trim");
        }
    }

    /**
     * Move a node to a new parent with new metadata
     *
     * @param newParent The new parent node ID
     * @param newMeta   The new metadata
     * @param child     The child node ID
     * @throws AlreadyExistsException If the node already exists and failCreatingIfExists is true
     */
    public <LocalMetaT extends MetaT> void move(NodeIdT newParent, LocalMetaT newMeta, NodeIdT child) {
        move(newParent, newMeta, child, true);
    }

    /**
     * Move a node to a new parent with new metadata
     *
     * @param newParent            The new parent node ID
     * @param newMeta              The new metadata
     * @param child                The child node ID
     * @param failCreatingIfExists Whether to fail if there is a name conflict,
     *                             otherwise replace the existing node
     * @throws AlreadyExistsException If the node already exists and failCreatingIfExists is true
     */
    public void move(NodeIdT newParent, MetaT newMeta, NodeIdT child, boolean failCreatingIfExists) {
        var createdMove = createMove(newParent, newMeta, child);
        applyOp(_peers.getSelfId(), createdMove, failCreatingIfExists);
        _opRecorder.recordOp(createdMove);
    }

    /**
     * Apply an external operation from a remote peer
     *
     * @param from The peer ID
     * @param op   The operation to apply
     */
    public void applyExternalOp(PeerIdT from, OpMove<TimestampT, PeerIdT, MetaT, NodeIdT> op) {
        _clock.updateTimestamp(op.timestamp().timestamp());
        applyOp(from, op, false);
    }

    /**
     * Update the causality threshold timestamp for a peer
     *
     * @param from         The peer ID
     * @param newTimestamp The timestamp received from it
     * @return True if the timestamp was updated, false otherwise
     */
    private boolean updateTimestampImpl(PeerIdT from, TimestampT newTimestamp) {
        TimestampT oldRef = _storage.getPeerTimestampLog().getForPeer(from);
        if (oldRef != null && oldRef.compareTo(newTimestamp) >= 0) { // FIXME?
            LOGGER.warning("Wrong op order: received older than known from " + from.toString());
            return false;
        }
        _storage.getPeerTimestampLog().putForPeer(from, newTimestamp);
        return true;
    }

    /**
     * Update the causality threshold timestamp for a peer
     *
     * @param from      The peer ID
     * @param timestamp The timestamp received from it
     */
    public void updateExternalTimestamp(PeerIdT from, TimestampT timestamp) {
        var gotExt = _storage.getPeerTimestampLog().getForPeer(from);
        var gotSelf = _storage.getPeerTimestampLog().getForPeer(_peers.getSelfId());
        if (!(gotExt != null && gotExt.compareTo(timestamp) >= 0))
            updateTimestampImpl(from, timestamp);
        if (!(gotSelf != null && gotSelf.compareTo(_clock.peekTimestamp()) >= 0))
            updateTimestampImpl(_peers.getSelfId(), _clock.peekTimestamp()); // FIXME:? Kind of a hack?
        tryTrimLog();
    }

    /**
     * Apply an operation from a peer
     *
     * @param from                 The peer ID
     * @param op                   The operation to apply
     * @param failCreatingIfExists Whether to fail if there is a name conflict,
     *                             otherwise replace the existing node
     * @throws AlreadyExistsException If the node already exists and failCreatingIfExists is true
     */
    private void applyOp(PeerIdT from, OpMove<TimestampT, PeerIdT, MetaT, NodeIdT> op, boolean failCreatingIfExists) {
        if (!updateTimestampImpl(op.timestamp().nodeId(), op.timestamp().timestamp())) return;

        LOGGER.finer(() -> "Will apply op: " + op + " from " + from);

        var log = _storage.getLog();

        // FIXME: hack?
        int cmp = log.isEmpty() ? 1 : op.timestamp().compareTo(log.peekNewest().getKey());

        if (log.containsKey(op.timestamp())) {
            tryTrimLog();
            return;
        }
        assert cmp != 0;
        if (cmp < 0) {
            if (log.containsKey(op.timestamp())) return;
            var toUndo = log.newestSlice(op.timestamp(), false);
            for (var entry : toUndo.reversed()) {
                undoOp(entry.getValue());
            }
            doAndPut(op, failCreatingIfExists);
            for (var entry : toUndo) {
                redoOp(entry);
            }
            tryTrimLog();
        } else {
            doAndPut(op, failCreatingIfExists);
            tryTrimLog();
        }
    }

    /**
     * Get a new timestamp, incrementing the one in storage
     *
     * @return A new timestamp
     */
    private CombinedTimestamp<TimestampT, PeerIdT> getTimestamp() {
        return new CombinedTimestamp<>(_clock.getTimestamp(), _peers.getSelfId());
    }

    /**
     * Create a new move operation
     *
     * @param newParent The new parent node ID
     * @param newMeta   The new metadata
     * @param node      The child node ID
     * @return A new move operation
     */
    private <LocalMetaT extends MetaT> OpMove<TimestampT, PeerIdT, LocalMetaT, NodeIdT> createMove(NodeIdT newParent, LocalMetaT newMeta, NodeIdT node) {
        return new OpMove<>(getTimestamp(), newParent, newMeta, node);
    }

    /**
     * Perform the operation and return the log record
     *
     * @param op                   The operation to perform
     * @param failCreatingIfExists Whether to fail if there is a name conflict,
     *                             otherwise replace the existing node
     * @return The log record
     * @throws AlreadyExistsException If the node already exists and failCreatingIfExists is true
     */
    private LogRecord<TimestampT, PeerIdT, MetaT, NodeIdT> doOp(OpMove<TimestampT, PeerIdT, MetaT, NodeIdT> op, boolean failCreatingIfExists) {
        LOGGER.finer(() -> "Doing op: " + op);
        LogRecord<TimestampT, PeerIdT, MetaT, NodeIdT> computed;
        try {
            computed = computeEffects(op, failCreatingIfExists);
        } catch (AlreadyExistsException aex) {
            throw aex;
        } catch (Exception e) {
            throw new RuntimeException("Error computing effects for op " + op.toString(), e);
        }

        if (computed.effects() != null)
            applyEffects(op, computed.effects());
        return computed;
    }

    /**
     * Get a new node from storage
     *
     * @param key    The node ID
     * @param parent The parent node ID
     * @param meta   The metadata
     * @return A new tree node
     */
    private TreeNode<TimestampT, PeerIdT, MetaT, NodeIdT> getNewNode(NodeIdT key, NodeIdT parent, MetaT meta) {
        return _storage.createNewNode(key, parent, meta);
    }

    /**
     * Apply the effects of a log record
     *
     * @param sourceOp The source operation
     * @param effects  The list of log effects
     */
    private void applyEffects(OpMove<TimestampT, PeerIdT, MetaT, NodeIdT> sourceOp, List<LogEffect<TimestampT, PeerIdT, MetaT, NodeIdT>> effects) {
        for (var effect : effects) {
            LOGGER.finer(() -> "Applying effect: " + effect + " from op " + sourceOp);
            TreeNode<TimestampT, PeerIdT, MetaT, NodeIdT> oldParentNode = null;
            TreeNode<TimestampT, PeerIdT, MetaT, NodeIdT> newParentNode;
            TreeNode<TimestampT, PeerIdT, MetaT, NodeIdT> node;

            if (effect.oldInfo() != null) {
                oldParentNode = _storage.getById(effect.oldInfo().oldParent());
            }
            if (oldParentNode == null) {
                node = getNewNode(effect.childId(), effect.newParentId(), effect.newMeta());
            } else {
                node = _storage.getById(effect.childId());
            }
            if (oldParentNode != null) {
                var newOldParentChildren = oldParentNode.children().minus(effect.oldName());
                oldParentNode = oldParentNode.withChildren(newOldParentChildren);
                _storage.putNode(oldParentNode);
            }

            // Needs to be read after changing oldParentNode, as it might be the same node
            newParentNode = _storage.getById(effect.newParentId());

            {
                var newNewParentChildren = newParentNode.children().plus(effect.newName(), effect.childId());
                newParentNode = newParentNode.withChildren(newNewParentChildren);
                _storage.putNode(newParentNode);
            }
            if (effect.newParentId().equals(_storage.getTrashId()) &&
                    !Objects.equals(effect.newName(), effect.childId().toString()))
                throw new IllegalArgumentException("Move to trash should have id of node as name");
            _storage.putNode(
                    node.withParent(effect.newParentId())
                            .withMeta(effect.newMeta())
                            .withLastEffectiveOp(sourceOp)
            );
        }
    }

    /**
     * Compute the effects of a move operation
     *
     * @param op                   The operation to process
     * @param failCreatingIfExists Whether to fail if there is a name conflict,
     *                             otherwise replace the existing node
     * @return The log record with the computed effects
     * @throws AlreadyExistsException If the node already exists and failCreatingIfExists is true
     */
    private LogRecord<TimestampT, PeerIdT, MetaT, NodeIdT> computeEffects(OpMove<TimestampT, PeerIdT, MetaT, NodeIdT> op, boolean failCreatingIfExists) {
        var node = _storage.getById(op.childId());

        NodeIdT oldParentId = (node != null && node.parent() != null) ? node.parent() : null;
        NodeIdT newParentId = op.newParentId();
        TreeNode<TimestampT, PeerIdT, MetaT, NodeIdT> newParent = _storage.getById(newParentId);


        if (newParent == null) {
            LOGGER.log(Level.SEVERE, "New parent not found " + op.newName() + " " + op.childId());

            // Creation
            if (oldParentId == null) {
                LOGGER.severe(() -> "Creating both dummy parent and child node");
                return new LogRecord<>(op, List.of(
                        new LogEffect<>(null, op, _storage.getLostFoundId(), null, newParentId),
                        new LogEffect<>(null, op, newParentId, op.newMeta(), op.childId())
                ));
            } else {
                LOGGER.severe(() -> "Moving child node to dummy parent");
                return new LogRecord<>(op, List.of(
                        new LogEffect<>(null, op, _storage.getLostFoundId(), null, newParentId),
                        new LogEffect<>(new LogEffectOld<>(node.lastEffectiveOp(), oldParentId, node.meta()), op, op.newParentId(), op.newMeta(), op.childId())
                ));
            }
        }

        if (oldParentId == null) {
            var conflictNodeId = newParent.children().get(op.newName());

            if (conflictNodeId != null) {
                if (failCreatingIfExists)
                    throw new AlreadyExistsException("Already exists: " + op.newName() + ": " + conflictNodeId);

                var conflictNode = _storage.getById(conflictNodeId);
                MetaT conflictNodeMeta = conflictNode.meta();

                LOGGER.finer(() -> "Node creation conflict: " + conflictNode);

                String newConflictNodeName = op.newName() + ".conflict." + conflictNode.key();
                String newOursName = op.newName() + ".conflict." + op.childId();
                return new LogRecord<>(op, List.of(
                        new LogEffect<>(new LogEffectOld<>(conflictNode.lastEffectiveOp(), newParentId, conflictNodeMeta), conflictNode.lastEffectiveOp(), newParentId, (MetaT) conflictNodeMeta.withName(newConflictNodeName), conflictNodeId),
                        new LogEffect<>(null, op, op.newParentId(), (MetaT) op.newMeta().withName(newOursName), op.childId())
                ));
            } else {
                LOGGER.finer(() -> "Simple node creation");
                return new LogRecord<>(op, List.of(
                        new LogEffect<>(null, op, newParentId, op.newMeta(), op.childId())
                ));
            }
        }

        if (Objects.equals(op.childId(), op.newParentId()) || isAncestor(op.childId(), op.newParentId())) {
            return new LogRecord<>(op, null);
        }

        MetaT oldMeta = node.meta();
        if (oldMeta != null
                && op.newMeta() != null
                && !oldMeta.getClass().equals(op.newMeta().getClass())) {
            throw new RuntimeException("Class mismatch for meta for node " + node.key());
        }

        var replaceNodeId = newParent.children().get(op.newName());
        if (replaceNodeId != null) {
            var replaceNode = _storage.getById(replaceNodeId);
            var replaceNodeMeta = replaceNode.meta();

            LOGGER.finer(() -> "Node replacement: " + replaceNode);

            return new LogRecord<>(op, List.of(
                    new LogEffect<>(new LogEffectOld<>(replaceNode.lastEffectiveOp(), newParentId, replaceNodeMeta), replaceNode.lastEffectiveOp(), _storage.getTrashId(), (MetaT) replaceNodeMeta.withName(replaceNodeId.toString()), replaceNodeId),
                    new LogEffect<>(new LogEffectOld<>(node.lastEffectiveOp(), oldParentId, oldMeta), op, op.newParentId(), op.newMeta(), op.childId())
            ));
        }

        LOGGER.finer(() -> "Simple node move");
        return new LogRecord<>(op, List.of(
                new LogEffect<>(new LogEffectOld<>(node.lastEffectiveOp(), oldParentId, oldMeta), op, op.newParentId(), op.newMeta(), op.childId())
        ));
    }

    /**
     * Check if a node is an ancestor of another node
     *
     * @param child  The child node ID
     * @param parent The parent node ID
     * @return True if the child is an ancestor of the parent, false otherwise
     */
    private boolean isAncestor(NodeIdT child, NodeIdT parent) {
        var node = _storage.getById(parent);
        NodeIdT curParent;
        while ((curParent = node.parent()) != null) {
            if (Objects.equals(child, curParent)) return true;
            node = _storage.getById(curParent);
        }
        return false;
    }

    /**
     * Walk the tree and apply the given consumer to each node
     *
     * @param consumer The consumer to apply to each node
     */
    public void walkTree(Consumer<TreeNode<TimestampT, PeerIdT, MetaT, NodeIdT>> consumer) {
        ArrayDeque<NodeIdT> queue = new ArrayDeque<>();
        queue.push(_storage.getRootId());

        while (!queue.isEmpty()) {
            var id = queue.pop();
            var node = _storage.getById(id);
            if (node == null) continue;
            queue.addAll(node.children().values());
            consumer.accept(node);
        }
    }

    /**
     * Find the parent of a node that matches the given predicate
     *
     * @param kidPredicate The predicate to match the child node
     * @return A pair containing the name of the child and the ID of the parent, or null if not found
     */
    public Pair<String, NodeIdT> findParent(Function<TreeNode<TimestampT, PeerIdT, MetaT, NodeIdT>, Boolean> kidPredicate) {
        ArrayDeque<NodeIdT> queue = new ArrayDeque<>();
        queue.push(_storage.getRootId());

        while (!queue.isEmpty()) {
            var id = queue.pop();
            var node = _storage.getById(id);
            if (node == null) continue;
            var children = node.children();
            for (var childEntry : children.entrySet()) {
                var child = _storage.getById(childEntry.getValue());
                if (kidPredicate.apply(child)) {
                    return Pair.of(childEntry.getKey(), node.key());
                }
            }
            queue.addAll(children.values());
        }
        return null;
    }

    /**
     * Record the bootstrap operations for a given peer
     * Will visit all nodes of the tree and add their effective operations to both the queue to be sent to the peer,
     * and to the global operation log.
     *
     * @param host The peer ID
     */
    public void recordBoostrapFor(PeerIdT host) {
        TreeMap<CombinedTimestamp<TimestampT, PeerIdT>, OpMove<TimestampT, PeerIdT, MetaT, NodeIdT>> result = new TreeMap<>();

        walkTree(node -> {
            var op = node.lastEffectiveOp();
            if (node.lastEffectiveOp() == null) return;
            LOGGER.info("visited bootstrap op for " + host + ": " + op.timestamp().toString() + " " + op.newName() + " " + op.childId() + "->" + op.newParentId());
            result.put(node.lastEffectiveOp().timestamp(), node.lastEffectiveOp());
        });

        for (var le : _storage.getLog().getAll()) {
            var op = le.getValue().op();
            LOGGER.info("bootstrap op from log for " + host + ": " + op.timestamp().toString() + " " + op.newName() + " " + op.childId() + "->" + op.newParentId());
            result.put(le.getKey(), le.getValue().op());
        }

        for (var op : result.values()) {
            LOGGER.info("Recording bootstrap op for " + host + ": " + op.timestamp().toString() + " " + op.newName() + " " + op.childId() + "->" + op.newParentId());
            _opRecorder.recordOpForPeer(host, op);
        }
    }
}
