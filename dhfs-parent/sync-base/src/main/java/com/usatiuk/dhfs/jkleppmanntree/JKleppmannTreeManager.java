package com.usatiuk.dhfs.jkleppmanntree;

import com.usatiuk.dhfs.invalidation.Op;
import com.usatiuk.dhfs.jkleppmanntree.structs.JKleppmannTreeNode;
import com.usatiuk.dhfs.jkleppmanntree.structs.JKleppmannTreeNodeHolder;
import com.usatiuk.dhfs.jkleppmanntree.structs.JKleppmannTreeNodeMeta;
import com.usatiuk.dhfs.jkleppmanntree.structs.JKleppmannTreePersistentData;
import com.usatiuk.dhfs.peersync.PeerId;
import com.usatiuk.dhfs.peersync.PeerInfoService;
import com.usatiuk.dhfs.peersync.PersistentPeerDataService;
import com.usatiuk.kleppmanntree.*;
import com.usatiuk.objects.JObjectKey;
import com.usatiuk.objects.transaction.Transaction;
import com.usatiuk.objects.transaction.TransactionManager;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.commons.lang3.tuple.Pair;
import org.pcollections.HashTreePMap;
import org.pcollections.TreePMap;
import org.pcollections.TreePSet;

import java.util.*;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Automatically synchronized and persistent Kleppmann tree service.
 * The trees are identified by their names, and can have any type of root node.
 */
@ApplicationScoped
public class JKleppmannTreeManager {
    private static final String dataFileName = "trees";
    @Inject
    Transaction curTx;
    @Inject
    TransactionManager txManager;
    @Inject
    JKleppmannTreePeerInterface peerInterface;
    @Inject
    PeerInfoService peerInfoService;
    @Inject
    PersistentPeerDataService persistentPeerDataService;

    /**
     * Get or create a tree with the given name.
     * @param name the name of the tree
     * @param rootNodeSupplier a supplier for the root node meta
     * @return the tree
     */
    public JKleppmannTree getTree(JObjectKey name, Supplier<JKleppmannTreeNodeMeta> rootNodeSupplier) {
        return txManager.executeTx(() -> {
            var data = curTx.get(JKleppmannTreePersistentData.class, name).orElse(null);
            if (data == null) {
                data = new JKleppmannTreePersistentData(
                        name,
                        TreePSet.empty(),
                        true,
                        1L,
                        HashTreePMap.empty(),
                        HashTreePMap.empty(),
                        TreePMap.empty()
                );
                curTx.put(data);
                var rootNode = new JKleppmannTreeNode(JObjectKey.of(name.value() + "_jt_root"), null, rootNodeSupplier.get());
                curTx.put(new JKleppmannTreeNodeHolder(rootNode, true));
                var trashNode = new JKleppmannTreeNode(JObjectKey.of(name.value() + "_jt_trash"), null, rootNodeSupplier.get());
                curTx.put(new JKleppmannTreeNodeHolder(trashNode, true));
                var lf_node = new JKleppmannTreeNode(JObjectKey.of(name.value() + "_jt_lf"), null, rootNodeSupplier.get());
                curTx.put(new JKleppmannTreeNodeHolder(lf_node, true));
            }
            return new JKleppmannTree(data);
//            opObjectRegistry.registerObject(tree);
        });
    }

    /**
     * Get a tree with the given name.
     * @param name the name of the tree
     * @return the tree
     */
    public Optional<JKleppmannTree> getTree(JObjectKey name) {
        return txManager.executeTx(() -> {
            return curTx.get(JKleppmannTreePersistentData.class, name).map(JKleppmannTree::new);
        });
    }

    /**
     * Kleppmann tree wrapper, automatically synchronized and persistent.
     */
    public class JKleppmannTree {
        private final KleppmannTree<Long, PeerId, JKleppmannTreeNodeMeta, JObjectKey> _tree;
        private final JKleppmannTreeStorageInterface _storageInterface;
        private final JKleppmannTreeClock _clock;
        private final JObjectKey _treeName;
        private JKleppmannTreePersistentData _data;

        JKleppmannTree(JKleppmannTreePersistentData data) {
            _treeName = data.key();
            _data = data;

            _storageInterface = new JKleppmannTreeStorageInterface();
            _clock = new JKleppmannTreeClock();

            _tree = new KleppmannTree<>(_storageInterface, peerInterface, _clock, new JOpRecorder());
        }

        /**
         * Traverse the tree from root to find a node with the given name.
         * @param names list of names to traverse
         * @return the node key
         */
        public JObjectKey traverse(List<String> names) {
            return _tree.traverse(names);
        }

        /**
         * Get a new node id. (random)
         * @return the new node id
         */
        public JObjectKey getNewNodeId() {
            return _storageInterface.getNewNodeId();
        }

        /**
         * Move a node to a new parent.
         * @param newParent the new parent
         * @param newMeta the new node metadata
         * @param node the node to move
         */
        public void move(JObjectKey newParent, JKleppmannTreeNodeMeta newMeta, JObjectKey node) {
            _tree.move(newParent, newMeta, node);
        }

        /**
         * Move a node to the trash.
         * @param newMeta the new node metadata
         * @param nodeKey the node key
         */
        public void trash(JKleppmannTreeNodeMeta newMeta, JObjectKey nodeKey) {
            _tree.move(_storageInterface.getTrashId(), newMeta.withName(nodeKey.toString()), nodeKey);
        }

        /**
         * Check if there are any pending operations for the given peer.
         * @param host the peer id
         * @return true if there are pending operations, false otherwise
         */
        public boolean hasPendingOpsForHost(PeerId host) {
            return !_data.queues().getOrDefault(host, TreePMap.empty()).isEmpty();
        }

        /**
         * Get the pending operations for the given peer.
         * @param host the peer id
         * @param limit the maximum number of operations to return
         * @return the list of pending operations
         */
        public List<Op> getPendingOpsForHost(PeerId host, int limit) {
            ArrayList<Op> collected = new ArrayList<>();
            for (var node : _data.queues().getOrDefault(host, TreePMap.empty()).entrySet()) {
                collected.add(new JKleppmannTreeOpWrapper(_data.key(), node.getValue()));
                if (collected.size() >= limit) break;
            }
            Log.tracev("Collected pending op for host: {0} - {1}, out of {2}", host, collected,
                    _data.queues().getOrDefault(host, TreePMap.empty()));
            return Collections.unmodifiableList(collected);
        }

        /**
         * Mark the operation as committed for the given host.
         * This should be called when the operation is successfully applied on the host.
         * All operations should be sent and received in timestamp order.
         * @param host the peer id
         * @param op the operation to commit
         */
        public void commitOpForHost(PeerId host, Op op) {
            if (op instanceof JKleppmannTreePeriodicPushOp)
                return;

            if (!(op instanceof JKleppmannTreeOpWrapper jop))
                throw new IllegalArgumentException("Invalid incoming op type for JKleppmannTree: " + op.getClass());

            var firstOp = _data.queues().get(host).firstEntry();
            if (!Objects.equals(firstOp.getValue(), jop.op()))
                throw new IllegalArgumentException("Committed op push was not the oldest");

            _data = _data.withQueues(_data.queues().plus(host, _data.queues().get(host).minus(firstOp.getKey())));
            curTx.put(_data);
        }

        /**
         * Record bootstrap operations for the given host.
         * @param host the peer id
         */
        public void recordBootstrap(PeerId host) {
            _tree.recordBoostrapFor(host);
        }

        /**
         * Get the parent of a node that matches the given predicate.
         * @param predicate the predicate to match
         */
        public Pair<String, JObjectKey> findParent(Function<TreeNode<Long, PeerId, JKleppmannTreeNodeMeta, JObjectKey>, Boolean> predicate) {
            return _tree.findParent(predicate);
        }

        /**
         * Accept an external operation from the given peer.
         * @param from the peer id
         * @param op the operation to accept
         */
        public void acceptExternalOp(PeerId from, Op op) {
            if (op instanceof JKleppmannTreePeriodicPushOp(JObjectKey treeName, PeerId from1, long timestamp)) {
                _tree.updateExternalTimestamp(from1, timestamp);
                return;
            }

            if (!(op instanceof JKleppmannTreeOpWrapper jop))
                throw new IllegalArgumentException("Invalid incoming op type for JKleppmannTree: " + op.getClass());

//            if (jop.op().newMeta() instanceof JKleppmannTreeNodeMetaFile f) {
//                var fino = f.getFileIno();
//                fileRef = jObjectManager.getOrPut(fino, File.class, Optional.of(jop.getOp().childId()));
//            } else {
//                fileRef = null;
//            }

            if (Log.isTraceEnabled())
                Log.trace("Received op from " + from + ": " + jop.op().timestamp().timestamp() + " " + jop.op().childId() + "->" + jop.op().newParentId() + " as " + jop.op().newMeta().name());

            _tree.applyExternalOp(from, jop.op());
        }

        /**
         * Create a dummy operation that contains the timestamp of the last operation, to move causality threshold
         * forward even without any real operations.
         * @return the periodic push operation
         */
        public Op getPeriodicPushOp() {
            return new JKleppmannTreePeriodicPushOp(_treeName, persistentPeerDataService.getSelfUuid(), _clock.peekTimestamp());
        }

        private class JOpRecorder implements OpRecorder<Long, PeerId, JKleppmannTreeNodeMeta, JObjectKey> {
            @Override
            public void recordOp(OpMove<Long, PeerId, JKleppmannTreeNodeMeta, JObjectKey> op) {
                for (var p : peerInfoService.getPeersNoSelf()) {
                    recordOpForPeer(p.id(), op);
                }
            }

            @Override
            public void recordOpForPeer(PeerId peer, OpMove<Long, PeerId, JKleppmannTreeNodeMeta, JObjectKey> op) {
                _data = _data.withQueues(_data.queues().plus(peer, _data.queues().getOrDefault(peer, TreePMap.empty()).plus(op.timestamp(), op)));
                curTx.put(_data);
            }
        }

        private class JKleppmannTreeClock implements Clock<Long> {
            @Override
            public Long getTimestamp() {
                var res = _data.clock() + 1;
                _data = _data.withClock(res);
                curTx.put(_data);
                return res;
            }

            @Override
            public Long peekTimestamp() {
                return _data.clock();
            }

            @Override
            public Long updateTimestamp(Long receivedTimestamp) {
                var old = _data.clock();
                _data = _data.withClock(Math.max(old, receivedTimestamp) + 1);
                curTx.put(_data);
                return old;
            }
        }

        public class JKleppmannTreeStorageInterface implements StorageInterface<Long, PeerId, JKleppmannTreeNodeMeta, JObjectKey> {
            private final LogWrapper _logWrapper = new LogWrapper();
            private final PeerLogWrapper _peerLogWrapper = new PeerLogWrapper();

            public JKleppmannTreeStorageInterface() {
            }

            @Override
            public JObjectKey getRootId() {
                return JObjectKey.of(_treeName.value() + "_jt_root");
            }

            @Override
            public JObjectKey getTrashId() {
                return JObjectKey.of(_treeName.value() + "_jt_trash");
            }

            @Override
            public JObjectKey getLostFoundId() {
                return JObjectKey.of(_treeName.value() + "_jt_lf");
            }

            @Override
            public JObjectKey getNewNodeId() {
                return JObjectKey.of(UUID.randomUUID().toString());
            }

            @Override
            public JKleppmannTreeNode getById(JObjectKey id) {
                var got = curTx.get(JKleppmannTreeNodeHolder.class, id);
                return got.map(JKleppmannTreeNodeHolder::node).orElse(null);
            }

            @Override
            public JKleppmannTreeNode createNewNode(JObjectKey key, JObjectKey parent, JKleppmannTreeNodeMeta meta) {
                return new JKleppmannTreeNode(key, parent, meta);
            }

            @Override
            public void putNode(TreeNode<Long, PeerId, JKleppmannTreeNodeMeta, JObjectKey> node) {
                curTx.put(curTx.get(JKleppmannTreeNodeHolder.class, node.key())
                        .map(n -> n.withNode((JKleppmannTreeNode) node))
                        .orElse(new JKleppmannTreeNodeHolder((JKleppmannTreeNode) node)));
            }

            @Override
            public void removeNode(JObjectKey id) {
                // GC
            }

            @Override
            public LogInterface<Long, PeerId, JKleppmannTreeNodeMeta, JObjectKey> getLog() {
                return _logWrapper;
            }

            @Override
            public PeerTimestampLogInterface<Long, PeerId> getPeerTimestampLog() {
                return _peerLogWrapper;
            }

            private class PeerLogWrapper implements PeerTimestampLogInterface<Long, PeerId> {
                @Override
                public Long getForPeer(PeerId peerId) {
                    return _data.peerTimestampLog().get(peerId);
                }

                @Override
                public void putForPeer(PeerId peerId, Long timestamp) {
                    _data = _data.withPeerTimestampLog(_data.peerTimestampLog().plus(peerId, timestamp));
                    curTx.put(_data);
                }
            }

            private class LogWrapper implements LogInterface<Long, PeerId, JKleppmannTreeNodeMeta, JObjectKey> {
                @Override
                public Pair<CombinedTimestamp<Long, PeerId>, LogRecord<Long, PeerId, JKleppmannTreeNodeMeta, JObjectKey>> peekOldest() {
                    if (_data.log().isEmpty()) return null;
                    return Pair.of(_data.log().firstEntry());
                }

                @Override
                public Pair<CombinedTimestamp<Long, PeerId>, LogRecord<Long, PeerId, JKleppmannTreeNodeMeta, JObjectKey>> takeOldest() {
                    if (_data.log().isEmpty()) return null;
                    var ret = _data.log().firstEntry();
                    _data = _data.withLog(_data.log().minusFirstEntry());
                    curTx.put(_data);
                    return Pair.of(ret);
                }

                @Override
                public Pair<CombinedTimestamp<Long, PeerId>, LogRecord<Long, PeerId, JKleppmannTreeNodeMeta, JObjectKey>> peekNewest() {
                    if (_data.log().isEmpty()) return null;
                    return Pair.of(_data.log().lastEntry());
                }

                @Override
                public List<Pair<CombinedTimestamp<Long, PeerId>, LogRecord<Long, PeerId, JKleppmannTreeNodeMeta, JObjectKey>>> newestSlice(CombinedTimestamp<Long, PeerId> since, boolean inclusive) {
                    return _data.log().tailMap(since, inclusive).entrySet().stream().map(e -> Pair.of(e.getKey(), e.getValue())).toList();
                }

                @Override
                public List<Pair<CombinedTimestamp<Long, PeerId>, LogRecord<Long, PeerId, JKleppmannTreeNodeMeta, JObjectKey>>> getAll() {
                    return _data.log().entrySet().stream().map(e -> Pair.of(e.getKey(), e.getValue())).toList();
                }

                @Override
                public boolean isEmpty() {
                    return _data.log().isEmpty();
                }

                @Override
                public boolean containsKey(CombinedTimestamp<Long, PeerId> timestamp) {
                    return _data.log().containsKey(timestamp);
                }

                @Override
                public long size() {
                    return _data.log().size();
                }

                @Override
                public void put(CombinedTimestamp<Long, PeerId> timestamp, LogRecord<Long, PeerId, JKleppmannTreeNodeMeta, JObjectKey> record) {
                    if (_data.log().containsKey(timestamp))
                        throw new IllegalStateException("Overwriting log entry?");
                    _data = _data.withLog(_data.log().plus(timestamp, record));
                    curTx.put(_data);
                }

                @Override
                public void replace(CombinedTimestamp<Long, PeerId> timestamp, LogRecord<Long, PeerId, JKleppmannTreeNodeMeta, JObjectKey> record) {
                    _data = _data.withLog(_data.log().plus(timestamp, record));
                    curTx.put(_data);
                }
            }
        }
    }
}
