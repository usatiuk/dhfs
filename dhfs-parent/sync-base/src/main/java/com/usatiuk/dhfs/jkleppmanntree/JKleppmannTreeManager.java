package com.usatiuk.dhfs.jkleppmanntree;

import com.google.protobuf.ByteString;
import com.usatiuk.objects.JObjectKey;
import com.usatiuk.dhfs.PeerId;
import com.usatiuk.dhfs.jkleppmanntree.structs.*;
import com.usatiuk.dhfs.repository.PersistentPeerDataService;
import com.usatiuk.dhfs.repository.invalidation.Op;
import com.usatiuk.dhfs.repository.peersync.PeerInfoService;
import com.usatiuk.objects.transaction.LockingStrategy;
import com.usatiuk.objects.transaction.Transaction;
import com.usatiuk.objects.transaction.TransactionManager;
import com.usatiuk.kleppmanntree.*;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.commons.lang3.tuple.Pair;
import org.pcollections.HashTreePMap;
import org.pcollections.TreePMap;
import org.pcollections.TreePSet;

import java.util.*;
import java.util.function.Function;

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

    public static final ByteString ROOT_SUFFIX = ByteString.copyFromUtf8("_jt_root");
    public static final ByteString TRASH_SUFFIX = ByteString.copyFromUtf8("_jt_trash");
    public static final ByteString LF_SUFFIX = ByteString.copyFromUtf8("_jt_lf");

    public JKleppmannTree getTree(JObjectKey name, LockingStrategy lockingStrategy) {
        return txManager.executeTx(() -> {
            var data = curTx.get(JKleppmannTreePersistentData.class, name, lockingStrategy).orElse(null);
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
                var rootNode = new JKleppmannTreeNode(JObjectKey.of(name.value().concat(ROOT_SUFFIX)), null, new JKleppmannTreeNodeMetaDirectory(""));
                curTx.put(rootNode);
                var trashNode = new JKleppmannTreeNode(JObjectKey.of(name.value().concat(TRASH_SUFFIX)), null, new JKleppmannTreeNodeMetaDirectory(""));
                curTx.put(trashNode);
                var lf_node = new JKleppmannTreeNode(JObjectKey.of(name.value().concat(LF_SUFFIX)), null, new JKleppmannTreeNodeMetaDirectory(""));
                curTx.put(lf_node);
            }
            return new JKleppmannTree(data);
//            opObjectRegistry.registerObject(tree);
        });
    }

    public JKleppmannTree getTree(JObjectKey name) {
        return getTree(name, LockingStrategy.WRITE);
    }

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

        public JObjectKey traverse(List<String> names) {
            return _tree.traverse(names);
        }

        public JObjectKey getNewNodeId() {
            return _storageInterface.getNewNodeId();
        }

        public void move(JObjectKey newParent, JKleppmannTreeNodeMeta newMeta, JObjectKey node) {
            _tree.move(newParent, newMeta, node);
        }

        public void trash(JKleppmannTreeNodeMeta newMeta, JObjectKey nodeKey) {
            _tree.move(_storageInterface.getTrashId(), newMeta.withName(nodeKey.toString()), nodeKey);
        }

        public boolean hasPendingOpsForHost(PeerId host) {
            return !_data.queues().getOrDefault(host, TreePMap.empty()).isEmpty();
        }

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

        //        @Override
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

        public void recordBootstrap(PeerId host) {
            _tree.recordBoostrapFor(host);
        }

        public Pair<String, JObjectKey> findParent(Function<TreeNode<Long, PeerId, JKleppmannTreeNodeMeta, JObjectKey>, Boolean> predicate) {
            return _tree.findParent(predicate);
        }

        //        @Override
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
                Log.trace("Received op from " + from + ": " + jop.op().timestamp().timestamp() + " " + jop.op().childId() + "->" + jop.op().newParentId() + " as " + jop.op().newMeta().getName());

            try {
                _tree.applyExternalOp(from, jop.op());
            } catch (Exception e) {
                Log.error("Error applying external op", e);
                throw e;
            } finally {
                // FIXME:
                // Fixup the ref if it didn't really get applied

//                if ((fileRef == null) && (jop.getOp().newMeta() instanceof JKleppmannTreeNodeMetaFile))
//                    Log.error("Could not create child of pushed op: " + jop.getOp());

//                if (jop.getOp().newMeta() instanceof JKleppmannTreeNodeMetaFile f) {
//                    if (fileRef != null) {
//                        var got = jObjectManager.get(jop.getOp().childId()).orElse(null);
//
//                        VoidFn remove = () -> {
//                            fileRef.runWriteLockedVoid(JObjectManager.ResolutionStrategy.LOCAL_ONLY, (m, d, b, v) -> {
//                                m.removeRef(jop.getOp().childId());
//                            });
//                        };
//
//                        if (got == null) {
//                            remove.apply();
//                        } else {
//                            try {
//                                got.rLock();
//                                try {
//                                    got.tryResolve(JObjectManager.ResolutionStrategy.LOCAL_ONLY);
//                                    if (got.getData() == null || !got.getData().extractRefs().contains(f.getFileIno()))
//                                        remove.apply();
//                                } finally {
//                                    got.rUnlock();
//                                }
//                            } catch (DeletedObjectAccessException dex) {
//                                remove.apply();
//                            }
//                        }
//                    }
//                }
            }
        }

        public Op getPeriodicPushOp() {
            return new JKleppmannTreePeriodicPushOp(_treeName, persistentPeerDataService.getSelfUuid(), _clock.peekTimestamp());
        }

//        @Override
//        public void addToTx() {
//            // FIXME: a hack
//            _persistentData.get().rwLockNoCopy();
//            _persistentData.get().rwUnlock();
//        }

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
                return JObjectKey.of(_treeName.value().concat(ROOT_SUFFIX));
            }

            @Override
            public JObjectKey getTrashId() {
                return JObjectKey.of(_treeName.value().concat(TRASH_SUFFIX));
            }

            @Override
            public JObjectKey getLostFoundId() {
                return JObjectKey.of(_treeName.value().concat(LF_SUFFIX));
            }

            @Override
            public JObjectKey getNewNodeId() {
                return JObjectKey.of(UUID.randomUUID().toString());
            }

            @Override
            public JKleppmannTreeNode getById(JObjectKey id) {
                var got = curTx.get(JKleppmannTreeNode.class, id);
                return got.orElse(null);
            }

            @Override
            public JKleppmannTreeNode createNewNode(JObjectKey key, JObjectKey parent, JKleppmannTreeNodeMeta meta) {
                return new JKleppmannTreeNode(key, parent, meta);
            }

            @Override
            public void putNode(TreeNode<Long, PeerId, JKleppmannTreeNodeMeta, JObjectKey> node) {
                curTx.put(((JKleppmannTreeNode) node));
            }

            @Override
            public void removeNode(JObjectKey id) {
                // TODO
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
