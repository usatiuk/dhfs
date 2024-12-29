package com.usatiuk.dhfs.objects.jkleppmanntree;

import com.usatiuk.dhfs.objects.TransactionManager;
import com.usatiuk.dhfs.objects.jkleppmanntree.structs.JKleppmannTreeNode;
import com.usatiuk.dhfs.objects.jkleppmanntree.structs.JKleppmannTreeNodeMeta;
import com.usatiuk.dhfs.objects.jkleppmanntree.structs.JKleppmannTreeNodeMetaDirectory;
import com.usatiuk.dhfs.objects.jkleppmanntree.structs.JKleppmannTreePersistentData;
import com.usatiuk.dhfs.objects.transaction.Transaction;
import com.usatiuk.kleppmanntree.*;
import com.usatiuk.objects.alloc.runtime.ObjectAllocator;
import com.usatiuk.objects.common.runtime.JObjectKey;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.commons.lang3.tuple.Pair;

import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;
import java.util.UUID;
import java.util.function.Function;

@ApplicationScoped
public class JKleppmannTreeManager {
    private static final String dataFileName = "trees";
    @Inject
    JKleppmannTreePeerInterface jKleppmannTreePeerInterface;
    @Inject
    Transaction curTx;
    @Inject
    TransactionManager txManager;
    @Inject
    ObjectAllocator objectAllocator;
    @Inject
    JKleppmannTreePeerInterface peerInterface;

    public JKleppmannTree getTree(JObjectKey name) {
        return txManager.executeTx(() -> {
            var data = curTx.get(JKleppmannTreePersistentData.class, name).orElse(null);
            if (data == null) {
                data = objectAllocator.create(JKleppmannTreePersistentData.class, name);
                data.setClock(new AtomicClock(1L));
                data.setQueues(new HashMap<>());
                data.setLog(new TreeMap<>());
                data.setPeerTimestampLog(new HashMap<>());
                curTx.put(data);
            }
            return new JKleppmannTree(data);
//            opObjectRegistry.registerObject(tree);
        });
    }

    public class JKleppmannTree {
        private final KleppmannTree<Long, UUID, JKleppmannTreeNodeMeta, JObjectKey, JKleppmannTreeNodeWrapper> _tree;

        private final JKleppmannTreePersistentData _data;

        private final JKleppmannTreeStorageInterface _storageInterface;
        private final JKleppmannTreeClock _clock;

        private final JObjectKey _treeName;

        JKleppmannTree(JKleppmannTreePersistentData data) {
            _treeName = data.getKey();
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

        public void trash(JKleppmannTreeNodeMeta newMeta, JObjectKey node) {
            _tree.move(_storageInterface.getTrashId(), newMeta.withName(node.name()), node);
        }

//        @Override
//        public boolean hasPendingOpsForHost(UUID host) {
//            return _persistentData.get()
//                    .runReadLocked(JObjectManager.ResolutionStrategy.LOCAL_ONLY,
//                            (m, d) -> d.getQueues().containsKey(host) &&
//                                    !d.getQueues().get(host).isEmpty()
//                    );
//        }
//
//        @Override
//        public List<Op> getPendingOpsForHost(UUID host, int limit) {
//            return _persistentData.get().runReadLocked(JObjectManager.ResolutionStrategy.LOCAL_ONLY, (m, d) -> {
//                if (d.getQueues().containsKey(host)) {
//                    var queue = d.getQueues().get(host);
//                    ArrayList<Op> collected = new ArrayList<>();
//
//                    for (var node : queue.entrySet()) {
//                        collected.add(new JKleppmannTreeOpWrapper(node.getValue()));
//                        if (collected.size() >= limit) break;
//                    }
//
//                    return collected;
//                }
//                return List.of();
//            });
//        }

//        @Override
//        public String getId() {
//            return _treeName;
//        }

//        @Override
//        public void commitOpForHost(UUID host, Op op) {
//            if (!(op instanceof JKleppmannTreeOpWrapper jop))
//                throw new IllegalArgumentException("Invalid incoming op type for JKleppmannTree: " + op.getClass() + " " + getId());
//            _persistentData.get().assertRwLock();
//            _persistentData.get().tryResolve(JObjectManager.ResolutionStrategy.LOCAL_ONLY);
//
//            var got = _persistentData.get().getData().getQueues().get(host).firstEntry().getValue();
//            if (!Objects.equals(jop.getOp(), got))
//                throw new IllegalArgumentException("Committed op push was not the oldest");
//
//            _persistentData.get().mutate(new JMutator<JKleppmannTreePersistentData>() {
//                @Override
//                public boolean mutate(JKleppmannTreePersistentData object) {
//                    object.getQueues().get(host).pollFirstEntry();
//                    return true;
//                }
//
//                @Override
//                public void revert(JKleppmannTreePersistentData object) {
//                    object.getQueues().get(host).put(jop.getOp().timestamp(), jop.getOp());
//                }
//            });
//
//        }

//        @Override
//        public void pushBootstrap(UUID host) {
//            _tree.recordBoostrapFor(host);
//        }

        public Pair<String, JObjectKey> findParent(Function<JKleppmannTreeNodeWrapper, Boolean> predicate) {
            return _tree.findParent(predicate);
        }

//        @Override
//        public boolean acceptExternalOp(UUID from, Op op) {
//            if (op instanceof JKleppmannTreePeriodicPushOp pushOp) {
//                return _tree.updateExternalTimestamp(pushOp.getFrom(), pushOp.getTimestamp());
//            }
//
//            if (!(op instanceof JKleppmannTreeOpWrapper jop))
//                throw new IllegalArgumentException("Invalid incoming op type for JKleppmannTree: " + op.getClass() + " " + getId());
//
//            JObject<?> fileRef;
//            if (jop.getOp().newMeta() instanceof JKleppmannTreeNodeMetaFile f) {
//                var fino = f.getFileIno();
//                fileRef = jObjectManager.getOrPut(fino, File.class, Optional.of(jop.getOp().childId()));
//            } else {
//                fileRef = null;
//            }
//
//            if (Log.isTraceEnabled())
//                Log.trace("Received op from " + from + ": " + jop.getOp().timestamp().timestamp() + " " + jop.getOp().childId() + "->" + jop.getOp().newParentId() + " as " + jop.getOp().newMeta().getName());
//
//            try {
//                _tree.applyExternalOp(from, jop.getOp());
//            } catch (Exception e) {
//                Log.error("Error applying external op", e);
//                throw e;
//            } finally {
//                // FIXME:
//                // Fixup the ref if it didn't really get applied
//
//                if ((fileRef == null) && (jop.getOp().newMeta() instanceof JKleppmannTreeNodeMetaFile))
//                    Log.error("Could not create child of pushed op: " + jop.getOp());
//
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
//            }
//            return true;
//        }

//        @Override
//        public Op getPeriodicPushOp() {
//            return new JKleppmannTreePeriodicPushOp(persistentPeerDataService.getSelfUuid(), _clock.peekTimestamp());
//        }

//        @Override
//        public void addToTx() {
//            // FIXME: a hack
//            _persistentData.get().rwLockNoCopy();
//            _persistentData.get().rwUnlock();
//        }

        private class JOpRecorder implements OpRecorder<Long, UUID, JKleppmannTreeNodeMeta, JObjectKey> {
            @Override
            public void recordOp(OpMove<Long, UUID, JKleppmannTreeNodeMeta, JObjectKey> op) {
//                _persistentData.get().assertRwLock();
//                _persistentData.get().tryResolve(JObjectManager.ResolutionStrategy.LOCAL_ONLY);
//                var hostUuds = persistentPeerDataService.getHostUuids().stream().toList();
//                _persistentData.get().mutate(new JMutator<JKleppmannTreePersistentData>() {
//                    @Override
//                    public boolean mutate(JKleppmannTreePersistentData object) {
//                        object.recordOp(hostUuds, op);
//                        return true;
//                    }
//
//                    @Override
//                    public void revert(JKleppmannTreePersistentData object) {
//                        object.removeOp(hostUuds, op);
//                    }
//                });
//                opSender.push(JKleppmannTree.this);
            }

            @Override
            public void recordOpForPeer(UUID peer, OpMove<Long, UUID, JKleppmannTreeNodeMeta, JObjectKey> op) {
//                _persistentData.get().assertRwLock();
//                _persistentData.get().tryResolve(JObjectManager.ResolutionStrategy.LOCAL_ONLY);
//                _persistentData.get().mutate(new JMutator<JKleppmannTreePersistentData>() {
//                    @Override
//                    public boolean mutate(JKleppmannTreePersistentData object) {
//                        object.recordOp(peer, op);
//                        return true;
//                    }
//
//                    @Override
//                    public void revert(JKleppmannTreePersistentData object) {
//                        object.removeOp(peer, op);
//                    }
//                });
//                opSender.push(JKleppmannTree.this);
            }
        }

        private class JKleppmannTreeClock implements Clock<Long> {
            @Override
            public Long getTimestamp() {
                return _data.getClock().getTimestamp();
            }

            @Override
            public Long peekTimestamp() {
                return _data.getClock().peekTimestamp();
            }

            @Override
            public Long updateTimestamp(Long receivedTimestamp) {
                return _data.getClock().updateTimestamp(receivedTimestamp);
            }
        }

        public class JKleppmannTreeStorageInterface implements StorageInterface<Long, UUID, JKleppmannTreeNodeMeta, JObjectKey, JKleppmannTreeNodeWrapper> {
            private final LogWrapper _logWrapper = new LogWrapper();
            private final PeerLogWrapper _peerLogWrapper = new PeerLogWrapper();

            public JKleppmannTreeStorageInterface() {
                if (curTx.get(JKleppmannTreeNode.class, getRootId()).isEmpty()) {
                    var rootNode = objectAllocator.create(JKleppmannTreeNode.class, getRootId());
                    rootNode.setNode(new TreeNode<>(getRootId(), null, new JKleppmannTreeNodeMetaDirectory("")));
                    rootNode.setRefsFrom(List.of());
                    curTx.put(rootNode);
                    var trashNode = objectAllocator.create(JKleppmannTreeNode.class, getTrashId());
                    trashNode.setRefsFrom(List.of());
                    trashNode.setNode(new TreeNode<>(getTrashId(), null, new JKleppmannTreeNodeMetaDirectory("")));
                    curTx.put(trashNode);
                }
            }

            @Override
            public JObjectKey getRootId() {
                return new JObjectKey(_treeName.name() + "_jt_root");
            }

            @Override
            public JObjectKey getTrashId() {
                return new JObjectKey(_treeName.name() + "_jt_trash");
            }

            @Override
            public JObjectKey getNewNodeId() {
                return new JObjectKey(UUID.randomUUID().toString());
            }

            @Override
            public JKleppmannTreeNodeWrapper getById(JObjectKey id) {
                var got = curTx.get(JKleppmannTreeNode.class, id);
                if (got.isEmpty()) return null;
                return new JKleppmannTreeNodeWrapper(got.get());
            }

            @Override
            public JKleppmannTreeNodeWrapper createNewNode(TreeNode<Long, UUID, JKleppmannTreeNodeMeta, JObjectKey> node) {
                var created = objectAllocator.create(JKleppmannTreeNode.class, node.getId());
                created.setNode(node);
                created.setRefsFrom(List.of());
                curTx.put(created);
                return new JKleppmannTreeNodeWrapper(created);
            }

            @Override
            public void removeNode(JObjectKey id) {
                // TODO
            }

            @Override
            public LogInterface<Long, UUID, JKleppmannTreeNodeMeta, JObjectKey> getLog() {
                return _logWrapper;
            }

            @Override
            public PeerTimestampLogInterface<Long, UUID> getPeerTimestampLog() {
                return _peerLogWrapper;
            }

            @Override
            public void rLock() {
            }

            @Override
            public void rUnlock() {
            }

            @Override
            public void rwLock() {
            }

            @Override
            public void rwUnlock() {
            }

            @Override
            public void assertRwLock() {
            }

            private class PeerLogWrapper implements PeerTimestampLogInterface<Long, UUID> {
                @Override
                public Long getForPeer(UUID peerId) {
                    return _data.getPeerTimestampLog().get(peerId);
                }

                @Override
                public void putForPeer(UUID peerId, Long timestamp) {
                    _data.getPeerTimestampLog().put(peerId, timestamp);
                }
            }

            private class LogWrapper implements LogInterface<Long, UUID, JKleppmannTreeNodeMeta, JObjectKey> {
                @Override
                public Pair<CombinedTimestamp<Long, UUID>, LogRecord<Long, UUID, JKleppmannTreeNodeMeta, JObjectKey>> peekOldest() {
                    var ret = _data.getLog().firstEntry();
                    if (ret == null) return null;
                    return Pair.of(ret);
                }

                @Override
                public Pair<CombinedTimestamp<Long, UUID>, LogRecord<Long, UUID, JKleppmannTreeNodeMeta, JObjectKey>> takeOldest() {
                    var ret = _data.getLog().pollFirstEntry();
                    if (ret == null) return null;
                    return Pair.of(ret);
                }

                @Override
                public Pair<CombinedTimestamp<Long, UUID>, LogRecord<Long, UUID, JKleppmannTreeNodeMeta, JObjectKey>> peekNewest() {
                    var ret = _data.getLog().lastEntry();
                    if (ret == null) return null;
                    return Pair.of(ret);
                }

                @Override
                public List<Pair<CombinedTimestamp<Long, UUID>, LogRecord<Long, UUID, JKleppmannTreeNodeMeta, JObjectKey>>> newestSlice(CombinedTimestamp<Long, UUID> since, boolean inclusive) {
                    return _data.getLog().tailMap(since, inclusive).entrySet().stream().map(e -> Pair.of(e.getKey(), e.getValue())).toList();
                }

                @Override
                public List<Pair<CombinedTimestamp<Long, UUID>, LogRecord<Long, UUID, JKleppmannTreeNodeMeta, JObjectKey>>> getAll() {
                    return _data.getLog().entrySet().stream().map(e -> Pair.of(e.getKey(), e.getValue())).toList();
                }

                @Override
                public boolean isEmpty() {
                    return _data.getLog().isEmpty();
                }

                @Override
                public boolean containsKey(CombinedTimestamp<Long, UUID> timestamp) {
                    return _data.getLog().containsKey(timestamp);
                }

                @Override
                public long size() {
                    return (long) _data.getLog().size();
                }

                @Override
                public void put(CombinedTimestamp<Long, UUID> timestamp, LogRecord<Long, UUID, JKleppmannTreeNodeMeta, JObjectKey> record) {
                    if (_data.getLog().containsKey(timestamp))
                        throw new IllegalStateException("Overwriting log entry?");
                    _data.getLog().put(timestamp, record);
                }

                @Override
                public void replace(CombinedTimestamp<Long, UUID> timestamp, LogRecord<Long, UUID, JKleppmannTreeNodeMeta, JObjectKey> record) {
                    _data.getLog().put(timestamp, record);
                }
            }
        }
    }
}
