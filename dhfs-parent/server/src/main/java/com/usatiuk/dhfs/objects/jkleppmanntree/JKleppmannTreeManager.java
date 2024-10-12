package com.usatiuk.dhfs.objects.jkleppmanntree;

import com.usatiuk.dhfs.files.objects.File;
import com.usatiuk.dhfs.objects.jkleppmanntree.structs.*;
import com.usatiuk.dhfs.objects.jrepository.*;
import com.usatiuk.dhfs.objects.repository.PersistentPeerDataService;
import com.usatiuk.dhfs.objects.repository.opsupport.Op;
import com.usatiuk.dhfs.objects.repository.opsupport.OpObject;
import com.usatiuk.dhfs.objects.repository.opsupport.OpObjectRegistry;
import com.usatiuk.dhfs.objects.repository.opsupport.OpSender;
import com.usatiuk.kleppmanntree.*;
import com.usatiuk.utils.VoidFn;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.commons.lang3.tuple.Pair;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

@ApplicationScoped
public class JKleppmannTreeManager {
    private static final String dataFileName = "trees";
    private final ConcurrentHashMap<String, JKleppmannTree> _trees = new ConcurrentHashMap<>();
    @Inject
    JKleppmannTreePeerInterface jKleppmannTreePeerInterface;
    @Inject
    OpSender opSender;
    @Inject
    OpObjectRegistry opObjectRegistry;
    @Inject
    JObjectManager jObjectManager;
    @Inject
    PersistentPeerDataService persistentPeerDataService;
    @Inject
    JObjectTxManager jObjectTxManager;
    @Inject
    SoftJObjectFactory softJObjectFactory;
    @Inject
    JKleppmannTreePeerInterface peerInterface;

    public JKleppmannTree getTree(String name) {
        return _trees.computeIfAbsent(name, this::createTree);
    }

    private JKleppmannTree createTree(String name) {
        return jObjectTxManager.executeTx(() -> {
            var data = jObjectManager.get(JKleppmannTreePersistentData.nameFromTreeName(name)).orElse(null);
            if (data == null) {
                data = jObjectManager.put(new JKleppmannTreePersistentData(name), Optional.empty());
            }
            var tree = new JKleppmannTree(name);
            opObjectRegistry.registerObject(tree);
            return tree;
        });
    }

    public class JKleppmannTree implements OpObject {
        private final KleppmannTree<Long, UUID, JKleppmannTreeNodeMeta, String, JKleppmannTreeNodeWrapper> _tree;

        private final SoftJObject<JKleppmannTreePersistentData> _persistentData;

        private final JKleppmannTreeStorageInterface _storageInterface;
        private final JKleppmannTreeClock _clock;

        private final String _treeName;

        JKleppmannTree(String treeName) {
            _treeName = treeName;

            _persistentData = softJObjectFactory.create(JKleppmannTreePersistentData.nameFromTreeName(treeName));

            _storageInterface = new JKleppmannTreeStorageInterface();
            _clock = new JKleppmannTreeClock();

            _tree = new KleppmannTree<>(_storageInterface, peerInterface, _clock, new JOpRecorder());
        }

        public String traverse(List<String> names) {
            return _tree.traverse(names);
        }

        public String getNewNodeId() {
            return _storageInterface.getNewNodeId();
        }

        public void move(String newParent, JKleppmannTreeNodeMeta newMeta, String node) {
            _tree.move(newParent, newMeta, node);
        }

        public void trash(JKleppmannTreeNodeMeta newMeta, String node) {
            _tree.move(_storageInterface.getTrashId(), newMeta.withName(node), node);
        }

        @Override
        public List<Op> getPendingOpsForHost(UUID host, int limit) {
            return _persistentData.get().runReadLocked(JObjectManager.ResolutionStrategy.LOCAL_ONLY, (m, d) -> {
                if (d.getQueues().containsKey(host)) {
                    var queue = d.getQueues().get(host);
                    ArrayList<Op> collected = new ArrayList<>();

                    for (var node : queue.entrySet()) {
                        collected.add(new JKleppmannTreeOpWrapper(node.getValue()));
                        if (collected.size() >= limit) break;
                    }

                    return collected;
                }
                return List.of();
            });
        }

        @Override
        public String getId() {
            return _treeName;
        }

        @Override
        public void commitOpForHost(UUID host, Op op) {
            if (!(op instanceof JKleppmannTreeOpWrapper jop))
                throw new IllegalArgumentException("Invalid incoming op type for JKleppmannTree: " + op.getClass() + " " + getId());
            _persistentData.get().assertRwLock();
            _persistentData.get().tryResolve(JObjectManager.ResolutionStrategy.LOCAL_ONLY);

            var got = _persistentData.get().getData().getQueues().get(host).firstEntry().getValue();
            if (!Objects.equals(jop.getOp(), got))
                throw new IllegalArgumentException("Committed op push was not the oldest");

            _persistentData.get().mutate(new JMutator<>() {
                @Override
                public boolean mutate(JKleppmannTreePersistentData object) {
                    _persistentData.get().getData().getQueues().get(host).pollFirstEntry();
                    return true;
                }

                @Override
                public void revert(JKleppmannTreePersistentData object) {
                    _persistentData.get().getData().getQueues().get(host).put(jop.getOp().timestamp(), jop.getOp());
                }
            });

        }

        @Override
        public void pushBootstrap(UUID host) {
            _tree.recordBoostrapFor(host);
        }

        public Pair<String, String> findParent(Function<JKleppmannTreeNodeWrapper, Boolean> predicate) {
            return _tree.findParent(predicate);
        }

        @Override
        public void acceptExternalOp(UUID from, Op op) {
            if (op instanceof JKleppmannTreePeriodicPushOp pushOp) {
                _tree.updateExternalTimestamp(pushOp.getFrom(), pushOp.getTimestamp());
                return;
            }

            if (!(op instanceof JKleppmannTreeOpWrapper jop))
                throw new IllegalArgumentException("Invalid incoming op type for JKleppmannTree: " + op.getClass() + " " + getId());

            JObject<?> fileRef;
            if (jop.getOp().newMeta() instanceof JKleppmannTreeNodeMetaFile f) {
                var fino = f.getFileIno();
                fileRef = jObjectManager.getOrPut(fino, File.class, Optional.of(jop.getOp().childId()));
            } else {
                fileRef = null;
            }

            if (Log.isTraceEnabled())
                Log.trace("Received op from " + from + ": " + jop.getOp().timestamp().timestamp() + " " + jop.getOp().childId() + "->" + jop.getOp().newParentId() + " as " + jop.getOp().newMeta().getName());

            try {
                _tree.applyExternalOp(from, jop.getOp());
            } catch (Exception e) {
                Log.error("Error applying external op", e);
                throw e;
            } finally {
                // FIXME:
                // Fixup the ref if it didn't really get applied

                if ((fileRef == null) && (jop.getOp().newMeta() instanceof JKleppmannTreeNodeMetaFile))
                    Log.error("Could not create child of pushed op: " + jop.getOp());

                if (jop.getOp().newMeta() instanceof JKleppmannTreeNodeMetaFile f) {
                    if (fileRef != null) {
                        var got = jObjectManager.get(jop.getOp().childId()).orElse(null);

                        VoidFn remove = () -> {
                            fileRef.runWriteLockedVoid(JObjectManager.ResolutionStrategy.LOCAL_ONLY, (m, d, b, v) -> {
                                m.removeRef(jop.getOp().childId());
                            });
                        };

                        if (got == null) {
                            remove.apply();
                        } else {
                            try {
                                got.rLock();
                                try {
                                    got.tryResolve(JObjectManager.ResolutionStrategy.LOCAL_ONLY);
                                    if (got.getData() == null || !got.getData().extractRefs().contains(f.getFileIno()))
                                        remove.apply();
                                } finally {
                                    got.rUnlock();
                                }
                            } catch (DeletedObjectAccessException dex) {
                                remove.apply();
                            }
                        }
                    }
                }
            }
        }

        @Override
        public Op getPeriodicPushOp() {
            return new JKleppmannTreePeriodicPushOp(persistentPeerDataService.getSelfUuid(), _clock.peekTimestamp());
        }

        @Override
        public void addToTx() {
            // FIXME: a hack
            _persistentData.get().rwLockNoCopy();
            _persistentData.get().rwUnlock();
        }

        private class JOpRecorder implements OpRecorder<Long, UUID, JKleppmannTreeNodeMeta, String> {
            @Override
            public void recordOp(OpMove<Long, UUID, JKleppmannTreeNodeMeta, String> op) {
                _persistentData.get().assertRwLock();
                _persistentData.get().tryResolve(JObjectManager.ResolutionStrategy.LOCAL_ONLY);
                _persistentData.get().mutate(new JMutator<>() {
                    @Override
                    public boolean mutate(JKleppmannTreePersistentData object) {
                        object.recordOp(persistentPeerDataService.getHostUuids(), op);
                        return true;
                    }

                    @Override
                    public void revert(JKleppmannTreePersistentData object) {
                        object.removeOp(persistentPeerDataService.getHostUuids(), op);
                    }
                });
                opSender.push(JKleppmannTree.this);
            }

            @Override
            public void recordOpForPeer(UUID peer, OpMove<Long, UUID, JKleppmannTreeNodeMeta, String> op) {
                _persistentData.get().assertRwLock();
                _persistentData.get().tryResolve(JObjectManager.ResolutionStrategy.LOCAL_ONLY);
                _persistentData.get().mutate(new JMutator<>() {
                    @Override
                    public boolean mutate(JKleppmannTreePersistentData object) {
                        object.recordOp(peer, op);
                        return true;
                    }

                    @Override
                    public void revert(JKleppmannTreePersistentData object) {
                        object.removeOp(peer, op);
                    }
                });
                opSender.push(JKleppmannTree.this);
            }
        }

        private class JKleppmannTreeClock implements Clock<Long> {
            @Override
            public Long getTimestamp() {
                _persistentData.get().assertRwLock();
                _persistentData.get().tryResolve(JObjectManager.ResolutionStrategy.LOCAL_ONLY);
                var ret = _persistentData.get().getData().getClock().peekTimestamp() + 1;
                _persistentData.get().mutate(new JMutator<>() {
                    @Override
                    public boolean mutate(JKleppmannTreePersistentData object) {
                        object.getClock().getTimestamp();
                        return true;
                    }

                    @Override
                    public void revert(JKleppmannTreePersistentData object) {
                        object.getClock().ungetTimestamp();
                    }
                });
                return ret;
            }

            @Override
            public Long peekTimestamp() {
                return _persistentData.get().runReadLocked(JObjectManager.ResolutionStrategy.LOCAL_ONLY, (m, d) -> d.getClock().peekTimestamp());
            }

            @Override
            public Long updateTimestamp(Long receivedTimestamp) {
                _persistentData.get().assertRwLock();
                _persistentData.get().tryResolve(JObjectManager.ResolutionStrategy.LOCAL_ONLY);
                _persistentData.get().mutate(new JMutator<>() {
                    Long _old;

                    @Override
                    public boolean mutate(JKleppmannTreePersistentData object) {
                        _old = object.getClock().updateTimestamp(receivedTimestamp);
                        return true;
                    }

                    @Override
                    public void revert(JKleppmannTreePersistentData object) {
                        object.getClock().setTimestamp(_old);
                    }
                });
                return _persistentData.get().getData().getClock().peekTimestamp();
            }
        }

        public class JKleppmannTreeStorageInterface implements StorageInterface<Long, UUID, JKleppmannTreeNodeMeta, String, JKleppmannTreeNodeWrapper> {
            private final LogWrapper _logWrapper = new LogWrapper();
            private final PeerLogWrapper _peerLogWrapper = new PeerLogWrapper();

            public JKleppmannTreeStorageInterface() {
                if (jObjectManager.get(getRootId()).isEmpty()) {
                    putNode(new JKleppmannTreeNode(new TreeNode<>(getRootId(), null, new JKleppmannTreeNodeMetaDirectory(""))));
                    putNode(new JKleppmannTreeNode(new TreeNode<>(getTrashId(), null, null)));
                }
            }

            public JObject<JKleppmannTreeNode> putNode(JKleppmannTreeNode node) {
                return jObjectManager.put(node, Optional.ofNullable(node.getNode().getParent()));
            }

            public JObject<JKleppmannTreeNode> putNodeLocked(JKleppmannTreeNode node) {
                return jObjectManager.putLocked(node, Optional.ofNullable(node.getNode().getParent()));
            }

            @Override
            public String getRootId() {
                return _treeName + "_jt_root";
            }

            @Override
            public String getTrashId() {
                return _treeName + "_jt_trash";
            }

            @Override
            public String getNewNodeId() {
                return persistentPeerDataService.getUniqueId();
            }

            @Override
            public JKleppmannTreeNodeWrapper getById(String id) {
                var got = jObjectManager.get(id);
                if (got.isEmpty()) return null;
                return new JKleppmannTreeNodeWrapper((JObject<JKleppmannTreeNode>) got.get());
            }

            @Override
            public JKleppmannTreeNodeWrapper createNewNode(TreeNode<Long, UUID, JKleppmannTreeNodeMeta, String> node) {
                return new JKleppmannTreeNodeWrapper(putNodeLocked(new JKleppmannTreeNode(node)));
            }

            @Override
            public void removeNode(String id) {}

            @Override
            public LogInterface<Long, UUID, JKleppmannTreeNodeMeta, String> getLog() {
                return _logWrapper;
            }

            @Override
            public PeerTimestampLogInterface<Long, UUID> getPeerTimestampLog() {
                return _peerLogWrapper;
            }

            @Override
            public void rLock() {
                _persistentData.get().rLock();
            }

            @Override
            public void rUnlock() {
                _persistentData.get().rUnlock();
            }

            @Override
            public void rwLock() {
                _persistentData.get().rwLockNoCopy();
            }

            @Override
            public void rwUnlock() {
                _persistentData.get().rwUnlock();
            }

            @Override
            public void assertRwLock() {
                _persistentData.get().assertRwLock();
            }

            private class PeerLogWrapper implements PeerTimestampLogInterface<Long, UUID> {

                @Override
                public Long getForPeer(UUID peerId) {
                    return _persistentData.get().runReadLocked(JObjectManager.ResolutionStrategy.LOCAL_ONLY,
                            (m, d) -> d.getPeerTimestampLog().get(peerId));
                }

                @Override
                public void putForPeer(UUID peerId, Long timestamp) {
                    _persistentData.get().assertRwLock();
                    _persistentData.get().tryResolve(JObjectManager.ResolutionStrategy.LOCAL_ONLY);
                    _persistentData.get().mutate(new JMutator<>() {
                        Long old;

                        @Override
                        public boolean mutate(JKleppmannTreePersistentData object) {
                            old = object.getPeerTimestampLog().put(peerId, timestamp);
                            return !Objects.equals(old, timestamp);
                        }

                        @Override
                        public void revert(JKleppmannTreePersistentData object) {
                            if (old != null)
                                object.getPeerTimestampLog().put(peerId, old);
                            else
                                object.getPeerTimestampLog().remove(peerId, timestamp);
                        }
                    });
                }
            }

            private class LogWrapper implements LogInterface<Long, UUID, JKleppmannTreeNodeMeta, String> {
                @Override
                public Pair<CombinedTimestamp<Long, UUID>, LogRecord<Long, UUID, JKleppmannTreeNodeMeta, String>> peekOldest() {
                    return _persistentData.get().runReadLocked(JObjectManager.ResolutionStrategy.LOCAL_ONLY, (m, d) -> {
                        var ret = d.getLog().firstEntry();
                        if (ret == null) return null;
                        return Pair.of(ret);
                    });
                }

                @Override
                public Pair<CombinedTimestamp<Long, UUID>, LogRecord<Long, UUID, JKleppmannTreeNodeMeta, String>> takeOldest() {
                    _persistentData.get().assertRwLock();
                    _persistentData.get().tryResolve(JObjectManager.ResolutionStrategy.LOCAL_ONLY);

                    var ret = _persistentData.get().getData().getLog().firstEntry();
                    if (ret != null)
                        _persistentData.get().mutate(new JMutator<>() {
                            @Override
                            public boolean mutate(JKleppmannTreePersistentData object) {
                                object.getLog().pollFirstEntry();
                                return true;
                            }

                            @Override
                            public void revert(JKleppmannTreePersistentData object) {
                                object.getLog().put(ret.getKey(), ret.getValue());
                            }
                        });
                    return Pair.of(ret);
                }

                @Override
                public Pair<CombinedTimestamp<Long, UUID>, LogRecord<Long, UUID, JKleppmannTreeNodeMeta, String>> peekNewest() {
                    return _persistentData.get().runReadLocked(JObjectManager.ResolutionStrategy.LOCAL_ONLY, (m, d) -> {
                        var ret = d.getLog().lastEntry();
                        if (ret == null) return null;
                        return Pair.of(ret);
                    });
                }

                @Override
                public List<Pair<CombinedTimestamp<Long, UUID>, LogRecord<Long, UUID, JKleppmannTreeNodeMeta, String>>> newestSlice(CombinedTimestamp<Long, UUID> since, boolean inclusive) {
                    return _persistentData.get().runReadLocked(JObjectManager.ResolutionStrategy.LOCAL_ONLY, (m, d) -> {
                        var tail = d.getLog().tailMap(since, inclusive);
                        return tail.entrySet().stream().map(e -> Pair.of(e.getKey(), e.getValue())).toList();
                    });
                }

                @Override
                public List<Pair<CombinedTimestamp<Long, UUID>, LogRecord<Long, UUID, JKleppmannTreeNodeMeta, String>>> getAll() {
                    return _persistentData.get().runReadLocked(JObjectManager.ResolutionStrategy.LOCAL_ONLY, (m, d) -> {
                        return d.getLog().entrySet().stream().map(e -> Pair.of(e.getKey(), e.getValue())).toList();
                    });
                }

                @Override
                public boolean isEmpty() {
                    return _persistentData.get().runReadLocked(JObjectManager.ResolutionStrategy.LOCAL_ONLY, (m, d) -> {
                        return d.getLog().isEmpty();
                    });
                }

                @Override
                public boolean containsKey(CombinedTimestamp<Long, UUID> timestamp) {
                    return _persistentData.get().runReadLocked(JObjectManager.ResolutionStrategy.LOCAL_ONLY, (m, d) -> {
                        return d.getLog().containsKey(timestamp);
                    });
                }

                @Override
                public long size() {
                    return _persistentData.get().runReadLocked(JObjectManager.ResolutionStrategy.LOCAL_ONLY, (m, d) -> {
                        return (long) d.getLog().size();
                    });
                }

                @Override
                public void put(CombinedTimestamp<Long, UUID> timestamp, LogRecord<Long, UUID, JKleppmannTreeNodeMeta, String> record) {
                    _persistentData.get().assertRwLock();
                    _persistentData.get().tryResolve(JObjectManager.ResolutionStrategy.LOCAL_ONLY);
                    if (_persistentData.get().getData().getLog().containsKey(timestamp))
                        throw new IllegalStateException("Overwriting log entry?");
                    _persistentData.get().mutate(new JMutator<>() {
                        @Override
                        public boolean mutate(JKleppmannTreePersistentData object) {
                            object.getLog().put(timestamp, record);
                            return true;
                        }

                        @Override
                        public void revert(JKleppmannTreePersistentData object) {
                            object.getLog().remove(timestamp, record);
                        }
                    });
                }

                @Override
                public void replace(CombinedTimestamp<Long, UUID> timestamp, LogRecord<Long, UUID, JKleppmannTreeNodeMeta, String> record) {
                    _persistentData.get().assertRwLock();
                    _persistentData.get().tryResolve(JObjectManager.ResolutionStrategy.LOCAL_ONLY);
                    _persistentData.get().mutate(new JMutator<>() {
                        LogRecord<Long, UUID, JKleppmannTreeNodeMeta, String> old;

                        @Override
                        public boolean mutate(JKleppmannTreePersistentData object) {
                            old = object.getLog().put(timestamp, record);
                            return !Objects.equals(old, record);
                        }

                        @Override
                        public void revert(JKleppmannTreePersistentData object) {
                            if (old != null)
                                object.getLog().put(timestamp, old);
                            else
                                object.getLog().remove(timestamp, record);
                        }
                    });
                }
            }
        }
    }
}
