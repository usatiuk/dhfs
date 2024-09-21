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
                jObjectManager.put(new JKleppmannTreeOpLog(name, new TreeMap<>()), Optional.of(JKleppmannTreePersistentData.nameFromTreeName(name)));
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
        public Op getPendingOpForHost(UUID host) {
            return _persistentData.get().runReadLocked(JObjectManager.ResolutionStrategy.LOCAL_ONLY, (m, d) -> {
                if (d.getQueues().containsKey(host)) {
                    var peeked = d.getQueues().get(host).firstEntry();
                    return peeked != null ? new JKleppmannTreeOpWrapper(d.getQueues().get(host).firstEntry().getValue()) : null;
                }
                return null;
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
            _persistentData.get().runWriteLockedVoid(JObjectManager.ResolutionStrategy.LOCAL_ONLY, (m, d, b, v) -> {
                var got = d.getQueues().get(host).pollFirstEntry().getValue();
                if (jop.getOp() != got) {
                    throw new IllegalArgumentException("Committed op push was not the oldest");
                }
                _persistentData.get().bumpVer();
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

            if (jop.getOp().newMeta() instanceof JKleppmannTreeNodeMetaFile f) {
                var fino = f.getFileIno();
                jObjectManager.getOrPut(fino, File.class, Optional.of(jop.getOp().childId()));
            }

            if (Log.isTraceEnabled())
                Log.trace("Received op from " + from + ": " + jop.getOp().timestamp().timestamp() + " " + jop.getOp().childId() + "->" + jop.getOp().newParentId() + " as " + jop.getOp().newMeta().getName());

            try {
                _tree.applyExternalOp(from, jop.getOp());
            } catch (Exception e) {
                Log.error("Error applying external op", e);
                throw e;
            }
        }

        @Override
        public Op getPeriodicPushOp() {
            return new JKleppmannTreePeriodicPushOp(persistentPeerDataService.getSelfUuid(), _clock.peekTimestamp());
        }

        private class JOpRecorder implements OpRecorder<Long, UUID, JKleppmannTreeNodeMeta, String> {
            @Override
            public void recordOp(OpMove<Long, UUID, JKleppmannTreeNodeMeta, String> op) {
                _persistentData.get().runWriteLockedVoid(JObjectManager.ResolutionStrategy.LOCAL_ONLY, (m, d, b, v) -> {
                    d.recordOp(persistentPeerDataService.getHostUuids(), op);
                    opSender.push(JKleppmannTree.this);
                    _persistentData.get().bumpVer();
                });
            }

            @Override
            public void recordOpForPeer(UUID peer, OpMove<Long, UUID, JKleppmannTreeNodeMeta, String> op) {
                _persistentData.get().runWriteLockedVoid(JObjectManager.ResolutionStrategy.LOCAL_ONLY, (m, d, b, v) -> {
                    d.recordOp(peer, op);
                    opSender.push(JKleppmannTree.this);
                    _persistentData.get().bumpVer();
                });
            }
        }

        private class JKleppmannTreeClock implements Clock<Long> {
            @Override
            public Long getTimestamp() {
                return _persistentData.get().runWriteLocked(JObjectManager.ResolutionStrategy.LOCAL_ONLY, (m, d, b, v) -> {
                    _persistentData.get().bumpVer();
                    return d.getClock().getTimestamp();
                });
            }

            @Override
            public Long peekTimestamp() {
                return _persistentData.get().runReadLocked(JObjectManager.ResolutionStrategy.LOCAL_ONLY, (m, d) -> d.getClock().peekTimestamp());
            }

            @Override
            public void updateTimestamp(Long receivedTimestamp) {
                _persistentData.get().runWriteLockedVoid(JObjectManager.ResolutionStrategy.LOCAL_ONLY, (m, d, b, v) -> {
                    d.getClock().updateTimestamp(receivedTimestamp);
                    _persistentData.get().bumpVer();
                });
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

            private class PeerLogWrapper implements PeerTimestampLogInterface<Long, UUID> {

                @Override
                public Long getForPeer(UUID peerId) {
                    return _persistentData.get().runReadLocked(JObjectManager.ResolutionStrategy.LOCAL_ONLY,
                            (m, d) -> d.getPeerTimestampLog().get(peerId));
                }

                @Override
                public void putForPeer(UUID peerId, Long timestamp) {
                    _persistentData.get().runWriteLockedVoid(JObjectManager.ResolutionStrategy.LOCAL_ONLY,
                            (m, d, b, v) -> {
                                d.getPeerTimestampLog().put(peerId, timestamp);
                                _persistentData.get().bumpVer();
                            });
                }
            }

            private class LogWrapper implements LogInterface<Long, UUID, JKleppmannTreeNodeMeta, String> {
                private final SoftJObject<JKleppmannTreeOpLog> _backing;

                private LogWrapper() {
                    _backing = softJObjectFactory.create(JKleppmannTreeOpLog.fromTreeName(_treeName));
                }

                @Override
                public Pair<CombinedTimestamp<Long, UUID>, LogRecord<Long, UUID, JKleppmannTreeNodeMeta, String>> peekOldest() {
                    return _backing.get().runReadLocked(JObjectManager.ResolutionStrategy.LOCAL_ONLY, (m, d) -> {
                        var ret = d.getLog().firstEntry();
                        if (ret == null) return null;
                        return Pair.of(ret);
                    });
                }

                @Override
                public Pair<CombinedTimestamp<Long, UUID>, LogRecord<Long, UUID, JKleppmannTreeNodeMeta, String>> takeOldest() {
                    return _backing.get().runWriteLocked(JObjectManager.ResolutionStrategy.LOCAL_ONLY, (m, d, b, v) -> {
                        var ret = d.getLog().pollFirstEntry();
                        if (ret == null) return null;
                        _backing.get().bumpVer();
                        return Pair.of(ret);
                    });
                }

                @Override
                public Pair<CombinedTimestamp<Long, UUID>, LogRecord<Long, UUID, JKleppmannTreeNodeMeta, String>> peekNewest() {
                    return _backing.get().runReadLocked(JObjectManager.ResolutionStrategy.LOCAL_ONLY, (m, d) -> {
                        var ret = d.getLog().lastEntry();
                        if (ret == null) return null;
                        return Pair.of(ret);
                    });
                }

                @Override
                public List<Pair<CombinedTimestamp<Long, UUID>, LogRecord<Long, UUID, JKleppmannTreeNodeMeta, String>>> newestSlice(CombinedTimestamp<Long, UUID> since, boolean inclusive) {
                    return _backing.get().runReadLocked(JObjectManager.ResolutionStrategy.LOCAL_ONLY, (m, d) -> {
                        var tail = d.getLog().tailMap(since, inclusive);
                        return tail.entrySet().stream().map(e -> Pair.of(e.getKey(), e.getValue())).toList();
                    });
                }

                @Override
                public List<Pair<CombinedTimestamp<Long, UUID>, LogRecord<Long, UUID, JKleppmannTreeNodeMeta, String>>> getAll() {
                    return _backing.get().runReadLocked(JObjectManager.ResolutionStrategy.LOCAL_ONLY, (m, d) -> {
                        return d.getLog().entrySet().stream().map(e -> Pair.of(e.getKey(), e.getValue())).toList();
                    });
                }

                @Override
                public boolean isEmpty() {
                    return _backing.get().runReadLocked(JObjectManager.ResolutionStrategy.LOCAL_ONLY, (m, d) -> {
                        return d.getLog().isEmpty();
                    });
                }

                @Override
                public boolean containsKey(CombinedTimestamp<Long, UUID> timestamp) {
                    return _backing.get().runReadLocked(JObjectManager.ResolutionStrategy.LOCAL_ONLY, (m, d) -> {
                        return d.getLog().containsKey(timestamp);
                    });
                }

                @Override
                public long size() {
                    return _backing.get().runReadLocked(JObjectManager.ResolutionStrategy.LOCAL_ONLY, (m, d) -> {
                        return (long) d.getLog().size();
                    });
                }

                @Override
                public void put(CombinedTimestamp<Long, UUID> timestamp, LogRecord<Long, UUID, JKleppmannTreeNodeMeta, String> record) {
                    _backing.get().runWriteLockedVoid(JObjectManager.ResolutionStrategy.LOCAL_ONLY, (m, d, b, v) -> {
                        var old = d.getLog().put(timestamp, record);
                        if (!Objects.equals(old, record))
                            _backing.get().bumpVer();
                    });
                }

                @Override
                public void replace(CombinedTimestamp<Long, UUID> timestamp, LogRecord<Long, UUID, JKleppmannTreeNodeMeta, String> record) {
                    _backing.get().runWriteLockedVoid(JObjectManager.ResolutionStrategy.LOCAL_ONLY, (m, d, b, v) -> {
                        var old = d.getLog().put(timestamp, record);
                        if (!Objects.equals(old, record))
                            _backing.get().bumpVer();
                    });
                }
            }
        }
    }
}
