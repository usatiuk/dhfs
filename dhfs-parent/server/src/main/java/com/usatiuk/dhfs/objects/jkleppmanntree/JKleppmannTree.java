package com.usatiuk.dhfs.objects.jkleppmanntree;

import com.usatiuk.dhfs.files.objects.File;
import com.usatiuk.dhfs.objects.jkleppmanntree.structs.JKleppmannTreeNodeMeta;
import com.usatiuk.dhfs.objects.jkleppmanntree.structs.JKleppmannTreeNodeMetaFile;
import com.usatiuk.dhfs.objects.jrepository.JObjectManager;
import com.usatiuk.dhfs.objects.repository.PersistentPeerDataService;
import com.usatiuk.dhfs.objects.repository.opsupport.OpObject;
import com.usatiuk.dhfs.objects.repository.opsupport.OpSender;
import com.usatiuk.kleppmanntree.KleppmannTree;
import com.usatiuk.kleppmanntree.OpMove;
import com.usatiuk.kleppmanntree.OpRecorder;
import io.quarkus.logging.Log;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;

public class JKleppmannTree implements OpObject<JKleppmannTreeOpWrapper> {
    private final KleppmannTree<Long, UUID, JKleppmannTreeNodeMeta, String, JKleppmannTreeNodeWrapper> _tree;

    private final JKleppmannTreePersistentData _persistentData;

    private final JKleppmannTreeStorageInterface _storageInterface;
    private final JKleppmannTreePeerInterface _peerInterface;

    private final JObjectManager _jObjectManager;
    private final PersistentPeerDataService _persistentPeerDataService;
    private final OpSender _opSender;

    private class JOpRecorder implements OpRecorder<Long, UUID, JKleppmannTreeNodeMeta, String> {
        @Override
        public void recordOp(OpMove<Long, UUID, JKleppmannTreeNodeMeta, String> op) {
            _persistentData.recordOp(_persistentPeerDataService.getHostUuids(), op);
            _opSender.push(JKleppmannTree.this);
        }

        @Override
        public void recordOpForPeer(UUID peer, OpMove<Long, UUID, JKleppmannTreeNodeMeta, String> op) {
            _persistentData.recordOp(peer, op);
            _opSender.push(JKleppmannTree.this);
        }
    }

    JKleppmannTree(JKleppmannTreePersistentData persistentData,
                   JKleppmannTreePeerInterface peerInterface,
                   JObjectManager jObjectManager,
                   PersistentPeerDataService persistentPeerDataService,
                   OpSender opSender) {
        _persistentData = persistentData;
        _persistentPeerDataService = persistentPeerDataService;
        _opSender = opSender;
        _jObjectManager = jObjectManager;
        _peerInterface = peerInterface;

        _storageInterface = new JKleppmannTreeStorageInterface(persistentData, jObjectManager, persistentPeerDataService);

        _tree = new KleppmannTree<>(_storageInterface, peerInterface, _persistentData.getClock(), new JOpRecorder());
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
        _tree.move(_storageInterface.getTrashId(), newMeta, node);
    }

    @Override
    public JKleppmannTreeOpWrapper getPendingOpForHost(UUID host) {
        if (_persistentData.getQueues().containsKey(host)) {
            var peeked = _persistentData.getQueues().get(host).firstEntry();
            return peeked != null ? new JKleppmannTreeOpWrapper(_persistentData.getQueues().get(host).firstEntry().getValue()) : null;
        }
        return null;
    }

    @Override
    public String getId() {
        return _persistentData.getName();
    }

    @Override
    public void commitOpForHost(UUID host, JKleppmannTreeOpWrapper op) {
        var got = _persistentData.getQueues().get(host).pollFirstEntry().getValue();
        if (op.getOp() != got) {
            throw new IllegalArgumentException("Committed op push was not the oldest");
        }
    }

    @Override
    public void pushBootstrap(UUID host) {
        _tree.recordBoostrapFor(host);
    }

    public Pair<String, String> findParent(Function<JKleppmannTreeNodeWrapper, Boolean> predicate) {
        return _tree.findParent(predicate);
    }

    @Override
    public void acceptExternalOp(UUID from, JKleppmannTreeOpWrapper op) {
        if (!(op instanceof JKleppmannTreeOpWrapper jop))
            throw new IllegalArgumentException("Invalid incoming op type for JKleppmannTree: " + op.getClass() + " " + getId());

        if (jop.getOp().newMeta() instanceof JKleppmannTreeNodeMetaFile f) {
            var fino = f.getFileIno();
            _jObjectManager.getOrPut(fino, File.class, Optional.of(jop.getOp().childId()));
        }

        if (Log.isTraceEnabled())
            Log.trace("Received op from " + from + ": " + jop.getOp().timestamp().timestamp() + " " + jop.getOp().childId() + "->" + jop.getOp().newParentId() + " as " + jop.getOp().newMeta().getName());

        _tree.applyExternalOp(from, jop.getOp());
    }
}

