package com.usatiuk.dhfs.objects.jklepmanntree;

import com.usatiuk.dhfs.objects.jklepmanntree.helpers.StorageInterfaceService;
import com.usatiuk.dhfs.objects.jklepmanntree.structs.JTreeNodeMeta;
import com.usatiuk.kleppmanntree.CombinedTimestamp;
import com.usatiuk.kleppmanntree.KleppmannTree;
import com.usatiuk.kleppmanntree.OpMove;

import java.util.List;
import java.util.UUID;

public class JKleppmannTree {
    private final JKleppmannTreePersistentData _persistentData;
    private final JStorageInterface _storageInterface;
    private final KleppmannTree<Long, UUID, String, JTreeNodeMeta, String, JTreeNodeWrapper> _tree;

    JKleppmannTree(JKleppmannTreePersistentData persistentData, StorageInterfaceService storageInterfaceService, JPeerInterface peerInterface) {
        _persistentData = persistentData;
        var si = new JStorageInterface(persistentData, storageInterfaceService);
        _storageInterface = si;
        si.ensureRootCreated();
        _tree = new KleppmannTree<>(si, peerInterface, _persistentData.getClock());
    }

    private CombinedTimestamp<Long, UUID> getTimestamp() {
        return new CombinedTimestamp<>(_persistentData.getClock().getTimestamp(), _persistentData.getSelfUuid());
    }

    public String traverse(List<String> names) {
        return _tree.traverse(names);
    }

    OpMove<Long, UUID, String, JTreeNodeMeta, String> createMove(String newParent, JTreeNodeMeta newMeta, String node) {
        return new OpMove<>(getTimestamp(), newParent, newMeta, node);
    }

    public void move(String newParent, JTreeNodeMeta newMeta, String node) {
        applyOp(createMove(newParent, newMeta, node));
    }

    public String getNewNodeId() {
        return _storageInterface.getNewNodeId();
    }

    public void trash(JTreeNodeMeta newMeta, String node) {
        applyOp(createMove(_storageInterface.getTrashId(), newMeta, node));
    }

    public void applyOp(OpMove<Long, UUID, String, JTreeNodeMeta, String> opMove) {
        _persistentData.recordOp(opMove);
        _tree.applyOp(opMove);
    }
}

