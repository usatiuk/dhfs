package com.usatiuk.dhfs.objects.jkleppmanntree;

import com.usatiuk.dhfs.objects.jkleppmanntree.helpers.StorageInterfaceService;
import com.usatiuk.dhfs.objects.jkleppmanntree.structs.JTreeNodeMeta;
import com.usatiuk.kleppmanntree.KleppmannTree;
import com.usatiuk.kleppmanntree.OpMove;

import java.util.List;
import java.util.UUID;

public class JKleppmannTree {
    private final JKleppmannTreePersistentData _persistentData;
    private final JStorageInterface _storageInterface;
    private final KleppmannTree<Long, UUID, JTreeNodeMeta, String, JTreeNodeWrapper> _tree;

    JKleppmannTree(JKleppmannTreePersistentData persistentData, StorageInterfaceService storageInterfaceService, JPeerInterface peerInterface) {
        _persistentData = persistentData;
        var si = new JStorageInterface(persistentData, storageInterfaceService);
        _storageInterface = si;
        si.ensureRootCreated();
        _tree = new KleppmannTree<>(si, peerInterface, _persistentData.getClock());
    }

    public String traverse(List<String> names) {
        return _tree.traverse(names);
    }

    public void move(String newParent, JTreeNodeMeta newMeta, String node) {
        applyOp(_tree.createMove(newParent, newMeta, node));
    }

    public String getNewNodeId() {
        return _storageInterface.getNewNodeId();
    }

    public void trash(JTreeNodeMeta newMeta, String node) {
        applyOp(_tree.createMove(_storageInterface.getTrashId(), newMeta.withName(node), node));
    }

    public void applyOp(OpMove<Long, UUID,  JTreeNodeMeta, String> opMove) {
        _persistentData.recordOp(opMove);
        _tree.applyOp(opMove);
    }
}

