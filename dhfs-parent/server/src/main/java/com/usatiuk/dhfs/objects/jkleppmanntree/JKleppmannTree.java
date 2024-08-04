package com.usatiuk.dhfs.objects.jkleppmanntree;

import com.usatiuk.dhfs.objects.jkleppmanntree.helpers.StorageInterfaceService;
import com.usatiuk.dhfs.objects.jkleppmanntree.structs.JTreeNodeMeta;
import com.usatiuk.kleppmanntree.KleppmannTree;
import com.usatiuk.kleppmanntree.OpMove;
import com.usatiuk.kleppmanntree.OpRecorder;

import java.util.List;
import java.util.UUID;

public class JKleppmannTree {
    private final JKleppmannTreePersistentData _persistentData;
    private final JStorageInterface _storageInterface;
    private final KleppmannTree<Long, UUID, JTreeNodeMeta, String, JTreeNodeWrapper> _tree;
    private final JPeerInterface _peerInterface;

    private class JOpRecorder implements OpRecorder<Long, UUID, JTreeNodeMeta, String> {
        @Override
        public void recordOp(OpMove<Long, UUID, ? extends JTreeNodeMeta, String> op) {
            _persistentData.recordOp(op);
        }
    }

    JKleppmannTree(JKleppmannTreePersistentData persistentData, StorageInterfaceService storageInterfaceService, JPeerInterface peerInterface) {
        _persistentData = persistentData;
        var si = new JStorageInterface(persistentData, storageInterfaceService);
        _storageInterface = si;
        si.ensureRootCreated();
        _peerInterface = peerInterface;
        _tree = new KleppmannTree<>(si, peerInterface, _persistentData.getClock(), new JOpRecorder());
    }

    public String traverse(List<String> names) {
        return _tree.traverse(names);
    }

    public String getNewNodeId() {
        return _storageInterface.getNewNodeId();
    }

    public void move(String newParent, JTreeNodeMeta newMeta, String node) {
        _tree.applyOp(_peerInterface.getSelfId(), _tree.createMove(newParent, newMeta, node));
    }

    public void trash(JTreeNodeMeta newMeta, String node) {
        _tree.applyOp(_peerInterface.getSelfId(), _tree.createMove(_storageInterface.getTrashId(), newMeta.withName(node), node));
    }

    void applyExternalOp(UUID from, OpMove<Long, UUID, ? extends JTreeNodeMeta, String> op) {
        _tree.applyOp(from, op);
    }
}

