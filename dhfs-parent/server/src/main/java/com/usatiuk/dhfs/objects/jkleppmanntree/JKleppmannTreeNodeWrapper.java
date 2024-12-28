package com.usatiuk.dhfs.objects.jkleppmanntree;

import com.usatiuk.dhfs.objects.jkleppmanntree.structs.JKleppmannTreeNode;
import com.usatiuk.dhfs.objects.jkleppmanntree.structs.JKleppmannTreeNodeMeta;
import com.usatiuk.kleppmanntree.TreeNode;
import com.usatiuk.kleppmanntree.TreeNodeWrapper;
import com.usatiuk.objects.common.runtime.JObjectKey;

import java.util.UUID;

public class JKleppmannTreeNodeWrapper implements TreeNodeWrapper<Long, UUID, JKleppmannTreeNodeMeta, JObjectKey> {
    private final JKleppmannTreeNode _backing;

    public JKleppmannTreeNodeWrapper(JKleppmannTreeNode backing) {
        assert backing != null;
        assert backing.getNode() != null;
        _backing = backing;
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
    public void freeze() {
    }

    @Override
    public void unfreeze() {
    }

    @Override
    public void notifyRef(JObjectKey id) {
    }

    @Override
    public void notifyRmRef(JObjectKey id) {
    }

    @Override
    public TreeNode<Long, UUID, JKleppmannTreeNodeMeta, JObjectKey> getNode() {
        // TODO:
        return _backing.getNode();
//        _backing.tryResolve(JObjectManager.ResolutionStrategy.LOCAL_ONLY);
//        if (_backing.getData() == null)
//            throw new IllegalStateException("Node " + _backing.getMeta().getName() + " data lost!");
//        return _backing.getData().getNode();
    }
}
