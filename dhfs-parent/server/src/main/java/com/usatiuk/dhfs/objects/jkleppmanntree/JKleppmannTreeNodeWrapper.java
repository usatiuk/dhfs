package com.usatiuk.dhfs.objects.jkleppmanntree;

import com.usatiuk.dhfs.objects.jkleppmanntree.structs.JKleppmannTreeNode;
import com.usatiuk.dhfs.objects.jkleppmanntree.structs.JKleppmannTreeNodeMeta;
import com.usatiuk.dhfs.objects.jrepository.JObjectManager;
import com.usatiuk.kleppmanntree.TreeNode;
import com.usatiuk.kleppmanntree.TreeNodeWrapper;

import java.util.UUID;

public class JKleppmannTreeNodeWrapper implements TreeNodeWrapper<Long, UUID, JKleppmannTreeNodeMeta, String> {
    private final JObjectManager.JObject<JKleppmannTreeNode> _backing;

    public JKleppmannTreeNodeWrapper(JObjectManager.JObject<JKleppmannTreeNode> backing) {_backing = backing;}

    @Override
    public void rLock() {
        _backing.rLock();
    }

    @Override
    public void rUnlock() {
        _backing.rUnlock();
    }

    @Override
    public void rwLock() {
        _backing.rwLock();
    }

    @Override
    public void rwUnlock() {
        _backing.bumpVer(); // FIXME:?
        _backing.rwUnlock();
    }

    @Override
    public void lock() {
        _backing.runWriteLocked(JObjectManager.ResolutionStrategy.NO_RESOLUTION, (m, d, b, v) -> {
            m.lock();
            return null;
        });
    }

    @Override
    public void unlock() {
        _backing.runWriteLocked(JObjectManager.ResolutionStrategy.NO_RESOLUTION, (m, d, b, v) -> {
            m.unlock();
            return null;
        });
    }

    @Override
    public void notifyRef(String id) {
        _backing.getMeta().addRef(id);
    }

    @Override
    public void notifyRmRef(String id) {
        _backing.getMeta().removeRef(id);
    }

    @Override
    public TreeNode<Long, UUID, JKleppmannTreeNodeMeta, String> getNode() {
        _backing.tryResolve(JObjectManager.ResolutionStrategy.LOCAL_ONLY);
        if (_backing.getData() == null)
            throw new IllegalStateException("Node " + _backing.getMeta().getName() + " data lost!");
        return _backing.getData().getNode();
    }
}
