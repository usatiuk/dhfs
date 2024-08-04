package com.usatiuk.dhfs.objects.jkleppmanntree;

import com.usatiuk.dhfs.objects.jkleppmanntree.structs.JTreeNodeMeta;
import com.usatiuk.dhfs.objects.jkleppmanntree.structs.TreeNodeJObjectData;
import com.usatiuk.dhfs.objects.jrepository.JObject;
import com.usatiuk.kleppmanntree.TreeNode;
import com.usatiuk.kleppmanntree.TreeNodeWrapper;

public class JTreeNodeWrapper implements TreeNodeWrapper<JTreeNodeMeta, String> {
    private final JObject<TreeNodeJObjectData> _backing;

    public JTreeNodeWrapper(JObject<TreeNodeJObjectData> backing) {_backing = backing;}

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
        _backing.runWriteLocked(JObject.ResolutionStrategy.NO_RESOLUTION, (m, d, b, v) -> {
            m.lock();
            return null;
        });
    }

    @Override
    public void unlock() {
        _backing.runWriteLocked(JObject.ResolutionStrategy.NO_RESOLUTION, (m, d, b, v) -> {
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
    public TreeNode<JTreeNodeMeta, String> getNode() {
        _backing.tryResolve(JObject.ResolutionStrategy.LOCAL_ONLY);
        if (_backing.getData() == null) throw new IllegalStateException("Node " + _backing.getName() + " data lost!");
        return _backing.getData().getNode();
    }
}
