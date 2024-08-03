package com.usatiuk.dhfs.objects.jklepmanntree;

import com.usatiuk.dhfs.objects.jklepmanntree.structs.JTreeNodeMeta;
import com.usatiuk.dhfs.objects.jklepmanntree.structs.TreeNodeJObjectData;
import com.usatiuk.dhfs.objects.jrepository.JObject;
import com.usatiuk.kleppmanntree.TreeNode;
import com.usatiuk.kleppmanntree.TreeNodeWrapper;

public class JTreeNodeWrapper implements TreeNodeWrapper<String, JTreeNodeMeta, String> {
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
    public void notifyRef(String id) {
        _backing.getMeta().addRef(id);
    }

    @Override
    public void notifyRmRef(String id) {
        _backing.getMeta().removeRef(id);
    }

    @Override
    public TreeNode<String, JTreeNodeMeta, String> getNode() {
        return _backing.getData().getNode();
    }
}
