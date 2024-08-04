package com.usatiuk.dhfs.objects.jkleppmanntree;

import com.usatiuk.dhfs.objects.jkleppmanntree.helpers.StorageInterfaceService;
import com.usatiuk.dhfs.objects.jkleppmanntree.structs.JTreeNodeMeta;
import com.usatiuk.dhfs.objects.jkleppmanntree.structs.JTreeNodeMetaDirectory;
import com.usatiuk.dhfs.objects.jkleppmanntree.structs.TreeNodeJObjectData;
import com.usatiuk.dhfs.objects.jrepository.JObject;
import com.usatiuk.kleppmanntree.CombinedTimestamp;
import com.usatiuk.kleppmanntree.LogOpMove;
import com.usatiuk.kleppmanntree.StorageInterface;
import com.usatiuk.kleppmanntree.TreeNode;
import org.apache.commons.lang3.NotImplementedException;

import java.util.Collection;
import java.util.NavigableMap;
import java.util.UUID;

public class JStorageInterface implements StorageInterface<Long, UUID,  JTreeNodeMeta, String, JTreeNodeWrapper> {
    private final JKleppmannTreePersistentData _persistentData;

    private final StorageInterfaceService _storageInterfaceService;

    public JStorageInterface(JKleppmannTreePersistentData persistentData, StorageInterfaceService storageInterfaceService) {
        _persistentData = persistentData;
        _storageInterfaceService = storageInterfaceService;
    }

    public void ensureRootCreated() {
        if (_storageInterfaceService.getObject(getRootId()).isEmpty()) {
            _storageInterfaceService.putObject(new TreeNodeJObjectData(new TreeNode<>(getRootId(), null, new JTreeNodeMetaDirectory(""))));
            _storageInterfaceService.putObject(new TreeNodeJObjectData(new TreeNode<>(getTrashId(), null, null)));
        }
    }

    @Override
    public String getRootId() {
        return _persistentData.getId() + "_jt_root";
    }

    @Override
    public String getTrashId() {
        return _persistentData.getId() + "_jt_trash";
    }

    @Override
    public String getNewNodeId() {
        return _storageInterfaceService.getUniqueId();
    }

    @Override
    public JTreeNodeWrapper getById(String id) {
        var got = _storageInterfaceService.getObject(id);
        if (got.isEmpty()) return null;
        return new JTreeNodeWrapper((JObject<TreeNodeJObjectData>) got.get());
    }

    @Override
    public JTreeNodeWrapper createNewNode(TreeNode< JTreeNodeMeta, String> node) {
        return new JTreeNodeWrapper(_storageInterfaceService.putObjectLocked(new TreeNodeJObjectData(node)));
    }

    @Override
    public void removeNode(String id) {
        // TODO:
    }

    @Override
    public void lockSet(Collection<JTreeNodeWrapper> nodes) {
        throw new NotImplementedException();
    }

    @Override
    public NavigableMap<CombinedTimestamp<Long, UUID>, LogOpMove<Long, UUID,  ? extends JTreeNodeMeta, String>> getLog() {
        return _persistentData.getLog();
    }
}
