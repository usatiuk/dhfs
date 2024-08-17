package com.usatiuk.dhfs.objects.jkleppmanntree;

import com.usatiuk.dhfs.objects.jkleppmanntree.structs.JKleppmannTreeNode;
import com.usatiuk.dhfs.objects.jkleppmanntree.structs.JKleppmannTreeNodeMeta;
import com.usatiuk.dhfs.objects.jkleppmanntree.structs.JKleppmannTreeNodeMetaDirectory;
import com.usatiuk.dhfs.objects.jrepository.JObjectManager;
import com.usatiuk.dhfs.objects.repository.PersistentPeerDataService;
import com.usatiuk.kleppmanntree.CombinedTimestamp;
import com.usatiuk.kleppmanntree.LogRecord;
import com.usatiuk.kleppmanntree.StorageInterface;
import com.usatiuk.kleppmanntree.TreeNode;
import org.apache.commons.lang3.NotImplementedException;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

public class JKleppmannTreeStorageInterface implements StorageInterface<Long, UUID, JKleppmannTreeNodeMeta, String, JKleppmannTreeNodeWrapper> {
    private final JKleppmannTreePersistentData _persistentData;
    private final JObjectManager jObjectManager;
    private final PersistentPeerDataService persistentPeerDataService;

    public JKleppmannTreeStorageInterface(JKleppmannTreePersistentData persistentData, JObjectManager jObjectManager, PersistentPeerDataService persistentPeerDataService) {
        _persistentData = persistentData;
        this.jObjectManager = jObjectManager;
        this.persistentPeerDataService = persistentPeerDataService;

        if (this.jObjectManager.get(getRootId()).isEmpty()) {
            putNode(new JKleppmannTreeNode(new TreeNode<>(getRootId(), null, new JKleppmannTreeNodeMetaDirectory(""))));
            putNode(new JKleppmannTreeNode(new TreeNode<>(getTrashId(), null, null)));
        }
    }

    public JObjectManager.JObject<JKleppmannTreeNode> putNode(JKleppmannTreeNode node) {
        return jObjectManager.put(node, Optional.ofNullable(node.getNode().getParent()));
    }

    public JObjectManager.JObject<JKleppmannTreeNode> putNodeLocked(JKleppmannTreeNode node) {
        return jObjectManager.putLocked(node, Optional.ofNullable(node.getNode().getParent()));
    }

    @Override
    public String getRootId() {
        return _persistentData.getName() + "_jt_root";
    }

    @Override
    public String getTrashId() {
        return _persistentData.getName() + "_jt_trash";
    }

    @Override
    public String getNewNodeId() {
        return persistentPeerDataService.getUniqueId();
    }

    @Override
    public JKleppmannTreeNodeWrapper getById(String id) {
        var got = jObjectManager.get(id);
        if (got.isEmpty()) return null;
        return new JKleppmannTreeNodeWrapper((JObjectManager.JObject<JKleppmannTreeNode>) got.get());
    }

    @Override
    public JKleppmannTreeNodeWrapper createNewNode(TreeNode<Long, UUID, JKleppmannTreeNodeMeta, String> node) {
        return new JKleppmannTreeNodeWrapper(putNodeLocked(new JKleppmannTreeNode(node)));
    }

    @Override
    public void removeNode(String id) {}

    @Override
    public void lockSet(Collection<JKleppmannTreeNodeWrapper> nodes) {
        throw new NotImplementedException();
    }

    @Override
    public NavigableMap<CombinedTimestamp<Long, UUID>, LogRecord<Long, UUID, JKleppmannTreeNodeMeta, String>> getLog() {
        return _persistentData.getLog();
    }

    @Override
    public Map<UUID, AtomicReference<Long>> getPeerTimestampLog() {
        return _persistentData.getPeerTimestampLog();
    }
}
