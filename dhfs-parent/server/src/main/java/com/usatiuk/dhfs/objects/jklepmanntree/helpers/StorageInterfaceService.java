package com.usatiuk.dhfs.objects.jklepmanntree.helpers;

import com.usatiuk.dhfs.objects.jklepmanntree.structs.TreeNodeJObjectData;
import com.usatiuk.dhfs.objects.jrepository.JObject;
import com.usatiuk.dhfs.objects.jrepository.JObjectManager;
import com.usatiuk.dhfs.objects.repository.PersistentRemoteHostsService;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.util.Optional;

@Singleton
public class StorageInterfaceService {
    @Inject
    PersistentRemoteHostsService persistentRemoteHostsService;
    @Inject
    JObjectManager jObjectManager;

    public String getUniqueId() {
        return persistentRemoteHostsService.getUniqueId();
    }

    public Optional<JObject<?>> getObject(String id) {
        return jObjectManager.get(id);
    }

    public JObject<TreeNodeJObjectData> putObject(TreeNodeJObjectData node) {
        return jObjectManager.put(node, Optional.ofNullable(node.getNode().getParent()));
    }

    public JObject<TreeNodeJObjectData> putObjectLocked(TreeNodeJObjectData node) {
        return jObjectManager.putLocked(node, Optional.ofNullable(node.getNode().getParent()));
    }

}
