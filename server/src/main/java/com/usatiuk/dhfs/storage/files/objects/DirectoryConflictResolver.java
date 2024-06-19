package com.usatiuk.dhfs.storage.files.objects;

import com.usatiuk.dhfs.objects.repository.distributed.ObjectHeader;
import com.usatiuk.dhfs.storage.DeserializationHelper;
import com.usatiuk.dhfs.storage.objects.jrepository.JObjectManager;
import com.usatiuk.dhfs.storage.objects.repository.distributed.*;
import com.usatiuk.dhfs.storage.objects.repository.persistence.ObjectPersistentStore;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.SerializationUtils;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.LinkedHashMap;
import java.util.UUID;

@ApplicationScoped
public class DirectoryConflictResolver implements ConflictResolver {
    @ConfigProperty(name = "dhfs.objects.distributed.selfname")
    String selfname;

    @Inject
    ObjectPersistentStore objectPersistentStore;

    @Inject
    RemoteObjectServiceClient remoteObjectServiceClient;

    @Inject
    ObjectIndexService objectIndexService;

    @Inject
    InvalidationQueueService invalidationQueueService;

    @Inject
    JObjectManager jObjectManager;

    @Override
    public ConflictResolutionResult resolve(String conflictHost,
                                            ObjectHeader conflictSource,
                                            ObjectMetaData localMeta) {
        var oursData = objectPersistentStore.readObject(localMeta.getName());
        var theirsData = remoteObjectServiceClient.getSpecificObject(conflictHost, conflictSource.getName());

        var oursHeader = localMeta.toRpcHeader();
        var theirsHeader = theirsData.getLeft();

        var ours = (Directory) DeserializationHelper.deserialize(oursData);
        var theirs = (Directory) DeserializationHelper.deserialize(theirsData.getRight());
        if (!ours.getClass().equals(Directory.class) || !theirs.getClass().equals(Directory.class)) {
            Log.error("Object type mismatch!");
            throw new NotImplementedException();
        }

        LinkedHashMap<String, UUID> mergedChildren = new LinkedHashMap<>(((Directory) ours).getChildrenMap());
        for (var entry : ((Directory) theirs).getChildrenMap().entrySet()) {
            if (mergedChildren.containsKey(entry.getKey())) {
                mergedChildren.put(entry.getValue() + ".conflict." + conflictHost, entry.getValue());
            }
        }

        var newMetaData = new ObjectMetaData(oursHeader.getName(), oursHeader.getConflictResolver());

        for (var entry : oursHeader.getChangelog().getEntriesList()) {
            newMetaData.getChangelog().put(entry.getHost(), entry.getVersion());
        }
        for (var entry : theirsHeader.getChangelog().getEntriesList()) {
            newMetaData.getChangelog().merge(entry.getHost(), entry.getVersion(), Long::max);
        }

        newMetaData.getChangelog().merge(selfname, 1L, Long::sum);

        var newHdr = newMetaData.toRpcHeader();

        var newDir = new Directory(((Directory) ours).getUuid(), ((Directory) ours).getMode());
        for (var entry : mergedChildren.entrySet()) newDir.putKid(entry.getKey(), entry.getValue());

        // FIXME:
        newDir.setMtime(System.currentTimeMillis());
        newDir.setCtime(ours.getCtime());

        var newBytes = SerializationUtils.serialize(newDir);

        objectIndexService.getOrCreateMeta(oursHeader.getName(), oursHeader.getConflictResolver()).runWriteLocked(m -> {
            m.getChangelog().clear();
            m.getChangelog().putAll(newMetaData.getChangelog());

            objectPersistentStore.writeObject(m.getName(), newBytes);
            return null;
        });
        invalidationQueueService.pushInvalidationToAll(oursHeader.getName());
        jObjectManager.invalidateJObject(oursHeader.getName());


        return ConflictResolutionResult.RESOLVED;
    }
}
