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
import java.util.Objects;
import java.util.UUID;

//@ApplicationScoped
//public class DirectoryConflictResolver implements ConflictResolver {
//    @ConfigProperty(name = "dhfs.objects.distributed.selfname")
//    String selfname;
//
//    @Inject
//    ObjectPersistentStore objectPersistentStore;
//
//    @Inject
//    RemoteObjectServiceClient remoteObjectServiceClient;
//
//    @Inject
//    InvalidationQueueService invalidationQueueService;
//
//    @Inject
//    JObjectManager jObjectManager;
//
//    @Override
//    public ConflictResolutionResult resolve(String conflictHost,
//                                            ObjectHeader conflictSource,
//                                            String localName) {
//
//        var oursData = objectPersistentStore.readObject(localName);
//        var theirsData = remoteObjectServiceClient.getSpecificObject(conflictHost, conflictSource.getName());
//
//        var localMeta = objectIndexService.getMeta(localName).orElseThrow(() ->
//                new NotImplementedException("Race when conflict resolving"));
//
//        var oursHeader = localMeta.runReadLocked(ObjectMetadata::toRpcHeader);
//        var theirsHeader = theirsData.getLeft();
//
//        var ours = (Directory) DeserializationHelper.deserialize(oursData);
//        var theirs = (Directory) DeserializationHelper.deserialize(theirsData.getRight());
//        if (!ours.getClass().equals(Directory.class) || !theirs.getClass().equals(Directory.class)) {
//            Log.error("Object type mismatch!");
//            throw new NotImplementedException();
//        }
//
//        LinkedHashMap<String, UUID> mergedChildren = new LinkedHashMap<>(ours.getChildrenMap());
//        for (var entry : theirs.getChildrenMap().entrySet()) {
//            if (mergedChildren.containsKey(entry.getKey()) &&
//                    !Objects.equals(mergedChildren.get(entry.getKey()), entry.getValue())) {
//                int i = 0;
//                do {
//                    String name = entry.getKey() + ".conflict." + i + "." + conflictHost;
//                    if (mergedChildren.containsKey(name)) {
//                        i++;
//                        continue;
//                    }
//                    mergedChildren.put(name, entry.getValue());
//                    break;
//                } while (true);
//            } else {
//                mergedChildren.put(entry.getKey(), entry.getValue());
//            }
//        }
//
//        var newMetaData = new ObjectMetadata(oursHeader.getName(), oursHeader.getConflictResolver());
//
//        for (var entry : oursHeader.getChangelog().getEntriesList()) {
//            newMetaData.getChangelog().put(entry.getHost(), entry.getVersion());
//        }
//        for (var entry : theirsHeader.getChangelog().getEntriesList()) {
//            newMetaData.getChangelog().merge(entry.getHost(), entry.getVersion(), Long::max);
//        }
//
//        boolean wasChanged = mergedChildren.size() != ours.getChildrenMap().size();
//        if (wasChanged) {
//            newMetaData.getChangelog().merge(selfname, 1L, Long::sum);
//        }
//
//        var newHdr = newMetaData.toRpcHeader();
//
//        var newDir = new Directory(ours.getUuid(), ours.getMode());
//        for (var entry : mergedChildren.entrySet()) newDir.putKid(entry.getKey(), entry.getValue());
//
//        // FIXME:
//        newDir.setMtime(System.currentTimeMillis());
//        newDir.setCtime(ours.getCtime());
//
//        var newBytes = SerializationUtils.serialize(newDir);
//
//        objectIndexService.getMeta(localName).orElseThrow(() ->
//                new NotImplementedException("Race when conflict resolving")).runWriteLocked(m -> {
//
//            if (wasChanged)
//                if (m.getBestVersion() >= newMetaData.getOurVersion())
//                    throw new NotImplementedException("Race when conflict resolving");
//
//            if (m.getBestVersion() > newMetaData.getOurVersion())
//                throw new NotImplementedException("Race when conflict resolving");
//
//            m.getChangelog().clear();
//            m.getChangelog().putAll(newMetaData.getChangelog());
//
//            objectPersistentStore.writeObject(m.getName(), newBytes);
//            return null;
//        });
//        invalidationQueueService.pushInvalidationToAll(oursHeader.getName());
//        jObjectManager.invalidateJObject(oursHeader.getName());
//
//        return ConflictResolutionResult.RESOLVED;
//    }
//}
