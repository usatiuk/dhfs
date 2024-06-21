package com.usatiuk.dhfs.storage.files.objects;

import com.usatiuk.dhfs.objects.repository.distributed.ObjectHeader;
import com.usatiuk.dhfs.storage.DeserializationHelper;
import com.usatiuk.dhfs.storage.objects.jrepository.JObjectManager;
import com.usatiuk.dhfs.storage.objects.repository.distributed.ConflictResolver;
import com.usatiuk.dhfs.storage.objects.repository.distributed.ObjectMetadata;
import com.usatiuk.dhfs.storage.objects.repository.distributed.RemoteObjectServiceClient;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.commons.lang3.NotImplementedException;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.LinkedHashMap;
import java.util.Objects;
import java.util.UUID;

@ApplicationScoped
public class DirectoryConflictResolver implements ConflictResolver {
    @ConfigProperty(name = "dhfs.objects.distributed.selfname")
    String selfname;

    @Inject
    RemoteObjectServiceClient remoteObjectServiceClient;

    @Inject
    JObjectManager jObjectManager;

    @Override
    public ConflictResolutionResult resolve(String conflictHost,
                                            ObjectHeader conflictSource,
                                            String localName) {

        var oursData = jObjectManager.get(localName, Directory.class).orElseThrow(() -> new NotImplementedException("Oops"));
        var theirsData = remoteObjectServiceClient.getSpecificObject(conflictHost, conflictSource.getName());

        var oursHeader = oursData.runReadLocked(ObjectMetadata::toRpcHeader);
        var theirsHeader = theirsData.getLeft();

        var theirs = (Directory) DeserializationHelper.deserialize(theirsData.getRight());
        if (!theirs.getClass().equals(Directory.class)) {
            Log.error("Object type mismatch!");
            throw new NotImplementedException();
        }

        LinkedHashMap<String, UUID> mergedChildren = new LinkedHashMap<>(oursData.runReadLocked((m, d) -> d.getChildrenMap()));
        for (var entry : theirs.getChildrenMap().entrySet()) {
            if (mergedChildren.containsKey(entry.getKey()) &&
                    !Objects.equals(mergedChildren.get(entry.getKey()), entry.getValue())) {
                int i = 0;
                do {
                    String name = entry.getKey() + ".conflict." + i + "." + conflictHost;
                    if (mergedChildren.containsKey(name)) {
                        i++;
                        continue;
                    }
                    mergedChildren.put(name, entry.getValue());
                    break;
                } while (true);
            } else {
                mergedChildren.put(entry.getKey(), entry.getValue());
            }
        }

        var newMetaData = new ObjectMetadata(oursHeader.getName(), oursHeader.getConflictResolver(), oursData.runReadLocked(m -> m.getType()));

        for (var entry : oursHeader.getChangelog().getEntriesList()) {
            newMetaData.getChangelog().put(entry.getHost(), entry.getVersion());
        }
        for (var entry : theirsHeader.getChangelog().getEntriesList()) {
            newMetaData.getChangelog().merge(entry.getHost(), entry.getVersion(), Long::max);
        }

        boolean wasChanged = mergedChildren.size() != oursData.runReadLocked((m, d) -> d.getChildrenMap().size());
        if (wasChanged) {
            newMetaData.getChangelog().merge(selfname, 1L, Long::sum);
        }

        var newHdr = newMetaData.toRpcHeader();

        oursData.runWriteLocked((m, d, bump) -> {
            if (wasChanged)
                if (m.getBestVersion() >= newMetaData.getOurVersion())
                    throw new NotImplementedException("Race when conflict resolving");

            if (m.getBestVersion() > newMetaData.getOurVersion())
                throw new NotImplementedException("Race when conflict resolving");

            d.setMtime(System.currentTimeMillis());
            d.setCtime(d.getCtime());
            d.getChildren().clear();
            d.getChildren().putAll(mergedChildren);

            m.getChangelog().clear();
            m.getChangelog().putAll(newMetaData.getChangelog());
            return null;
        });

        return ConflictResolutionResult.RESOLVED;
    }
}
