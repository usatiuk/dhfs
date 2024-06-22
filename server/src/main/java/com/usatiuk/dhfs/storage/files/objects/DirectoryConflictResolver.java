package com.usatiuk.dhfs.storage.files.objects;

import com.usatiuk.dhfs.storage.SerializationHelper;
import com.usatiuk.dhfs.storage.objects.jrepository.JObject;
import com.usatiuk.dhfs.storage.objects.repository.distributed.ConflictResolver;
import com.usatiuk.dhfs.storage.objects.repository.distributed.ObjectMetadata;
import com.usatiuk.dhfs.storage.objects.repository.distributed.PersistentRemoteHostsService;
import com.usatiuk.dhfs.storage.objects.repository.distributed.RemoteObjectServiceClient;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.commons.lang3.NotImplementedException;

import java.util.LinkedHashMap;
import java.util.Objects;
import java.util.UUID;

@ApplicationScoped
public class DirectoryConflictResolver implements ConflictResolver {
    @Inject
    PersistentRemoteHostsService persistentRemoteHostsService;

    @Inject
    RemoteObjectServiceClient remoteObjectServiceClient;

    @Override
    public ConflictResolutionResult resolve(UUID conflictHost, JObject<?> ours) {
        var theirsData = remoteObjectServiceClient.getSpecificObject(conflictHost, ours.getName());

        if (!ours.isOf(Directory.class))
            throw new NotImplementedException("Type conflict for " + ours.getName() + ", directory was expected");

        var oursAsDir = (JObject<Directory>) ours;

        oursAsDir.runWriteLocked((m, oursDir, bump) -> {
            if (!ours.tryLocalResolve())
                throw new NotImplementedException("Conflict but we don't have local copy for " + ours.getName());

            LinkedHashMap<String, UUID> mergedChildren = new LinkedHashMap<>();
            ObjectMetadata newMetadata;
            long newMtime;
            long newCtime;

            var oursHeader = m.toRpcHeader();
            var theirsHeader = theirsData.getLeft();

            var theirsDir = (Directory) SerializationHelper.deserialize(theirsData.getRight());
            if (!theirsDir.getClass().equals(Directory.class)) {
                Log.error("Object type mismatch!");
                throw new NotImplementedException();
            }

            Directory first;
            Directory second;
            UUID otherHostname;
            if (oursDir.getMtime() >= theirsDir.getMtime()) {
                first = oursDir;
                second = theirsDir;
                otherHostname = conflictHost;
            } else {
                second = oursDir;
                first = theirsDir;
                otherHostname = persistentRemoteHostsService.getSelfUuid();
            }

            mergedChildren.putAll(first.getChildren());
            for (var entry : second.getChildren().entrySet()) {
                if (mergedChildren.containsKey(entry.getKey()) &&
                        !Objects.equals(mergedChildren.get(entry.getKey()), entry.getValue())) {
                    int i = 0;
                    do {
                        String name = entry.getKey() + ".conflict." + i + "." + otherHostname;
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

            newMetadata = new ObjectMetadata(ours.getName(), oursHeader.getConflictResolver(), m.getType());

            for (var entry : oursHeader.getChangelog().getEntriesList()) {
                newMetadata.getChangelog().put(UUID.fromString(entry.getHost()), entry.getVersion());
            }
            for (var entry : theirsHeader.getChangelog().getEntriesList()) {
                newMetadata.getChangelog().merge(UUID.fromString(entry.getHost()), entry.getVersion(), Long::max);
            }

            boolean wasChanged = mergedChildren.size() != first.getChildren().size();
            if (wasChanged) {
                newMetadata.getChangelog().merge(persistentRemoteHostsService.getSelfUuid(), 1L, Long::sum);
            }

            newMtime = first.getMtime();
            newCtime = first.getCtime();

            if (wasChanged)
                if (m.getBestVersion() >= newMetadata.getOurVersion())
                    throw new NotImplementedException("Race when conflict resolving");

            if (m.getBestVersion() > newMetadata.getOurVersion())
                throw new NotImplementedException("Race when conflict resolving");

            oursDir.setMtime(newMtime);
            oursDir.setCtime(newCtime);
            oursDir.getChildren().clear();
            oursDir.getChildren().putAll(mergedChildren);

            m.getChangelog().clear();
            m.getChangelog().putAll(newMetadata.getChangelog());
            return null;
        });

        return ConflictResolutionResult.RESOLVED;
    }
}
