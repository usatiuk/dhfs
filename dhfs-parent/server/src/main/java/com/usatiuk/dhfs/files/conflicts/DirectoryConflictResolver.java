package com.usatiuk.dhfs.files.conflicts;

import com.usatiuk.dhfs.files.objects.Directory;
import com.usatiuk.dhfs.files.objects.FsNode;
import com.usatiuk.dhfs.objects.jrepository.JObject;
import com.usatiuk.dhfs.objects.jrepository.JObjectData;
import com.usatiuk.dhfs.objects.jrepository.JObjectManager;
import com.usatiuk.dhfs.objects.repository.ConflictResolver;
import com.usatiuk.dhfs.objects.repository.ObjectHeader;
import com.usatiuk.dhfs.objects.repository.PersistentPeerDataService;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.commons.lang3.NotImplementedException;

import java.util.*;

@ApplicationScoped
public class DirectoryConflictResolver implements ConflictResolver {
    @Inject
    PersistentPeerDataService persistentPeerDataService;

    @Inject
    JObjectManager jObjectManager;

    @Override
    public void resolve(UUID conflictHost, ObjectHeader theirsHeader, JObjectData theirsData, JObject<?> ours) {
        var theirsDir = (Directory) theirsData;
        if (!theirsDir.getClass().equals(Directory.class)) {
            Log.error("Object type mismatch!");
            throw new NotImplementedException();
        }

        ours.runWriteLocked(JObjectManager.ResolutionStrategy.LOCAL_ONLY, (m, oursDirU, bump, invalidate) -> {
            if (oursDirU == null)
                throw new StatusRuntimeException(Status.ABORTED.withDescription("Conflict but we don't have local copy"));
            if (!(oursDirU instanceof Directory oursDir))
                throw new NotImplementedException("Type conflict for " + ours.getMeta().getName() + ", directory was expected");

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
                otherHostname = persistentPeerDataService.getSelfUuid();
            }

            LinkedHashMap<String, UUID> mergedChildren = new LinkedHashMap<>(first.getChildren());
            Map<UUID, Long> newChangelog = new LinkedHashMap<>(m.getChangelog());

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

            for (var entry : theirsHeader.getChangelog().getEntriesList()) {
                newChangelog.merge(UUID.fromString(entry.getHost()), entry.getVersion(), Long::max);
            }

            boolean wasChanged = oursDir.getChildren().size() != mergedChildren.size()
                    || oursDir.getMtime() != first.getMtime()
                    || oursDir.getCtime() != first.getCtime();

            if (m.getBestVersion() > newChangelog.values().stream().reduce(0L, Long::sum))
                throw new StatusRuntimeException(Status.ABORTED.withDescription("Race when conflict resolving"));

            if (wasChanged) {
                newChangelog.merge(persistentPeerDataService.getSelfUuid(), 1L, Long::sum);

                for (var child : mergedChildren.values()) {
                    if (!(new HashSet<>(oursDir.getChildren().values()).contains(child))) {
                        jObjectManager.getOrPut(child.toString(), FsNode.class, Optional.of(oursDir.getName()));
                    }
                }

                oursDir.setMtime(first.getMtime());
                oursDir.setCtime(first.getCtime());

                oursDir.setChildren(mergedChildren);
            }

            m.setChangelog(newChangelog);
            return null;
        });
    }
}
