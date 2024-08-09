package com.usatiuk.dhfs.objects.repository.peersync;

import com.usatiuk.dhfs.objects.jrepository.JObject;
import com.usatiuk.dhfs.objects.jrepository.JObjectData;
import com.usatiuk.dhfs.objects.jrepository.JObjectManager;
import com.usatiuk.dhfs.objects.repository.ConflictResolver;
import com.usatiuk.dhfs.objects.repository.ObjectHeader;
import com.usatiuk.dhfs.objects.repository.PersistentPeerDataService;
import com.usatiuk.dhfs.objects.repository.RemoteObjectServiceClient;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.commons.lang3.NotImplementedException;

import java.util.*;

@ApplicationScoped
public class PeerDirectoryConflictResolver implements ConflictResolver {
    @Inject
    PersistentPeerDataService persistentPeerDataService;

    @Inject
    RemoteObjectServiceClient remoteObjectServiceClient;

    @Inject
    JObjectManager jObjectManager;

    @Override
    public void resolve(UUID conflictHost, ObjectHeader theirsHeader, JObjectData theirsData, JObject<?> ours) {
        var theirsDir = (PeerDirectory) theirsData;
        if (!theirsDir.getClass().equals(PeerDirectory.class)) {
            Log.error("Object type mismatch!");
            throw new NotImplementedException();
        }

        ours.runWriteLocked(JObject.ResolutionStrategy.LOCAL_ONLY, (m, oursDirU, bump, invalidate) -> {
            if (oursDirU == null)
                throw new StatusRuntimeException(Status.ABORTED.withDescription("Conflict but we don't have local copy"));
            if (!(oursDirU instanceof PeerDirectory oursPD))
                throw new NotImplementedException("Type conflict for " + ours.getName() + ", directory was expected");

            LinkedHashSet<UUID> mergedChildren = new LinkedHashSet<>(oursPD.getPeers());
            mergedChildren.addAll(theirsDir.getPeers());
            Map<UUID, Long> newChangelog = new LinkedHashMap<>(m.getChangelog());

            for (var entry : theirsHeader.getChangelog().getEntriesList()) {
                newChangelog.merge(UUID.fromString(entry.getHost()), entry.getVersion(), Long::max);
            }

            boolean wasChanged = oursPD.getPeers().size() != mergedChildren.size();

            if (m.getBestVersion() > newChangelog.values().stream().reduce(0L, Long::sum))
                throw new StatusRuntimeException(Status.ABORTED.withDescription("Race when conflict resolving"));

            if (wasChanged) {
                newChangelog.merge(persistentPeerDataService.getSelfUuid(), 1L, Long::sum);

                for (var child : mergedChildren) {
                    if (!oursPD.getPeers().contains(child)) {
                        jObjectManager.getOrPut(PersistentPeerInfo.getNameFromUuid(child), PersistentPeerInfo.class, Optional.of(oursPD.getName()));
                    }
                }

                oursPD.getPeers().addAll(mergedChildren);
            }

            m.setChangelog(newChangelog);
            return null;
        });
    }
}
