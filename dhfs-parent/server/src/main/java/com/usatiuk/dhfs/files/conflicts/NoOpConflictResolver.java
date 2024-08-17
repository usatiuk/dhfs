package com.usatiuk.dhfs.files.conflicts;

import com.usatiuk.dhfs.objects.jrepository.JObjectData;
import com.usatiuk.dhfs.objects.jrepository.JObjectManager;
import com.usatiuk.dhfs.objects.repository.ConflictResolver;
import com.usatiuk.dhfs.objects.repository.ObjectHeader;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import jakarta.enterprise.context.ApplicationScoped;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

@ApplicationScoped
public class NoOpConflictResolver implements ConflictResolver {
    @Override
    public void resolve(UUID conflictHost, ObjectHeader theirsHeader, JObjectData theirsData, JObjectManager.JObject<?> ours) {
        ours.runWriteLocked(JObjectManager.ResolutionStrategy.LOCAL_ONLY, (m, d, b, i) -> {
            if (d == null)
                throw new StatusRuntimeException(Status.ABORTED.withDescription("Conflict but we don't have local copy"));

            if (!Objects.equals(theirsData.getClass(), ours.getData().getClass()))
                throw new StatusRuntimeException(Status.ABORTED.withDescription("Type conflict for object " + m.getName()
                        + " ours: " + ours.getData().getClass() + " theirs: " + theirsData.getClass()));

            if (!Objects.equals(theirsData, ours.getData()))
                throw new StatusRuntimeException(Status.ABORTED.withDescription("Conflict for immutable object " + m.getName()));

            Map<UUID, Long> newChangelog = new LinkedHashMap<>(m.getChangelog());

            for (var entry : theirsHeader.getChangelog().getEntriesList())
                newChangelog.merge(UUID.fromString(entry.getHost()), entry.getVersion(), Long::max);

            if (m.getBestVersion() > newChangelog.values().stream().reduce(0L, Long::sum))
                throw new StatusRuntimeException(Status.ABORTED.withDescription("Race when conflict resolving"));

            m.setChangelog(newChangelog);

            return null;
        });
    }
}
