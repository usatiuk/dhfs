package com.usatiuk.dhfs.storage.files.conflicts;

import com.usatiuk.dhfs.objects.repository.distributed.ObjectHeader;
import com.usatiuk.dhfs.storage.files.objects.ChunkData;
import com.usatiuk.dhfs.storage.files.objects.ChunkInfo;
import com.usatiuk.dhfs.storage.files.objects.Directory;
import com.usatiuk.dhfs.storage.files.objects.File;
import com.usatiuk.dhfs.storage.objects.jrepository.JObject;
import com.usatiuk.dhfs.storage.objects.jrepository.JObjectData;
import com.usatiuk.dhfs.storage.objects.jrepository.JObjectManager;
import com.usatiuk.dhfs.storage.objects.repository.distributed.ConflictResolver;
import com.usatiuk.dhfs.storage.objects.repository.distributed.PersistentRemoteHostsService;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.tuple.Pair;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.*;

@ApplicationScoped
public class FileConflictResolver implements ConflictResolver {
    @Inject
    PersistentRemoteHostsService persistentRemoteHostsService;

    @Inject
    JObjectManager jObjectManager;

    @ConfigProperty(name = "dhfs.files.use_hash_for_chunks")
    boolean useHashForChunks;

    @Override
    public ConflictResolutionResult resolve(UUID conflictHost, ObjectHeader theirsHeader, JObjectData theirsData, JObject<?> ours) {

        var theirsFile = (File) theirsData;
        if (!theirsFile.getClass().equals(File.class)) {
            Log.error("Object type mismatch!");
            throw new NotImplementedException();
        }


        var _oursDir = jObjectManager.get(ours.runReadLocked(JObject.ResolutionStrategy.LOCAL_ONLY, (m, d) -> {
                    if (d == null)
                        throw new StatusRuntimeException(Status.ABORTED.withDescription("Conflict but we don't have local copy"));
                    if (!(d instanceof File df))
                        throw new StatusRuntimeException(Status.ABORTED.withDescription("Bad type for file"));
                    return df.getParent().toString();
                }))
                .orElseThrow(() -> new NotImplementedException("Could not find parent directory for file " + ours.getName()));

        _oursDir.runWriteLocked(JObject.ResolutionStrategy.LOCAL_ONLY, (mD, oursDirU, bumpDir, invalidateDir) -> {
            if (oursDirU == null)
                throw new StatusRuntimeException(Status.ABORTED.withDescription("Conflict but we don't have local copy"));
            if (!(oursDirU instanceof Directory oursDir))
                throw new StatusRuntimeException(Status.ABORTED.withDescription("Bad type for directory"));

            ours.runWriteLocked(JObject.ResolutionStrategy.LOCAL_ONLY, (m, oursFileU, bumpFile, invalidateFile) -> {
                if (oursFileU == null)
                    throw new StatusRuntimeException(Status.ABORTED.withDescription("Conflict but we don't have local copy"));
                if (!(oursFileU instanceof File oursFile))
                    throw new StatusRuntimeException(Status.ABORTED.withDescription("Bad type for file"));

                // TODO: dedup

                File first;
                File second;
                UUID otherHostname;

                if (oursFile.getMtime() >= theirsFile.getMtime()) {
                    first = oursFile;
                    second = theirsFile;
                    otherHostname = conflictHost;
                } else {
                    second = oursFile;
                    first = theirsFile;
                    otherHostname = persistentRemoteHostsService.getSelfUuid();
                }

                Map<UUID, Long> newChangelog = new LinkedHashMap<>(m.getChangelog());

                for (var entry : theirsHeader.getChangelog().getEntriesList()) {
                    newChangelog.merge(UUID.fromString(entry.getHost()), entry.getVersion(), Long::max);
                }

                boolean chunksDiff = !Objects.equals(first.getChunks(), second.getChunks());

                var firstChunksCopy = first.getChunks().entrySet().stream().map(e -> Pair.of(e.getKey(), e.getValue())).toList();
                var secondChunksCopy = second.getChunks().entrySet().stream().map(e -> Pair.of(e.getKey(), e.getValue())).toList();

                boolean wasChanged = oursFile.getMtime() != first.getMtime()
                        || oursFile.getCtime() != first.getCtime()
                        || chunksDiff;

                if (m.getBestVersion() > newChangelog.values().stream().reduce(0L, Long::sum))
                    throw new StatusRuntimeException(Status.ABORTED.withDescription("Race when conflict resolving"));

                if (wasChanged) {
                    newChangelog.merge(persistentRemoteHostsService.getSelfUuid(), 1L, Long::sum);

                    if (useHashForChunks)
                        throw new NotImplementedException();

                    // FIXME:
                    for (var cuuid : oursFile.getChunks().values()) {
                        jObjectManager
                                .get(ChunkInfo.getNameFromHash(cuuid))
                                .ifPresent(jObject -> jObject.runWriteLocked(JObject.ResolutionStrategy.NO_RESOLUTION, (mc, d, b, v) -> {
                                    m.removeRef(oursFile.getName());
                                    return null;
                                }));
                    }

                    oursFile.getChunks().clear();

                    for (var e : firstChunksCopy) {
                        oursFile.getChunks().put(e.getLeft(), e.getValue());
                        jObjectManager.getOrPut(ChunkData.getNameFromHash(e.getValue()), ChunkData.class, Optional.of(ChunkInfo.getNameFromHash(e.getValue())));
                        jObjectManager.getOrPut(ChunkInfo.getNameFromHash(e.getValue()), ChunkInfo.class, Optional.of(oursFile.getName()));
                    }

                    oursFile.setMtime(first.getMtime());
                    oursFile.setCtime(first.getCtime());

                    var newFile = new File(UUID.randomUUID(), second.getMode(), oursDir.getUuid());

                    newFile.setMtime(second.getMtime());
                    newFile.setCtime(second.getCtime());

                    for (var e : secondChunksCopy) {
                        newFile.getChunks().put(e.getLeft(), e.getValue());
                        jObjectManager.getOrPut(ChunkData.getNameFromHash(e.getValue()), ChunkData.class, Optional.of(ChunkInfo.getNameFromHash(e.getValue())));
                        jObjectManager.getOrPut(ChunkInfo.getNameFromHash(e.getValue()), ChunkInfo.class, Optional.ofNullable(newFile.getName()));
                    }

                    var theName = oursDir.getChildren().entrySet().stream()
                            .filter(p -> p.getValue().equals(oursFile.getUuid())).findAny()
                            .orElseThrow(() -> new NotImplementedException("Could not find our file in directory " + oursDir.getName()));

                    jObjectManager.put(newFile, Optional.of(_oursDir.getName()));

                    int i = 0;
                    do {
                        String name = theName.getKey() + ".conflict." + i + "." + otherHostname;
                        if (oursDir.getChildren().containsKey(name)) {
                            i++;
                            continue;
                        }
                        oursDir.getChildren().put(name, newFile.getUuid());
                        break;
                    } while (true);

                    bumpDir.apply();
                }

                m.setChangelog(newChangelog);
                return null;
            });
            return null;
        });

        return ConflictResolutionResult.RESOLVED;
    }
}
