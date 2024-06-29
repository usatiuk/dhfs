package com.usatiuk.dhfs.storage.files.conflicts;

import com.usatiuk.dhfs.storage.SerializationHelper;
import com.usatiuk.dhfs.storage.files.objects.ChunkData;
import com.usatiuk.dhfs.storage.files.objects.ChunkInfo;
import com.usatiuk.dhfs.storage.files.objects.Directory;
import com.usatiuk.dhfs.storage.files.objects.File;
import com.usatiuk.dhfs.storage.objects.jrepository.JObject;
import com.usatiuk.dhfs.storage.objects.jrepository.JObjectManager;
import com.usatiuk.dhfs.storage.objects.repository.distributed.ConflictResolver;
import com.usatiuk.dhfs.storage.objects.repository.distributed.ObjectMetadata;
import com.usatiuk.dhfs.storage.objects.repository.distributed.PersistentRemoteHostsService;
import com.usatiuk.dhfs.storage.objects.repository.distributed.RemoteObjectServiceClient;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

@ApplicationScoped
public class FileConflictResolver implements ConflictResolver {
    @Inject
    PersistentRemoteHostsService persistentRemoteHostsService;

    @Inject
    RemoteObjectServiceClient remoteObjectServiceClient;

    @Inject
    JObjectManager jObjectManager;

    @Override
    public ConflictResolutionResult resolve(UUID conflictHost, JObject<?> ours) {
        var theirsData = remoteObjectServiceClient.getSpecificObject(conflictHost, ours.getName());

        var theirsFile = (File) SerializationHelper.deserialize(theirsData.getRight());
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
                ObjectMetadata newMetadata;

                var oursHeader = m.toRpcHeader();
                var theirsHeader = theirsData.getLeft();

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

                newMetadata = new ObjectMetadata(ours.getName());

                for (var entry : oursHeader.getChangelog().getEntriesList()) {
                    newMetadata.getChangelog().put(UUID.fromString(entry.getHost()), entry.getVersion());
                }
                for (var entry : theirsHeader.getChangelog().getEntriesList()) {
                    newMetadata.getChangelog().merge(UUID.fromString(entry.getHost()), entry.getVersion(), Long::max);
                }

                boolean chunksDiff = !Objects.equals(first.getChunks(), second.getChunks());

                var firstChunksCopy = first.getChunks().entrySet().stream().map(e -> Pair.of(e.getKey(), e.getValue())).toList();
                var secondChunksCopy = second.getChunks().entrySet().stream().map(e -> Pair.of(e.getKey(), e.getValue())).toList();

                boolean wasChanged = oursFile.getMtime() != first.getMtime()
                        || oursFile.getCtime() != first.getCtime()
                        || chunksDiff;

                if (wasChanged) {
                    newMetadata.getChangelog().merge(persistentRemoteHostsService.getSelfUuid(), 1L, Long::sum);

                    if (m.getBestVersion() >= newMetadata.getOurVersion())
                        throw new NotImplementedException("Race when conflict resolving");

                    var oldChunks = oursFile.getChunks().values().stream().toList();
                    oursFile.getChunks().clear();

                    // FIXME:
                    for (var cuuid : oldChunks) {
                        var ci = jObjectManager.get(ChunkInfo.getNameFromHash(cuuid));
                        ci.ifPresent(jObject -> jObject.runWriteLocked(JObject.ResolutionStrategy.NO_RESOLUTION, (mc, d, b, v) -> {
                            m.removeRef(oursFile.getName());
                            return null;
                        }));
                    }
                    for (var e : firstChunksCopy) {
                        oursFile.getChunks().put(e.getLeft(), e.getValue());
                        jObjectManager.getOrPut(ChunkData.getNameFromHash(e.getValue()), Optional.of(ChunkInfo.getNameFromHash(e.getValue())));
                        jObjectManager.getOrPut(ChunkInfo.getNameFromHash(e.getValue()), Optional.of(oursFile.getName()));
                        jObjectManager.getOrPut(ChunkData.getNameFromHash(e.getValue()), Optional.of(ChunkInfo.getNameFromHash(e.getValue())));
                    }
                    oursFile.setMtime(first.getMtime());
                    oursFile.setCtime(first.getCtime());

                    var newFile = new File(UUID.randomUUID(), second.getMode(), oursDir.getUuid());
                    newFile.setMtime(second.getMtime());
                    newFile.setCtime(second.getCtime());
                    for (var e : secondChunksCopy) {
                        newFile.getChunks().put(e.getLeft(), e.getValue());
                        jObjectManager.getOrPut(ChunkData.getNameFromHash(e.getValue()), Optional.of(ChunkInfo.getNameFromHash(e.getValue())));
                        jObjectManager.getOrPut(ChunkInfo.getNameFromHash(e.getValue()), Optional.ofNullable(newFile.getName()));
                        jObjectManager.getOrPut(ChunkData.getNameFromHash(e.getValue()), Optional.of(ChunkInfo.getNameFromHash(e.getValue())));
                    }

                    var theName = oursDir.getChildren().entrySet().stream().filter(p -> p.getValue().equals(oursFile.getUuid())).findAny().orElseThrow(
                            () -> new NotImplementedException("Could not find our file in directory " + oursDir.getName())
                    );

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
                } else if (m.getBestVersion() > newMetadata.getOurVersion())
                    throw new NotImplementedException("Race when conflict resolving");

                m.getChangelog().clear();
                m.getChangelog().putAll(newMetadata.getChangelog());
                return null;
            });
            return null;
        });

        return ConflictResolutionResult.RESOLVED;
    }
}
