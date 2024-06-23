package com.usatiuk.dhfs.storage.files.conflicts;

import com.usatiuk.dhfs.storage.SerializationHelper;
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

        var oursAsFile = (JObject<File>) ours;

        var _oursDir = jObjectManager.get(oursAsFile.runReadLocked((m, d) -> d.getParent().toString()), Directory.class)
                .orElseThrow(() -> new NotImplementedException("Could not find parent directory for file " + oursAsFile.getName()));

        _oursDir.runWriteLockedMeta((a, b, c) -> {
            if (!_oursDir.tryLocalResolve())
                throw new StatusRuntimeException(Status.ABORTED.withDescription("Conflict but we don't have local copy"));

            _oursDir.runWriteLocked((mD, oursDir, bumpDir) -> {
                oursAsFile.runWriteLockedMeta((a2, b2, c2) -> {
                    if (!ours.tryLocalResolve())
                        throw new StatusRuntimeException(Status.ABORTED.withDescription("Conflict but we don't have local copy"));

                    oursAsFile.runWriteLocked((m, oursFile, bumpFile) -> {

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

                        newMetadata = new ObjectMetadata(ours.getName(), oursHeader.getConflictResolver(), m.getType());

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
                            oursFile.getChunks().clear();
                            for (var e : firstChunksCopy) {
                                oursFile.getChunks().put(e.getLeft(), e.getValue());
                            }
                            oursFile.setMtime(first.getMtime());
                            oursFile.setCtime(first.getCtime());

                            var newFile = new File(UUID.randomUUID(), second.getMode(), oursDir.getUuid());
                            newFile.setMtime(second.getMtime());
                            newFile.setCtime(second.getCtime());
                            for (var e : secondChunksCopy) {
                                newFile.getChunks().put(e.getLeft(), e.getValue());
                            }

                            var theName = oursDir.getChildren().entrySet().stream().filter(p -> p.getValue().equals(oursFile.getUuid())).findAny().orElseThrow(
                                    () -> new NotImplementedException("Could not find our file in directory " + oursDir.getName())
                            );

                            jObjectManager.put(newFile);

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

                            newMetadata.getChangelog().merge(persistentRemoteHostsService.getSelfUuid(), 1L, Long::sum);
                            bumpDir.apply();
                        }

                        if (wasChanged)
                            if (m.getBestVersion() >= newMetadata.getOurVersion())
                                throw new NotImplementedException("Race when conflict resolving");

                        if (m.getBestVersion() > newMetadata.getOurVersion())
                            throw new NotImplementedException("Race when conflict resolving");

                        m.getChangelog().clear();
                        m.getChangelog().putAll(newMetadata.getChangelog());
                        return null;
                    });
                    return null;
                });
                return null;
            });
            return null;
        });

        return ConflictResolutionResult.RESOLVED;
    }
}
