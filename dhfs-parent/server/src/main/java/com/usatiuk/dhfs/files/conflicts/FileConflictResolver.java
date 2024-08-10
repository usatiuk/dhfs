package com.usatiuk.dhfs.files.conflicts;

import com.usatiuk.dhfs.files.objects.ChunkData;
import com.usatiuk.dhfs.files.objects.ChunkInfo;
import com.usatiuk.dhfs.files.objects.File;
import com.usatiuk.dhfs.files.service.DhfsFileService;
import com.usatiuk.dhfs.objects.jkleppmanntree.JKleppmannTreeManager;
import com.usatiuk.dhfs.objects.jkleppmanntree.structs.JKleppmannTreeNodeMetaFile;
import com.usatiuk.dhfs.objects.jrepository.JObject;
import com.usatiuk.dhfs.objects.jrepository.JObjectData;
import com.usatiuk.dhfs.objects.jrepository.JObjectManager;
import com.usatiuk.dhfs.objects.repository.ConflictResolver;
import com.usatiuk.dhfs.objects.repository.ObjectHeader;
import com.usatiuk.dhfs.objects.repository.PersistentPeerDataService;
import com.usatiuk.kleppmanntree.AlreadyExistsException;
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
    PersistentPeerDataService persistentPeerDataService;
    @Inject
    DhfsFileService fileService;
    @Inject
    JKleppmannTreeManager jKleppmannTreeManager;
    @Inject
    JObjectManager jObjectManager;

    @ConfigProperty(name = "dhfs.files.use_hash_for_chunks")
    boolean useHashForChunks;

    // FIXME: There might be a race where node with conflict deletes a file, and we answer that
    // it can do it as we haven't recorded the received file in the object model yet
    @Override
    public void resolve(UUID conflictHost, ObjectHeader theirsHeader, JObjectData theirsData, JObject<?> ours) {
        var theirsFile = (File) theirsData;
        if (!theirsFile.getClass().equals(File.class)) {
            Log.error("Object type mismatch!");
            throw new NotImplementedException();
        }

        var newJFile = ours.runWriteLocked(JObject.ResolutionStrategy.LOCAL_ONLY, (m, oursFileU, bumpFile, invalidateFile) -> {
            if (oursFileU == null)
                throw new StatusRuntimeException(Status.ABORTED.withDescription("Conflict but we don't have local copy"));
            if (!(oursFileU instanceof File oursFile))
                throw new StatusRuntimeException(Status.ABORTED.withDescription("Bad type for file"));

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
                otherHostname = persistentPeerDataService.getSelfUuid();
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
                    || first.isSymlink() != second.isSymlink()
                    || chunksDiff;

            if (m.getBestVersion() > newChangelog.values().stream().reduce(0L, Long::sum))
                throw new StatusRuntimeException(Status.ABORTED.withDescription("Race when conflict resolving"));

            if (wasChanged) {
                newChangelog.merge(persistentPeerDataService.getSelfUuid(), 1L, Long::sum);

                if (useHashForChunks)
                    throw new NotImplementedException();

                HashSet<String> oursBackup = new HashSet<>(oursFile.getChunks().values());
                oursFile.getChunks().clear();

                for (var e : firstChunksCopy) {
                    oursFile.getChunks().put(e.getLeft(), e.getValue());
                    jObjectManager.getOrPut(ChunkData.getNameFromHash(e.getValue()), ChunkData.class, Optional.of(ChunkInfo.getNameFromHash(e.getValue())));
                    jObjectManager.getOrPut(ChunkInfo.getNameFromHash(e.getValue()), ChunkInfo.class, Optional.of(oursFile.getName()));
                }
                HashSet<String> oursNew = new HashSet<>(oursFile.getChunks().values());

                oursFile.setMtime(first.getMtime());
                oursFile.setCtime(first.getCtime());

                var newFile = new File(UUID.randomUUID(), second.getMode(), second.isSymlink());

                newFile.setMtime(second.getMtime());
                newFile.setCtime(second.getCtime());

                for (var e : secondChunksCopy) {
                    newFile.getChunks().put(e.getLeft(), e.getValue());
                    jObjectManager.getOrPut(ChunkData.getNameFromHash(e.getValue()), ChunkData.class, Optional.of(ChunkInfo.getNameFromHash(e.getValue())));
                    jObjectManager.getOrPut(ChunkInfo.getNameFromHash(e.getValue()), ChunkInfo.class, Optional.ofNullable(newFile.getName()));
                }

                var ret = jObjectManager.putLocked(newFile, Optional.empty());

                try {
                    for (var cuuid : oursBackup) {
                        if (!oursNew.contains(cuuid))
                            jObjectManager
                                    .get(ChunkInfo.getNameFromHash(cuuid))
                                    .ifPresent(jObject -> jObject.runWriteLocked(JObject.ResolutionStrategy.NO_RESOLUTION, (mc, d, b, v) -> {
                                        mc.removeRef(oursFile.getName());
                                        return null;
                                    }));
                    }
                } catch (Exception e) {
                    ret.getMeta().unlock();
                    ret.rwUnlock();
                    return null;
                }
                return ret;
            }

            m.setChangelog(newChangelog);
            return null;
        });

        if (newJFile == null) return;

        // FIXME: Slow and what happens if a directory is deleted?
        try {
            var parent = fileService.inoToParent(ours.getName());
            // FIXME?
            var tree = jKleppmannTreeManager.getTree("fs");

            var nodeId = tree.getNewNodeId();
            newJFile.getMeta().addRef(nodeId);
            newJFile.getMeta().unlock();
            newJFile.rwUnlock();

            int i = 0;

            do {
                try {
                    tree.move(parent.getRight(), new JKleppmannTreeNodeMetaFile(parent.getLeft() + ".fconflict." + persistentPeerDataService.getSelfUuid() + "." + conflictHost + "." + i, newJFile.getName()), nodeId);
                } catch (AlreadyExistsException aex) {
                    i++;
                    continue;
                }
                break;
            } while (true);
        } catch (Exception e) {
            Log.error("Error when creating new file for " + ours.getName(), e);
        } finally {
            if (newJFile.haveRwLock()) {
                newJFile.getMeta().unlock();
                newJFile.getMeta().getReferrersMutable().clear();
                newJFile.rwUnlock();
            }
        }
    }
}
