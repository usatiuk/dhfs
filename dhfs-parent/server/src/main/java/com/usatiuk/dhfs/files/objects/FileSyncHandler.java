package com.usatiuk.dhfs.files.objects;

import com.usatiuk.dhfs.files.service.DhfsFileService;
import com.usatiuk.dhfs.objects.*;
import com.usatiuk.dhfs.objects.jkleppmanntree.JKleppmannTreeManager;
import com.usatiuk.dhfs.objects.jkleppmanntree.structs.JKleppmannTreeNodeMetaFile;
import com.usatiuk.dhfs.objects.jmap.JMapHelper;
import com.usatiuk.dhfs.objects.repository.ObjSyncHandler;
import com.usatiuk.dhfs.objects.repository.PersistentPeerDataService;
import com.usatiuk.dhfs.objects.repository.SyncHelper;
import com.usatiuk.dhfs.objects.transaction.LockingStrategy;
import com.usatiuk.dhfs.objects.transaction.Transaction;
import com.usatiuk.kleppmanntree.AlreadyExistsException;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.commons.lang3.tuple.Pair;
import org.pcollections.HashPMap;
import org.pcollections.HashTreePMap;
import org.pcollections.PMap;

import javax.annotation.Nullable;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@ApplicationScoped
public class FileSyncHandler implements ObjSyncHandler<File, FileDto> {
    @Inject
    Transaction curTx;
    @Inject
    PersistentPeerDataService persistentPeerDataService;
    @Inject
    JMapHelper jMapHelper;
    @Inject
    RemoteTransaction remoteTx;
    @Inject
    FileHelper fileHelper;

    @Inject
    JKleppmannTreeManager jKleppmannTreeManager;
    @Inject
    DhfsFileService fileService;

    private JKleppmannTreeManager.JKleppmannTree getTreeW() {
        return jKleppmannTreeManager.getTree(new JObjectKey("fs"));
    }

    private JKleppmannTreeManager.JKleppmannTree getTreeR() {
        return jKleppmannTreeManager.getTree(new JObjectKey("fs"), LockingStrategy.OPTIMISTIC);
    }

    private void resolveConflict(PeerId from, JObjectKey key, PMap<PeerId, Long> receivedChangelog,
                                 @Nullable FileDto receivedData) {
        var oursCurMeta = curTx.get(RemoteObjectMeta.class, key).orElse(null);
        var oursCurFile = remoteTx.getDataLocal(File.class, key).orElse(null);
        if (oursCurFile == null)
            throw new StatusRuntimeException(Status.ABORTED.withDescription("Conflict but we don't have local copy"));

        var theirsFile = receivedData.file();

        File first;
        File second;
        List<Pair<Long, JObjectKey>> firstChunks;
        List<Pair<Long, JObjectKey>> secondChunks;
        PeerId otherHostname;

        if (oursCurFile.mTime() >= theirsFile.mTime()) {
            first = oursCurFile;
            firstChunks = fileHelper.getChunks(oursCurFile);
            second = theirsFile;
            secondChunks = receivedData.chunks();
            otherHostname = from;
        } else {
            second = oursCurFile;
            secondChunks = fileHelper.getChunks(oursCurFile);
            first = theirsFile;
            firstChunks = receivedData.chunks();
            otherHostname = persistentPeerDataService.getSelfUuid();
        }

        HashPMap<PeerId, Long> newChangelog = HashTreePMap.from(oursCurMeta.changelog());

        for (var entry : receivedChangelog.entrySet()) {
            newChangelog = newChangelog.plus(entry.getKey(),
                    Long.max(newChangelog.getOrDefault(entry.getKey(), 0L), entry.getValue())
            );
        }

        boolean chunksDiff = !Objects.equals(firstChunks, secondChunks);

        boolean wasChanged = first.mTime() != second.mTime()
                || first.cTime() != second.cTime()
                || first.symlink() != second.symlink()
                || chunksDiff;

        if (wasChanged) {
            oursCurMeta = oursCurMeta.withChangelog(
                    newChangelog.plus(persistentPeerDataService.getSelfUuid(), newChangelog.get(persistentPeerDataService.getSelfUuid()) + 1)
            );

            curTx.put(oursCurMeta);
            remoteTx.putDataRaw(oursCurFile.withCTime(first.cTime()).withMTime(first.mTime()).withMode(first.mode()).withSymlink(first.symlink()));
            fileHelper.replaceChunks(oursCurFile, firstChunks);

            var newFile = new File(
                    JObjectKey.random(), second.mode(), second.cTime(), second.mTime(), second.symlink()
            );
            remoteTx.putData(newFile);
            fileHelper.replaceChunks(newFile, secondChunks);

            var parent = fileService.inoToParent(oursCurFile.key());

            int i = 0;

            do {
                try {
                    getTreeW().move(parent.getRight(),
                            new JKleppmannTreeNodeMetaFile(
                                    parent.getLeft() + ".fconflict." + persistentPeerDataService.getSelfUuid() + "." + otherHostname.toString() + "." + i,
                                    newFile.key()
                            ),
                            getTreeW().getNewNodeId()
                    );
                } catch (AlreadyExistsException aex) {
                    i++;
                    continue;
                }
                break;
            } while (true);
        }

        var curKnownRemoteVersion = oursCurMeta.knownRemoteVersions().get(from);
        var receivedTotalVer = receivedChangelog.values().stream().mapToLong(Long::longValue).sum();

        if (curKnownRemoteVersion == null || curKnownRemoteVersion < receivedTotalVer) {
            oursCurMeta = oursCurMeta.withKnownRemoteVersions(oursCurMeta.knownRemoteVersions().plus(from, receivedTotalVer));
            curTx.put(oursCurMeta);
        }
    }

    @Override
    public void handleRemoteUpdate(PeerId from, JObjectKey key, PMap<PeerId, Long> receivedChangelog,
                                   @Nullable FileDto receivedData) {
        var current = curTx.get(RemoteObjectMeta.class, key).orElse(null);
        if (current == null) {
            current = new RemoteObjectMeta(key, HashTreePMap.empty());
            curTx.put(current);
        }

        var changelogCompare = SyncHelper.compareChangelogs(current.changelog(), receivedChangelog);

        switch (changelogCompare) {
            case EQUAL -> {
                Log.debug("No action on update: " + key + " from " + from);
                if (!current.hasLocalData() && receivedData != null) {
                    current = current.withHaveLocal(true);
                    curTx.put(current);
                    curTx.put(curTx.get(RemoteObjectDataWrapper.class, RemoteObjectMeta.ofDataKey(current.key()))
                            .map(w -> w.withData(receivedData.file())).orElse(new RemoteObjectDataWrapper<>(receivedData.file())));

                    fileHelper.replaceChunks(receivedData.file(), receivedData.chunks());
                }
            }
            case NEWER -> {
                Log.debug("Received newer index update than known: " + key + " from " + from);
                var newChangelog = receivedChangelog.containsKey(persistentPeerDataService.getSelfUuid()) ?
                        receivedChangelog : receivedChangelog.plus(persistentPeerDataService.getSelfUuid(), 0L);
                current = current.withChangelog(newChangelog);

                if (receivedData != null) {
                    current = current.withHaveLocal(true);
                    curTx.put(current);
                    curTx.put(curTx.get(RemoteObjectDataWrapper.class, RemoteObjectMeta.ofDataKey(current.key()))
                            .map(w -> w.withData(receivedData.file())).orElse(new RemoteObjectDataWrapper<>(receivedData.file())));

                    fileHelper.replaceChunks(receivedData.file(), receivedData.chunks());
                } else {
                    current = current.withHaveLocal(false);
                    curTx.put(current);
                }
            }
            case OLDER -> {
                Log.debug("Received older index update than known: " + key + " from " + from);
                return;
            }
            case CONFLICT -> {
                Log.debug("Conflict on update (inconsistent version): " + key + " from " + from);
                resolveConflict(from, key, receivedChangelog, receivedData);
                // TODO:
                return;
            }
        }
        var curKnownRemoteVersion = current.knownRemoteVersions().get(from);
        var receivedTotalVer = receivedChangelog.values().stream().mapToLong(Long::longValue).sum();

        if (curKnownRemoteVersion == null || curKnownRemoteVersion < receivedTotalVer) {
            current = current.withKnownRemoteVersions(current.knownRemoteVersions().plus(from, receivedTotalVer));
            curTx.put(current);
        }

    }
}
