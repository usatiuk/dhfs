package com.usatiuk.dhfs.peersync;

import com.usatiuk.dhfs.remoteobj.*;
import com.usatiuk.objects.JObjectKey;
import com.usatiuk.objects.transaction.Transaction;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.pcollections.HashPMap;
import org.pcollections.HashTreePMap;
import org.pcollections.PMap;

import javax.annotation.Nullable;

@ApplicationScoped
public class PeerInfoSyncHandler implements ObjSyncHandler<PeerInfo, PeerInfo> {
    @Inject
    Transaction curTx;
    @Inject
    PersistentPeerDataService persistentPeerDataService;
    @Inject
    RemoteTransaction remoteTx;

    @Override
    public void handleRemoteUpdate(PeerId from, JObjectKey key,
                                   PMap<PeerId, Long> receivedChangelog,
                                   @Nullable PeerInfo receivedData) {
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
                            .map(w -> w.withData(receivedData)).orElse(new RemoteObjectDataWrapper<>(receivedData)));

                    if (!current.knownType().isAssignableFrom(receivedData.getClass()))
                        throw new IllegalStateException("Object type mismatch: " + current.knownType() + " vs " + receivedData.getClass());

                    if (!current.knownType().equals(receivedData.getClass()))
                        current = current.withKnownType(receivedData.getClass());

                    curTx.put(current);
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
                            .map(w -> w.withData(receivedData)).orElse(new RemoteObjectDataWrapper<>(receivedData)));

                    if (!current.knownType().isAssignableFrom(receivedData.getClass()))
                        throw new IllegalStateException("Object type mismatch: " + current.knownType() + " vs " + receivedData.getClass());

                    if (!current.knownType().equals(receivedData.getClass()))
                        current = current.withKnownType(receivedData.getClass());

                    curTx.put(current);
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
                assert receivedData != null;

                var oursCurData = remoteTx.getDataLocal(PeerInfo.class, key).orElse(null);
                if (oursCurData == null)
                    throw new StatusRuntimeException(Status.ABORTED.withDescription("Conflict but we don't have local copy"));

                if (!receivedData.cert().equals(oursCurData.cert()))
                    throw new StatusRuntimeException(Status.ABORTED.withDescription("PeerInfo certificate conflict for " + key));

                HashPMap<PeerId, Long> newChangelog = HashTreePMap.from(current.changelog());
                HashPMap<PeerId, Long> newKickCounter = HashTreePMap.from(oursCurData.kickCounter());

                for (var entry : receivedChangelog.entrySet()) {
                    newChangelog = newChangelog.plus(entry.getKey(),
                            Long.max(newChangelog.getOrDefault(entry.getKey(), 0L), entry.getValue())
                    );
                }

                for (var entry : receivedData.kickCounter().entrySet()) {
                    newKickCounter = newKickCounter.plus(entry.getKey(),
                            Long.max(newKickCounter.getOrDefault(entry.getKey(), 0L), entry.getValue())
                    );
                }

                var newData = oursCurData.withKickCounter(newKickCounter)
                        .withLastSeenTimestamp(Math.max(oursCurData.lastSeenTimestamp(), receivedData.lastSeenTimestamp()));

                if (!newData.equals(oursCurData))
                    newChangelog = newChangelog.plus(persistentPeerDataService.getSelfUuid(), newChangelog.getOrDefault(persistentPeerDataService.getSelfUuid(), 0L) + 1L);

                remoteTx.putDataRaw(newData);

                current = current.withChangelog(newChangelog);
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
