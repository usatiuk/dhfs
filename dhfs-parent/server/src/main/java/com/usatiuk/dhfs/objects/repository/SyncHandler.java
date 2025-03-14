package com.usatiuk.dhfs.objects.repository;

import com.usatiuk.dhfs.objects.*;
import com.usatiuk.dhfs.objects.iterators.IteratorStart;
import com.usatiuk.dhfs.objects.repository.invalidation.InvalidationQueueService;
import com.usatiuk.dhfs.objects.transaction.Transaction;
import com.usatiuk.dhfs.objects.transaction.TransactionManager;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.pcollections.HashTreePMap;
import org.pcollections.PMap;

import javax.annotation.Nullable;

@ApplicationScoped
public class SyncHandler {
    @Inject
    Transaction curTx;
    @Inject
    PersistentPeerDataService persistentPeerDataService;
    @Inject
    TransactionManager txm;
    @Inject
    InvalidationQueueService invalidationQueueService;

    public <T extends JDataRemote> void handleRemoteUpdate(PeerId from, JObjectKey key, PMap<PeerId, Long> receivedChangelog, @Nullable JDataRemote receivedData) {
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

    public void doInitialSync(PeerId peer) {
        txm.run(() -> {
            try (var it = curTx.getIterator(IteratorStart.GE, JObjectKey.first())) {
                while (it.hasNext()) {
                    var key = it.peekNextKey();
                    invalidationQueueService.pushInvalidationToOne(peer, key, true);
                    it.skip();
                }
            }
        });
    }
}