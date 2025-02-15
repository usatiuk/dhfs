package com.usatiuk.dhfs.objects.repository;

import com.usatiuk.dhfs.objects.*;
import com.usatiuk.dhfs.objects.transaction.Transaction;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.pcollections.HashTreePMap;
import org.pcollections.PMap;

import javax.annotation.Nullable;

//

//import com.usatiuk.autoprotomap.runtime.ProtoSerializer;

//import com.usatiuk.dhfs.objects.jrepository.JObject;

//import com.usatiuk.dhfs.objects.jrepository.JObjectData;

//import com.usatiuk.dhfs.objects.jrepository.JObjectManager;

//import com.usatiuk.dhfs.objects.jrepository.JObjectTxManager;

//import com.usatiuk.dhfs.objects.persistence.JObjectDataP;

//import com.usatiuk.dhfs.objects.repository.invalidation.InvalidationQueueService;

//import com.usatiuk.dhfs.objects.repository.opsupport.OpObjectRegistry;
//import com.usatiuk.dhfs.utils.StatusRuntimeExceptionNoStacktrace;
//import io.grpc.Status;
//import io.quarkus.logging.Log;
//import jakarta.enterprise.context.ApplicationScoped;
//import jakarta.enterprise.inject.Instance;
//import jakarta.inject.Inject;
//
//import java.util.HashMap;
//import java.util.Objects;
//import java.util.Optional;
//import java.util.UUID;
//import java.util.concurrent.atomic.AtomicReference;
//import java.util.stream.Collectors;
//import java.util.stream.Stream;
//
@ApplicationScoped
public class SyncHandler {
    @Inject
    Transaction curTx;
    @Inject
    PersistentPeerDataService persistentPeerDataService;
//    @Inject
//    JObjectManager jObjectManager;
//    @Inject
//    PeerManager peerManager;
//    @Inject
//    RemoteObjectServiceClient remoteObjectServiceClient;
//    @Inject
//    InvalidationQueueService invalidationQueueService;
//    @Inject
//    Instance<ConflictResolver> conflictResolvers;
//    @Inject
//    PersistentPeerDataService persistentPeerDataService;
//    @Inject
//    ProtoSerializer<JObjectDataP, JObjectData> dataProtoSerializer;
//    @Inject
//    OpObjectRegistry opObjectRegistry;
//    @Inject
//    JObjectTxManager jObjectTxManager;
//
//    public void pushInitialResyncObj(UUID host) {
//        Log.info("Doing initial object push for " + host);
//
//        var objs = jObjectManager.findAll();
//
//        for (var obj : objs) {
//            Log.trace("IS: " + obj + " to " + host);
//            invalidationQueueService.pushInvalidationToOne(host, obj);
//        }
//    }
//
//    public void pushInitialResyncOp(UUID host) {
//        Log.info("Doing initial op push for " + host);
//
//        jObjectTxManager.executeTxAndFlush(
//                () -> {
//                    opObjectRegistry.pushBootstrapData(host);
//                }
//        );
//    }
//

//    public <T extends JDataRemote> RemoteObjectMeta<T> handleOneUpdate(PeerId from, RemoteObjectMeta<T> current, PMap<PeerId, Long> rcvChangelog) {
////        if (!rcv.key().equals(current.key())) {
////            Log.error("Received update for different object: " + rcv.key() + " from " + from);
////            throw new IllegalArgumentException("Received update for different object: " + rcv.key() + " from " + from);
////        }
//
//        var receivedTotalVer = rcvChangelog.values().stream().mapToLong(Long::longValue).sum();
//
//        if (current.meta().knownRemoteVersions().getOrDefault(from, 0L) > receivedTotalVer) {
//            Log.error("Received older index update than was known for host: " + from + " " + current.key());
//            throw new IllegalStateException(); // FIXME: OutdatedUpdateException
//        }
//
//        Log.trace("Handling update: " + current.key() + " from " + from + "\n" + "ours: " + current + " \n" + "received: " + rcvChangelog);
//
//        boolean conflict = false;
//        boolean updatedRemoteVersion = false;
//
//        var newObj = current;
//        var curKnownRemoteVersion = current.meta().knownRemoteVersions().get(from);
//
//        if (curKnownRemoteVersion == null || !curKnownRemoteVersion.equals(receivedTotalVer))
//            updatedRemoteVersion = true;
//
//        if (updatedRemoteVersion)
//            newObj = current.withMeta(current.meta().withKnownRemoteVersions(
//                    current.meta().knownRemoteVersions().plus(from, receivedTotalVer)
//            ));
//
//
//        boolean hasLower = false;
//        boolean hasHigher = false;
//        for (var e : Stream.concat(current.meta().changelog().keySet().stream(), rcvChangelog.keySet().stream()).collect(Collectors.toUnmodifiableSet())) {
//            if (rcvChangelog.getOrDefault(e, 0L) < current.meta().changelog().getOrDefault(e, 0L))
//                hasLower = true;
//            if (rcvChangelog.getOrDefault(e, 0L) > current.meta().changelog().getOrDefault(e, 0L))
//                hasHigher = true;
//        }
//
//        if (hasLower && hasHigher) {
//            Log.info("Conflict on update (inconsistent version): " + current.key() + " from " + from);
////            Log.
////
////                    info("Trying conflict resolution: " + header.getName() + " from " + from);
////            var found = foundExt.get();
////
////            JObjectData theirsData;
////            ObjectHeader theirsHeader;
////            if (header.        hasPushedData()) {
////                theirsHeader = header;
////                theirsData = dataProtoSerializer.
////
////                        deserialize(header.getPushedData());
////            } else {
////                var got = remoteObjectServiceClient.getSpecificObject(from, header.getName());
////                theirsData = dataProtoSerializer.
////
////                        deserialize(got.getRight());
////                theirsHeader = got.
////
////                        getLeft();
////            }
////
////            jObjectTxManager.
////
////                    executeTx(() -> {
////                        var resolverClass = found.runReadLocked(JObjectManager.ResolutionStrategy.LOCAL_ONLY, (m, d) -> {
////                            if (d == null)
////                                throw new StatusRuntimeExceptionNoStacktrace(Status.UNAVAILABLE.withDescription("No local data when conflict " + header.getName()));
////                            return d.getConflictResolver();
////                        });
////                        var resolver = conflictResolvers.select(resolverClass);
////                        resolver.
////
////                                get().
////
////                                resolve(from, theirsHeader, theirsData, found);
////                    });
////            Log.  info("Resolved conflict for " + from + " " + header.getName());
////            throw new NotImplementedException();
//        } else if (hasLower) {
//            Log.info("Received older index update than known: " + from + " " + current.key());
////            throw new OutdatedUpdateException();
////            throw new NotImplementedException();
//        } else if (hasHigher) {
//            var newChangelog = rcvChangelog.containsKey(persistentPeerDataService.getSelfUuid()) ?
//                    rcvChangelog : rcvChangelog.plus(persistentPeerDataService.getSelfUuid(), 0L);
//
//            newObj = newObj.withData(null).withMeta(newObj.meta().withChangelog(newChangelog));
////            if (header.hasPushedData())
////                found.externalResolution(dataProtoSerializer.deserialize(header.getPushedData()));
//        }
////        else if (data == null && header.hasPushedData()) {
////            found.tryResolve(JObjectManager.ResolutionStrategy.LOCAL_ONLY);
////            if (found.getData() == null)
////                found.externalResolution(dataProtoSerializer.deserialize(header.getPushedData()));
////        }
//

    /// /        assert Objects.equals(receivedTotalVer, md.getOurVersion());
//
//        if (!updatedRemoteVersion)
//            Log.debug("No action on update: " + current.meta().key() + " from " + from);
//
//        return newObj;
//    }
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
}