package com.usatiuk.dhfs.objects.repository;

import com.usatiuk.autoprotomap.runtime.ProtoSerializer;
import com.usatiuk.dhfs.objects.*;
import com.usatiuk.dhfs.objects.persistence.JObjectKeyP;
import com.usatiuk.dhfs.objects.repository.invalidation.InvalidationQueueService;
import com.usatiuk.dhfs.objects.repository.invalidation.Op;
import com.usatiuk.dhfs.objects.transaction.Transaction;
import com.usatiuk.dhfs.objects.transaction.TransactionManager;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.stream.Collectors;

@ApplicationScoped
public class RemoteObjectServiceClient {
    @Inject
    PersistentPeerDataService persistentPeerDataService;

    @Inject
    RpcClientFactory rpcClientFactory;

    @Inject
    TransactionManager txm;
    @Inject
    Transaction curTx;
    @Inject
    RemoteTransaction remoteTx;

    @Inject
    SyncHandler syncHandler;
    @Inject
    InvalidationQueueService invalidationQueueService;
    @Inject
    ProtoSerializer<OpP, Op> opProtoSerializer;
    @Inject
    ProtoSerializer<GetObjectReply, ReceivedObject> receivedObjectProtoSerializer;

    private final ExecutorService _batchExecutor = Executors.newVirtualThreadPerTaskExecutor();

    public Pair<PeerId, ReceivedObject> getSpecificObject(JObjectKey key, PeerId peerId) {
        return rpcClientFactory.withObjSyncClient(peerId, (peer, client) -> {
            var reply = client.getObject(GetObjectRequest.newBuilder().setName(JObjectKeyP.newBuilder().setName(key.toString()).build()).build());
            var deserialized = receivedObjectProtoSerializer.deserialize(reply);
            return Pair.of(peer, deserialized);
        });
    }

    public void getObject(JObjectKey key, Function<Pair<PeerId, ReceivedObject>, Boolean> onReceive) {
        var objMeta = remoteTx.getMeta(key).orElse(null);

        if (objMeta == null) {
            throw new IllegalArgumentException("Object " + key + " not found");
        }

        var targetVersion = objMeta.versionSum();
        var targets = objMeta.knownRemoteVersions().entrySet().stream()
                .filter(entry -> entry.getValue().equals(targetVersion))
                .map(Map.Entry::getKey).toList();

        if (targets.isEmpty())
            throw new IllegalStateException("No targets for object " + key);

        Log.info("Downloading object " + key + " from " + targets);

        rpcClientFactory.withObjSyncClient(targets, (peer, client) -> {
            var reply = client.getObject(GetObjectRequest.newBuilder().setName(JObjectKeyP.newBuilder().setName(key.toString()).build()).build());

            var deserialized = receivedObjectProtoSerializer.deserialize(reply);

            if (!onReceive.apply(Pair.of(peer, deserialized))) {
                throw new StatusRuntimeException(Status.ABORTED.withDescription("Failed to process object " + key + " from " + peer));
            }

            return null;
//            return jObjectTxManager.executeTx(() -> {
//                return key.runWriteLocked(JObjectManager.ResolutionStrategy.NO_RESOLUTION, (md, d, b, v) -> {
//                    var unexpected = !Objects.equals(
//                            Maps.filterValues(md.getChangelog(), val -> val != 0),
//                            Maps.filterValues(receivedMap, val -> val != 0));
//
//                    if (unexpected) {
//                        try {
//                            syncHandler.handleOneUpdate(UUID.fromString(reply.getSelfUuid()), reply.getObject().getHeader());
//                        } catch (SyncHandler.OutdatedUpdateException ignored) {
//                            Log.info("Outdated update of " + md.getName() + " from " + reply.getSelfUuid());
//                            invalidationQueueService.pushInvalidationToOne(UUID.fromString(reply.getSelfUuid()), md.getName()); // True?
//                            throw new StatusRuntimeException(Status.ABORTED.withDescription("Received outdated object version"));
//                        } catch (Exception e) {
//                            Log.error("Received unexpected object version from " + reply.getSelfUuid()
//                                    + " for " + reply.getObject().getHeader().getName() + " and conflict resolution failed", e);
//                            throw new StatusRuntimeException(Status.ABORTED.withDescription("Received unexpected object version"));
//                        }
//                    }
//
//                    return reply.getObject().getContent();
//                });
//            });
        });
    }

    //    @Nullable
//    public IndexUpdateReply notifyUpdate(JObject<?> obj, UUID host) {
//        var builder = IndexUpdatePush.newBuilder().setSelfUuid(persistentPeerDataService.getSelfUuid().toString());
//
//        var header = obj
//                .runReadLocked(
//                        obj.getMeta().getKnownClass().isAnnotationPresent(PushResolution.class)
//                                ? JObjectManager.ResolutionStrategy.LOCAL_ONLY
//                                : JObjectManager.ResolutionStrategy.NO_RESOLUTION,
//                        (m, d) -> {
//                            if (obj.getMeta().isDeleted()) return null;
//                            if (m.getKnownClass().isAnnotationPresent(PushResolution.class) && d == null)
//                                Log.warn("Object " + m.getName() + " is marked as PushResolution but no resolution found");
//                            if (m.getKnownClass().isAnnotationPresent(PushResolution.class))
//                                return m.toRpcHeader(dataProtoSerializer.serialize(d));
//                            else
//                                return m.toRpcHeader();
//                        });
//        if (header == null) return null;
//        jObjectTxManager.executeTx(obj::markSeen);
//        builder.setHeader(header);
//
//        var send = builder.build();
//
//        return rpcClientFactory.withObjSyncClient(host, client -> client.indexUpdate(send));
//    }
//
    public OpPushReply pushOps(PeerId target, List<Op> ops) {
        for (Op op : ops) {
            txm.run(() -> {
                for (var ref : op.getEscapedRefs()) {
                    curTx.get(RemoteObjectMeta.class, ref).map(m -> m.withSeen(true)).ifPresent(curTx::put);
                }
            });
            var serialized = opProtoSerializer.serialize(op);
            var built = OpPushRequest.newBuilder().addMsg(serialized).build();
            rpcClientFactory.withObjSyncClient(target, (tgt, client) -> client.opPush(built));
        }
        return OpPushReply.getDefaultInstance();
    }

    public Collection<Pair<PeerId, CanDeleteReply>> canDelete(Collection<PeerId> targets, JObjectKey objKey, Collection<JObjectKey> ourReferrers) {
        Log.trace("Asking canDelete for " + objKey + " from " + targets.stream().map(PeerId::toString).collect(Collectors.joining(", ")));
        try {
            return _batchExecutor.invokeAll(targets.stream().<Callable<Pair<PeerId, CanDeleteReply>>>map(h -> () -> {
                var req = CanDeleteRequest.newBuilder().setName(JObjectKeyP.newBuilder().setName(objKey.toString()).build());
                for (var ref : ourReferrers) {
                    req.addOurReferrers(JObjectKeyP.newBuilder().setName(ref.toString()).build());
                }
                return Pair.of(h, rpcClientFactory.withObjSyncClient(h, (p, client) -> client.canDelete(req.build())));
            }).toList()).stream().map(f -> {
                try {
                    return f.get();
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            }).toList();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
