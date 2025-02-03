package com.usatiuk.dhfs.objects.repository;

import com.usatiuk.autoprotomap.runtime.ProtoSerializer;
import com.usatiuk.dhfs.objects.*;
import com.usatiuk.dhfs.objects.repository.invalidation.InvalidationQueueService;
import com.usatiuk.dhfs.objects.repository.invalidation.Op;
import com.usatiuk.dhfs.objects.transaction.Transaction;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

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
    //    @Inject
//    ProtoSerializer<JObjectDataP, JObjectData> dataProtoSerializer;
    @Inject
    ProtoSerializer<OpPushPayload, Op> opProtoSerializer;

    @Inject
    ProtoSerializer<GetObjectReply, ReceivedObject> receivedObjectProtoSerializer;

//    public Pair<ObjectHeader, JObjectDataP> getSpecificObject(UUID host, String name) {
//        return rpcClientFactory.withObjSyncClient(host, client -> {
//            var reply = client.getObject(GetObjectRequest.newBuilder().setSelfUuid(persistentPeerDataService.getSelfUuid().toString()).setName(name).build());
//            return Pair.of(reply.getObject().getHeader(), reply.getObject().getContent());
//        });
//    }

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
            var reply = client.getObject(GetObjectRequest.newBuilder().setName(key.toString()).build());

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
//        for (Op op : ops) {
//            for (var ref : op.getEscapedRefs()) {
//                jObjectTxManager.executeTx(() -> {
//                    jObjectManager.get(ref).ifPresent(JObject::markSeen);
//                });
//            }
//        }
//        var builder = OpPushMsg.newBuilder()
//                .setSelfUuid(persistentPeerDataService.getSelfUuid().toString())
//                .setQueueId(queueName);
//        for (var op : ops)
//            builder.addMsg(opProtoSerializer.serialize(op));
        for (Op op : ops) {
            var serialized = opProtoSerializer.serialize(op);
            var built = OpPushRequest.newBuilder().addMsg(serialized).build();
            rpcClientFactory.withObjSyncClient(target, (tgt, client) -> client.opPush(built));
        }
        return OpPushReply.getDefaultInstance();
    }

//    public Collection<CanDeleteReply> canDelete(Collection<UUID> targets, String object, Collection<String> ourReferrers) {
//        ConcurrentLinkedDeque<CanDeleteReply> results = new ConcurrentLinkedDeque<>();
//        Log.trace("Asking canDelete for " + object + " from " + targets.stream().map(UUID::toString).collect(Collectors.joining(", ")));
//        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
//            try {
//                executor.invokeAll(targets.stream().<Callable<Void>>map(h -> () -> {
//                    try {
//                        var req = CanDeleteRequest.newBuilder()
//                                .setSelfUuid(persistentPeerDataService.getSelfUuid().toString())
//                                .setName(object);
//                        req.addAllOurReferrers(ourReferrers);
//                        var res = rpcClientFactory.withObjSyncClient(h, client -> client.canDelete(req.build()));
//                        if (res != null)
//                            results.add(res);
//                    } catch (Exception e) {
//                        Log.debug("Error when asking canDelete for object " + object, e);
//                    }
//                    return null;
//                }).toList());
//            } catch (InterruptedException e) {
//                Log.warn("Interrupted waiting for canDelete for object " + object);
//            }
//            if (!executor.shutdownNow().isEmpty())
//                Log.warn("Didn't ask all targets when asking canDelete for " + object);
//        }
//        return results;
//    }
}
