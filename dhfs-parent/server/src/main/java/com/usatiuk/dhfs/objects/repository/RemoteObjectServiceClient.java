package com.usatiuk.dhfs.objects.repository;

import com.google.common.collect.Maps;
import com.usatiuk.autoprotomap.runtime.ProtoSerializer;
import com.usatiuk.dhfs.objects.jrepository.*;
import com.usatiuk.dhfs.objects.persistence.JObjectDataP;
import com.usatiuk.dhfs.objects.repository.invalidation.InvalidationQueueService;
import com.usatiuk.dhfs.objects.repository.opsupport.Op;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.commons.lang3.tuple.Pair;

import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

@ApplicationScoped
public class RemoteObjectServiceClient {
    @Inject
    PersistentPeerDataService persistentPeerDataService;

    @Inject
    RpcClientFactory rpcClientFactory;

    @Inject
    JObjectManager jObjectManager;

    @Inject
    SyncHandler syncHandler;
    @Inject
    InvalidationQueueService invalidationQueueService;
    @Inject
    ProtoSerializer<JObjectDataP, JObjectData> dataProtoSerializer;
    @Inject
    ProtoSerializer<OpPushPayload, Op> opProtoSerializer;
    @Inject
    JObjectTxManager jObjectTxManager;

    public Pair<ObjectHeader, JObjectDataP> getSpecificObject(UUID host, String name) {
        return rpcClientFactory.withObjSyncClient(host, client -> {
            var reply = client.getObject(GetObjectRequest.newBuilder().setSelfUuid(persistentPeerDataService.getSelfUuid().toString()).setName(name).build());
            return Pair.of(reply.getObject().getHeader(), reply.getObject().getContent());
        });
    }

    public JObjectDataP getObject(JObject<?> jObject) {
        jObject.assertRwLock();

        var targets = jObject.runReadLocked(JObjectManager.ResolutionStrategy.NO_RESOLUTION, (md, d) -> {
            var ourVersion = md.getOurVersion();
            if (ourVersion >= 1)
                return md.getRemoteCopies().entrySet().stream()
                        .filter(entry -> entry.getValue().equals(ourVersion))
                        .map(Map.Entry::getKey).toList();
            else
                return persistentPeerDataService.getHostUuids();
        });

        if (targets.isEmpty())
            throw new IllegalStateException("No targets for object " + jObject.getMeta().getName());

        Log.info("Downloading object " + jObject.getMeta().getName() + " from " + targets.stream().map(UUID::toString).collect(Collectors.joining(", ")));

        return rpcClientFactory.withObjSyncClient(targets, client -> {
            var reply = client.getObject(GetObjectRequest.newBuilder().setSelfUuid(persistentPeerDataService.getSelfUuid().toString()).setName(jObject.getMeta().getName()).build());

            var receivedMap = new HashMap<UUID, Long>();
            for (var e : reply.getObject().getHeader().getChangelog().getEntriesList()) {
                receivedMap.put(UUID.fromString(e.getHost()), e.getVersion());
            }

            return jObjectTxManager.executeTx(() -> {
                return jObject.runWriteLocked(JObjectManager.ResolutionStrategy.NO_RESOLUTION, (md, d, b, v) -> {
                    var unexpected = !Objects.equals(
                            Maps.filterValues(md.getChangelog(), val -> val != 0),
                            Maps.filterValues(receivedMap, val -> val != 0));

                    if (unexpected) {
                        try {
                            syncHandler.handleOneUpdate(UUID.fromString(reply.getSelfUuid()), reply.getObject().getHeader());
                        } catch (SyncHandler.OutdatedUpdateException ignored) {
                            Log.info("Outdated update of " + md.getName() + " from " + reply.getSelfUuid());
                            invalidationQueueService.pushInvalidationToOne(UUID.fromString(reply.getSelfUuid()), md.getName()); // True?
                            throw new StatusRuntimeException(Status.ABORTED.withDescription("Received outdated object version"));
                        } catch (Exception e) {
                            Log.error("Received unexpected object version from " + reply.getSelfUuid()
                                    + " for " + reply.getObject().getHeader().getName() + " and conflict resolution failed", e);
                            throw new StatusRuntimeException(Status.ABORTED.withDescription("Received unexpected object version"));
                        }
                    }

                    return reply.getObject().getContent();
                });
            });
        });
    }

    @Nullable
    public IndexUpdateReply notifyUpdate(JObject<?> obj, UUID host) {
        var builder = IndexUpdatePush.newBuilder().setSelfUuid(persistentPeerDataService.getSelfUuid().toString());

        var header = obj
                .runReadLocked(
                        obj.getMeta().getKnownClass().isAnnotationPresent(PushResolution.class)
                                ? JObjectManager.ResolutionStrategy.LOCAL_ONLY
                                : JObjectManager.ResolutionStrategy.NO_RESOLUTION,
                        (m, d) -> {
                            if (obj.getMeta().isDeleted()) return null;
                            if (m.getKnownClass().isAnnotationPresent(PushResolution.class) && d == null)
                                Log.warn("Object " + m.getName() + " is marked as PushResolution but no resolution found");
                            if (m.getKnownClass().isAnnotationPresent(PushResolution.class))
                                return m.toRpcHeader(dataProtoSerializer.serialize(d));
                            else
                                return m.toRpcHeader();
                        });
        if (header == null) return null;
        jObjectTxManager.executeTx(obj::markSeen);
        builder.setHeader(header);

        var send = builder.build();

        return rpcClientFactory.withObjSyncClient(host, client -> client.indexUpdate(send));
    }

    public OpPushReply pushOp(Op op, String queueName, UUID host) {
        for (var ref : op.getEscapedRefs()) {
            jObjectTxManager.executeTx(() -> {
                jObjectManager.get(ref).ifPresent(JObject::markSeen);
            });
        }
        var msg = OpPushMsg.newBuilder()
                .setSelfUuid(persistentPeerDataService.getSelfUuid().toString())
                .setQueueId(queueName)
                .setMsg(opProtoSerializer.serialize(op))
                .build();
        return rpcClientFactory.withObjSyncClient(host, client -> client.opPush(msg));
    }

    public Collection<CanDeleteReply> canDelete(Collection<UUID> targets, String object, Collection<String> ourReferrers) {
        ConcurrentLinkedDeque<CanDeleteReply> results = new ConcurrentLinkedDeque<>();
        Log.trace("Asking canDelete for " + object + " from " + targets.stream().map(UUID::toString).collect(Collectors.joining(", ")));
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            try {
                executor.invokeAll(targets.stream().<Callable<Void>>map(h -> () -> {
                    try {
                        var req = CanDeleteRequest.newBuilder()
                                .setSelfUuid(persistentPeerDataService.getSelfUuid().toString())
                                .setName(object);
                        req.addAllOurReferrers(ourReferrers);
                        var res = rpcClientFactory.withObjSyncClient(h, client -> client.canDelete(req.build()));
                        if (res != null)
                            results.add(res);
                    } catch (Exception e) {
                        Log.debug("Error when asking canDelete for object " + object, e);
                    }
                    return null;
                }).toList());
            } catch (InterruptedException e) {
                Log.warn("Interrupted waiting for canDelete for object " + object);
            }
            if (!executor.shutdownNow().isEmpty())
                Log.warn("Didn't ask all targets when asking canDelete for " + object);
        }
        return results;
    }
}
