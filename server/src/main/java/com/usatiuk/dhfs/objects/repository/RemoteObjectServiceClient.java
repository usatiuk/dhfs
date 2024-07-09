package com.usatiuk.dhfs.objects.repository;

import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;
import com.usatiuk.dhfs.objects.jrepository.DeletedObjectAccessException;
import com.usatiuk.dhfs.objects.jrepository.JObject;
import com.usatiuk.dhfs.objects.jrepository.JObjectManager;
import com.usatiuk.dhfs.objects.jrepository.PushResolution;
import com.usatiuk.dhfs.objects.repository.peersync.PersistentPeerInfo;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.commons.lang3.tuple.Pair;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Executors;

@ApplicationScoped
public class RemoteObjectServiceClient {
    @Inject
    PersistentRemoteHostsService persistentRemoteHostsService;

    @Inject
    RpcClientFactory rpcClientFactory;

    @Inject
    JObjectManager jObjectManager;

    @Inject
    SyncHandler syncHandler;
    @Inject
    InvalidationQueueService invalidationQueueService;

    public Pair<ObjectHeader, ByteString> getSpecificObject(UUID host, String name) {
        return rpcClientFactory.withObjSyncClient(host, client -> {
            var reply = client.getObject(GetObjectRequest.newBuilder().setSelfUuid(persistentRemoteHostsService.getSelfUuid().toString()).setName(name).build());
            return Pair.of(reply.getObject().getHeader(), reply.getObject().getContent());
        });
    }

    public ByteString getObject(JObject<?> jObject) {
        jObject.assertRWLock();

        var targets = jObject.runReadLocked(JObject.ResolutionStrategy.NO_RESOLUTION, (md, d) -> {
            var ourVersion = md.getOurVersion();
            if (ourVersion > 1)
                return md.getRemoteCopies().entrySet().stream()
                        .filter(entry -> entry.getValue().equals(ourVersion))
                        .map(Map.Entry::getKey).toList();
            else
                return persistentRemoteHostsService.getHosts().stream().map(PersistentPeerInfo::getUuid).toList();
        });

        if (targets.isEmpty())
            throw new IllegalStateException("No targets for object " + jObject.getName());

        return rpcClientFactory.withObjSyncClient(targets, client -> {
            var reply = client.getObject(GetObjectRequest.newBuilder().setSelfUuid(persistentRemoteHostsService.getSelfUuid().toString()).setName(jObject.getName()).build());

            var receivedMap = new HashMap<UUID, Long>();
            for (var e : reply.getObject().getHeader().getChangelog().getEntriesList()) {
                receivedMap.put(UUID.fromString(e.getHost()), e.getVersion());
            }

            return jObject.runWriteLocked(JObject.ResolutionStrategy.NO_RESOLUTION, (md, d, b, v) -> {
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
    }

    public IndexUpdatePush getIndex(UUID host) {
        return rpcClientFactory.withObjSyncClient(host, client -> {
            var req = GetIndexRequest.newBuilder().setSelfUuid(persistentRemoteHostsService.getSelfUuid().toString()).build();
            return client.getIndex(req);
        });
    }

    public List<IndexUpdateError> notifyUpdate(UUID host, List<String> names) {
        var builder = IndexUpdatePush.newBuilder().setSelfUuid(persistentRemoteHostsService.getSelfUuid().toString());
        for (var v : names) {
            var obj = jObjectManager.get(v);
            if (obj.isEmpty()) continue;

            try {
                var header = obj.get()
                        .runReadLocked(
                                obj.get().getKnownClass().isAnnotationPresent(PushResolution.class)
                                        ? JObject.ResolutionStrategy.LOCAL_ONLY
                                        : JObject.ResolutionStrategy.NO_RESOLUTION,
                                (m, d) -> {
                                    if (m.getKnownClass().isAnnotationPresent(PushResolution.class) && d == null)
                                        Log.warn("Object " + m.getName() + " is marked as PushResolution but no resolution found");
                                    return Pair.of(m.toRpcHeader(d), m.isSeen());
                                });
                if (!header.getRight())
                    obj.get().runWriteLocked(JObject.ResolutionStrategy.NO_RESOLUTION, (m, d, b, i) -> {
                        m.markSeen();
                        return null;
                    });
                builder.addHeader(header.getLeft());
            } catch (DeletedObjectAccessException e) {
                continue;
            }
        }

        var send = builder.build();

        return rpcClientFactory.withObjSyncClient(host, client -> client.indexUpdate(send).getErrorsList());
    }

    public Collection<CanDeleteReply> canDelete(Collection<UUID> targets, String object, Collection<String> ourReferrers) {
        ConcurrentLinkedDeque<CanDeleteReply> results = new ConcurrentLinkedDeque<>();
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            try {
                executor.invokeAll(targets.stream().<Callable<Void>>map(h -> () -> {
                    try {
                        var req = CanDeleteRequest.newBuilder()
                                .setSelfUuid(persistentRemoteHostsService.getSelfUuid().toString())
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
