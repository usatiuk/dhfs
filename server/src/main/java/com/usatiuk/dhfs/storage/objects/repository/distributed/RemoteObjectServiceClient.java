package com.usatiuk.dhfs.storage.objects.repository.distributed;

import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;
import com.usatiuk.dhfs.objects.repository.distributed.*;
import com.usatiuk.dhfs.storage.objects.jrepository.JObject;
import com.usatiuk.dhfs.storage.objects.jrepository.JObjectManager;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.commons.lang3.tuple.Pair;

import java.util.*;

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

    public Pair<ObjectHeader, ByteString> getSpecificObject(UUID host, String name) {
        return rpcClientFactory.withObjSyncClient(host, client -> {
            var reply = client.getObject(GetObjectRequest.newBuilder().setSelfUuid(persistentRemoteHostsService.getSelfUuid().toString()).setName(name).build());
            return Pair.of(reply.getObject().getHeader(), reply.getObject().getContent());
        });
    }

    public ByteString getObject(JObject<?> jObject) {
        jObject.assertRWLock();

        var targets = jObject.runWriteLockedMeta((md, b, v) -> {
            var ourVersion = md.getOurVersion();
            return md.getRemoteCopies().entrySet().stream()
                    .filter(entry -> entry.getValue().equals(ourVersion))
                    .map(Map.Entry::getKey).toList();
        });

        return rpcClientFactory.withObjSyncClient(targets, client -> {
            var reply = client.getObject(GetObjectRequest.newBuilder().setSelfUuid(persistentRemoteHostsService.getSelfUuid().toString()).setName(jObject.getName()).build());

            var receivedMap = new HashMap<UUID, Long>();
            for (var e : reply.getObject().getHeader().getChangelog().getEntriesList()) {
                receivedMap.put(UUID.fromString(e.getHost()), e.getVersion());
            }

            return jObject.runWriteLockedMeta((md, b, v) -> {
                var unexpected = !Objects.equals(
                        Maps.filterValues(md.getChangelog(), val -> val != 0),
                        Maps.filterValues(receivedMap, val -> val != 0));

                if (unexpected) {
                    try {
                        syncHandler.handleOneUpdate(UUID.fromString(reply.getSelfUuid()), reply.getObject().getHeader());
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
            builder.addHeader(obj.get().runReadLocked(ObjectMetadata::toRpcHeader));
        }

        var send = builder.build();

        return rpcClientFactory.withObjSyncClient(host, client -> client.indexUpdate(send).getErrorsList());
    }
}
