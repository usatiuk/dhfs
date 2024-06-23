package com.usatiuk.dhfs.storage.objects.repository.distributed;

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

import java.util.List;
import java.util.Map;
import java.util.UUID;

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
            // TODO: Handle case when either is empty properly
            var ourVersion = md.getOurVersion();
            var bestVersion = md.getBestVersion();
            return md.getRemoteCopies().entrySet().stream()
                    .filter(entry -> entry.getValue().equals(bestVersion) || entry.getValue().equals(ourVersion))
                    .map(Map.Entry::getKey).toList();
        });

        return rpcClientFactory.withObjSyncClient(targets, client -> {
            var reply = client.getObject(GetObjectRequest.newBuilder().setSelfUuid(persistentRemoteHostsService.getSelfUuid().toString()).setName(jObject.getName()).build());

            var receivedSelfVer = reply.getObject().getHeader().getChangelog()
                    .getEntriesList().stream().filter(p -> p.getHost().equals(persistentRemoteHostsService.getSelfUuid().toString()))
                    .findFirst().map(ObjectChangelogEntry::getVersion).orElse(0L);

            var receivedTotalVer = reply.getObject().getHeader().getChangelog().getEntriesList()
                    .stream().map(ObjectChangelogEntry::getVersion).reduce(0L, Long::sum);

            return jObject.runWriteLockedMeta((md, b, v) -> {
                var outdated =
                        (md.getOurVersion() > receivedTotalVer)
                                || (md.getChangelog().get(persistentRemoteHostsService.getSelfUuid()) > receivedSelfVer);

                if (outdated) {
                    Log.error("Received older object then ours from " + reply.getSelfUuid() + " for " + reply.getObject().getHeader().getName());
                    throw new StatusRuntimeException(Status.ABORTED.withDescription("Received older object then ours"));
                }

                boolean newer = (receivedTotalVer > md.getOurVersion());
                // FIXME:? Hidden version difference?
                if (newer) {
                    if (syncHandler.tryHandleOneUpdate(UUID.fromString(reply.getSelfUuid()), reply.getObject().getHeader())) {
                        return reply.getObject().getContent();
                    }
                    Log.error("Received newer object then ours from " + reply.getSelfUuid() + " for " + reply.getObject().getHeader().getName());
                    throw new StatusRuntimeException(Status.ABORTED.withDescription("Received newer object then ours"));
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
