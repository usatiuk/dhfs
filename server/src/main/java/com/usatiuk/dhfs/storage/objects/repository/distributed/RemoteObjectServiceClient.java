package com.usatiuk.dhfs.storage.objects.repository.distributed;

import com.usatiuk.dhfs.objects.repository.distributed.*;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.tuple.Pair;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.Map;

@ApplicationScoped
public class RemoteObjectServiceClient {
    @ConfigProperty(name = "dhfs.objects.distributed.selfname")
    String selfname;

    @Inject
    ObjectIndexService objectIndexService;

    @Inject
    RemoteHostManager remoteHostManager;

    public Pair<ObjectHeader, byte[]> getSpecificObject(String host, String name) {
        return remoteHostManager.withClient(host, client -> {
            var reply = client.getObject(GetObjectRequest.newBuilder().setSelfname(selfname).setName(name).build());
            return Pair.of(reply.getObject().getHeader(), reply.getObject().getContent().toByteArray());
        });
    }

    public byte[] getObject(String name) {
        var meta = objectIndexService.getMeta(name).orElseThrow(() -> {
            Log.error("Race when trying to fetch");
            return new NotImplementedException();
        });

        var targets = meta.runReadLocked(d -> {
            var bestVer = d.getBestVersion();
            return d.getRemoteCopies().entrySet().stream().filter(entry -> entry.getValue().equals(bestVer)).map(Map.Entry::getKey).toList();
        });

        return remoteHostManager.withClientAny(targets, client -> {
            var reply = client.getObject(GetObjectRequest.newBuilder().setSelfname(selfname).setName(name).build());

            var receivedSelfVer = reply.getObject().getHeader().getChangelog()
                    .getEntriesList().stream().filter(p -> p.getHost().equals(selfname))
                    .findFirst().map(ObjectChangelogEntry::getVersion).orElse(0L);

            var receivedTotalVer = reply.getObject().getHeader().getChangelog().getEntriesList()
                    .stream().map(ObjectChangelogEntry::getVersion).reduce(0L, Long::sum);

            return meta.runReadLocked(md -> {
                var outdated =
                        (md.getTotalVersion() > receivedTotalVer)
                                || (md.getChangelog().get(selfname) > receivedSelfVer);

                if (outdated) {
                    Log.error("Race when trying to fetch");
                    throw new NotImplementedException();
                }
                return reply.getObject().getContent().toByteArray();
            });
        });
    }

    public GetIndexReply getIndex(String host) {
        return remoteHostManager.withClient(host, client -> {
            var req = GetIndexRequest.newBuilder().setSelfname(selfname).build();
            var reply = client.getIndex(req);
            return reply;
        });
    }

    public void notifyUpdate(String host, String name) {
        remoteHostManager.withClient(host, client -> {
            var meta = objectIndexService.getMeta(name).orElseThrow(() -> {
                Log.error("Race when trying to notify update");
                return new NotImplementedException();
            });

            var builder = IndexUpdatePush.newBuilder().setSelfname(selfname);

            client.indexUpdate(builder.setHeader(
                    meta.runReadLocked(ObjectMetaData::toRpcHeader)
            ).build());
            return null;
        });
    }
}
