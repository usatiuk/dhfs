package com.usatiuk.dhfs.storage.objects.repository.distributed;

import com.google.protobuf.ByteString;
import com.usatiuk.dhfs.objects.repository.distributed.*;
import com.usatiuk.dhfs.storage.objects.jrepository.JObject;
import com.usatiuk.dhfs.storage.objects.jrepository.JObjectManager;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.tuple.Pair;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.List;
import java.util.Map;

@ApplicationScoped
public class RemoteObjectServiceClient {
    @ConfigProperty(name = "dhfs.objects.distributed.selfname")
    String selfname;

    @Inject
    RemoteHostManager remoteHostManager;

    @Inject
    JObjectManager jObjectManager;

    public Pair<ObjectHeader, ByteString> getSpecificObject(String host, String name) {
        return remoteHostManager.withClient(host, client -> {
            var reply = client.getObject(GetObjectRequest.newBuilder().setSelfname(selfname).setName(name).build());
            return Pair.of(reply.getObject().getHeader(), reply.getObject().getContent());
        });
    }

    public ByteString getObject(JObject<?> jObject) {
        jObject.assertRWLock();

        var targets = jObject.runWriteLockedMeta((md, b, v) -> {
            var bestVer = md.getBestVersion();
            return md.getRemoteCopies().entrySet().stream().filter(entry -> entry.getValue().equals(bestVer)).map(Map.Entry::getKey).toList();
        });

        return remoteHostManager.withClientAny(targets, client -> {
            var reply = client.getObject(GetObjectRequest.newBuilder().setSelfname(selfname).setName(jObject.getName()).build());

            var receivedSelfVer = reply.getObject().getHeader().getChangelog()
                    .getEntriesList().stream().filter(p -> p.getHost().equals(selfname))
                    .findFirst().map(ObjectChangelogEntry::getVersion).orElse(0L);

            var receivedTotalVer = reply.getObject().getHeader().getChangelog().getEntriesList()
                    .stream().map(ObjectChangelogEntry::getVersion).reduce(0L, Long::sum);

            return jObject.runWriteLockedMeta((md, b, v) -> {
                var outdated =
                        (md.getOurVersion() > receivedTotalVer)
                                || (md.getChangelog().get(selfname) > receivedSelfVer);

                if (outdated) {
                    Log.error("Race when trying to fetch");
                    throw new NotImplementedException();
                }
                return reply.getObject().getContent();
            });
        });
    }

    public IndexUpdatePush getIndex(String host) {
        return remoteHostManager.withClient(host, client -> {
            var req = GetIndexRequest.newBuilder().setSelfname(selfname).build();
            return client.getIndex(req);
        });
    }

    public List<IndexUpdateError> notifyUpdate(String host, List<String> names) {
        var builder = IndexUpdatePush.newBuilder().setSelfname(selfname);
        for (var v : names) {
            var obj = jObjectManager.get(v);
            if (obj.isEmpty()) continue;
            builder.addHeader(obj.get().runReadLocked(ObjectMetadata::toRpcHeader));
        }

        var send = builder.build();

        return remoteHostManager.withClient(host, client -> client.indexUpdate(send).getErrorsList());
    }
}
