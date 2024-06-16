package com.usatiuk.dhfs.storage.objects.repository.distributed;

import com.usatiuk.dhfs.objects.repository.distributed.*;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.commons.lang3.NotImplementedException;
import org.eclipse.microprofile.config.inject.ConfigProperty;

@ApplicationScoped
public class RemoteObjectServiceClient {
    @ConfigProperty(name = "dhfs.objects.distributed.selfname")
    String selfname;

    @Inject
    ObjectIndexService objectIndexService;

    @Inject
    RemoteHostManager remoteHostManager;

    public byte[] getObject(String name) {
        return remoteHostManager.withClient(client -> {
            var reply = client.getObject(GetObjectRequest.newBuilder().setName(name).build());

            var meta = objectIndexService.getMeta(name).orElseThrow(() -> {
                Log.error("Race when trying to fetch");
                return new NotImplementedException();
            });

            var receivedSelfVer = reply.getObject().getHeader().getChangelog()
                    .getEntriesList().stream().filter(p -> p.getHost().equals(selfname))
                    .findFirst().map(ObjectChangelogEntry::getVersion).orElse(0L);

            var receivedTotalVer = reply.getObject().getHeader().getChangelog().getEntriesList()
                    .stream().map(ObjectChangelogEntry::getVersion).reduce(0L, Long::sum);

            return meta.runReadLocked(md -> {
                var outdated =
                        (
                                (md.getTotalVersion() > receivedTotalVer)
                                        || (md.getChangelog().get(selfname) > receivedSelfVer)
                        )
                                && !md.getAssumeUnique();

                if (outdated) {
                    Log.error("Race when trying to fetch");
                    throw new NotImplementedException();
                }
                return reply.getObject().getContent().toByteArray();
            });
        });
    }

    public GetIndexReply getIndex() {
        return remoteHostManager.withClient(client -> {
            var req = GetIndexRequest.newBuilder().build();
            var reply = client.getIndex(req);
            return reply;
        });
    }

    public Boolean notifyUpdate(String name) {
        return remoteHostManager.withClient(client -> {
            var meta = objectIndexService.getMeta(name).orElseThrow(() -> {
                Log.error("Race when trying to notify update");
                return new NotImplementedException();
            });

            var builder = IndexUpdatePush.newBuilder().setSelfname(selfname);

            client.indexUpdate(builder.setHeader(
                    meta.runReadLocked(ObjectMetaData::toRpcHeader)
            ).build());
            return true;
        });
    }
}
