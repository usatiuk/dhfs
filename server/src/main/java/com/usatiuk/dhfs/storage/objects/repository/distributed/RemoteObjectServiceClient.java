package com.usatiuk.dhfs.storage.objects.repository.distributed;

import com.usatiuk.dhfs.objects.repository.distributed.GetIndexRequest;
import com.usatiuk.dhfs.objects.repository.distributed.GetObjectRequest;
import com.usatiuk.dhfs.objects.repository.distributed.IndexUpdatePush;
import com.usatiuk.dhfs.objects.repository.distributed.ObjectHeader;
import com.usatiuk.dhfs.storage.objects.data.Object;
import io.quarkus.logging.Log;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.commons.lang3.NotImplementedException;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.List;

@ApplicationScoped
public class RemoteObjectServiceClient {
    @ConfigProperty(name = "dhfs.objects.distributed.selfname")
    String selfname;

    @Inject
    ObjectIndexService objectIndexService;

    @Inject
    RemoteHostManager remoteHostManager;

    public Uni<Object> getObject(String namespace, String name) {
        return remoteHostManager.withClient(client -> {
            var req = GetObjectRequest.newBuilder().setNamespace(namespace).setName(name).build();
            var reply = client.getObject(req);
            var metaOpt = objectIndexService.getMeta(namespace, name);
            if (metaOpt.isEmpty()) throw new RuntimeException("Oops!");
            var meta = metaOpt.get();
            if (meta.getMtime() != reply.getObject().getHeader().getMtime()) {
                if (!meta.getAssumeUnique() && (meta.getAssumeUnique() != reply.getObject().getHeader().getAssumeUnique())) {
                    Log.error("Conflict!");
                    throw new NotImplementedException();
                }
            }
            return Uni.createFrom().item(new Object(
                    reply.getObject().getHeader().getNamespace(),
                    reply.getObject().getHeader().getName(),
                    reply.getObject().getContent().toByteArray()
            ));
        });
    }

    public List<ObjectHeader> getIndex() {
        return remoteHostManager.withClient(client -> {
            var req = GetIndexRequest.newBuilder().build();
            var reply = client.getIndex(req);
            return reply.getObjectsList();
        });
    }

    public Boolean notifyUpdate(String namespace, String name, long prevMtime) {
        return remoteHostManager.withClient(client -> {
            var metaOpt = objectIndexService.getMeta(namespace, name);
            if (metaOpt.isEmpty()) throw new RuntimeException("Oops!");
            var meta = metaOpt.get();

            var req = IndexUpdatePush.newBuilder().setSelfname(selfname
                    ).setNamespace(namespace).setName(name).setAssumeUnique(meta.getAssumeUnique())
                    .setMtime(meta.getMtime()).setPrevMtime(prevMtime).build();
            client.indexUpdate(req);
            return true;
        });
    }
}
