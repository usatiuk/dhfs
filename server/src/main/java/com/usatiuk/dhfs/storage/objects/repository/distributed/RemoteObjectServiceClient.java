package com.usatiuk.dhfs.storage.objects.repository.distributed;

import com.usatiuk.dhfs.storage.objects.data.Object;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class RemoteObjectServiceClient {
    @Inject
    ObjectIndexService objectIndexService;

    public Uni<Object> getObject(String namespace, String name) {
        return Uni.createFrom().item(null);
    }

    public Uni<Boolean> notifyUpdate(String namespace, String name) {
        return Uni.createFrom().item(true);
    }
}
