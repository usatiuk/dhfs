package com.usatiuk.dhfs.storage.objects.repository.distributed;

import com.usatiuk.dhfs.storage.objects.data.Object;
import com.usatiuk.dhfs.storage.objects.repository.ObjectRepository;
import com.usatiuk.dhfs.storage.objects.repository.persistence.ObjectPersistentStore;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.core.Vertx;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.apache.commons.lang3.NotImplementedException;

import javax.annotation.Nonnull;
import java.io.IOException;

@ApplicationScoped
public class DistributedObjectRepository implements ObjectRepository {
    @Inject
    Vertx vertx;

    @Inject
    ObjectPersistentStore objectPersistentStore;

    @Inject
    ObjectIndexService objectIndexService;

    @Inject
    RemoteObjectServiceClient remoteObjectServiceClient;

    void init(@Observes @Priority(400) StartupEvent event) throws IOException {
    }

    void shutdown(@Observes @Priority(200) ShutdownEvent event) throws IOException {
    }

    @Nonnull
    @Override
    public Multi<String> findObjects(String namespace, String prefix) {
        throw new NotImplementedException();
    }

    @Nonnull
    @Override
    public Uni<Boolean> existsObject(String namespace, String name) {
        return Uni.createFrom().item(objectIndexService.exists(namespace, name));
    }

    @Nonnull
    @Override
    public Object readObject(String namespace, String name) {
        if (!objectIndexService.exists(namespace, name))
            throw new IllegalArgumentException("Object " + name + " doesn't exist");

        var infoOpt = objectIndexService.getMeta(namespace, name);
        if (infoOpt.isEmpty()) throw new IllegalArgumentException("Object " + name + " doesn't exist");

        var info = infoOpt.get();

        return info.runReadLocked(() -> {
            if (objectPersistentStore.existsObject(namespace, name).await().indefinitely())
                return objectPersistentStore.readObject(namespace, name).await().indefinitely();

            return remoteObjectServiceClient.getObject(namespace, name).map(got -> {
                objectPersistentStore.writeObject(namespace, got);
                return got;
            }).await().indefinitely();
        });
    }

    @Nonnull
    @Override
    public void writeObject(String namespace, Object object) {
        var info = objectIndexService.getOrCreateMeta(namespace, object.getName());

        info.runWriteLocked(() -> {
            objectPersistentStore.writeObject(namespace, object).await().indefinitely();
            info.setMtime(System.currentTimeMillis());
            remoteObjectServiceClient.notifyUpdate(namespace, object.getName()).await().indefinitely();
            return null;
        });
    }

    @Nonnull
    @Override
    public void deleteObject(String namespace, String name) {
        throw new NotImplementedException();
    }

    @Nonnull
    @Override
    public Uni<Void> createNamespace(String namespace) {
        return Uni.createFrom().voidItem();
    }

    @Nonnull
    @Override
    public Uni<Void> deleteNamespace(String namespace) {
        throw new NotImplementedException();
    }
}
