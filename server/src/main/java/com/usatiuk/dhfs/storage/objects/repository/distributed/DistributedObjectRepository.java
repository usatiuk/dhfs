package com.usatiuk.dhfs.storage.objects.repository.distributed;

import com.usatiuk.dhfs.objects.repository.distributed.IndexUpdatePush;
import com.usatiuk.dhfs.storage.objects.data.Object;
import com.usatiuk.dhfs.storage.objects.repository.ObjectRepository;
import com.usatiuk.dhfs.storage.objects.repository.persistence.ObjectPersistentStore;
import io.quarkus.logging.Log;
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
import org.eclipse.microprofile.config.inject.ConfigProperty;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Optional;

@ApplicationScoped
public class DistributedObjectRepository implements ObjectRepository {
    @ConfigProperty(name = "dhfs.objects.distributed.selfname")
    String selfname;
    @Inject
    Vertx vertx;

    @Inject
    ObjectPersistentStore objectPersistentStore;

    @Inject
    ObjectIndexService objectIndexService;

    @Inject
    RemoteObjectServiceClient remoteObjectServiceClient;

    @Inject
    SyncHandler syncHandler;

    void init(@Observes @Priority(400) StartupEvent event) throws IOException {
        try {
            Log.info("Starting sync");
            var got = remoteObjectServiceClient.getIndex();
            for (var h : got) {
                var prevMtime = objectIndexService.exists(h.getNamespace(), h.getName())
                        ? objectIndexService.getMeta(h.getNamespace(), h.getName()).get().getMtime()
                        : 0;
                syncHandler.handleRemoteUpdate(
                        IndexUpdatePush.newBuilder().setSelfname(selfname
                                ).setNamespace(h.getNamespace()).setName(h.getName()).setAssumeUnique(h.getAssumeUnique())
                                .setMtime(h.getMtime()).setPrevMtime(prevMtime).build()).await().indefinitely();
            }
            Log.info("Sync complete");
        } catch (Exception e) {
            Log.error("Error when fetching remote index:");
            Log.error(e);
        }
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

        Optional<Object> read = info.runReadLocked(() -> {
            if (objectPersistentStore.existsObject(namespace, name).await().indefinitely())
                return Optional.of(objectPersistentStore.readObject(namespace, name).await().indefinitely());
            return Optional.empty();
        });
        if (read.isPresent()) return read.get();
        // Race?

        return info.runWriteLocked(() -> {
            return remoteObjectServiceClient.getObject(namespace, name).map(got -> {
                objectPersistentStore.writeObject(namespace, got);
                return got;
            }).await().indefinitely();
        });
    }

    @Nonnull
    @Override
    public void writeObject(String namespace, Object object, Boolean canIgnoreConflict) {
        var info = objectIndexService.getOrCreateMeta(namespace, object.getName(), canIgnoreConflict);

        info.runWriteLocked(() -> {
            objectPersistentStore.writeObject(namespace, object).await().indefinitely();
            var prevMtime = info.getMtime();
            info.setMtime(System.currentTimeMillis());
            try {
                Log.warn("Updating object " + object.getNamespace() + "/" + object.getName() + " from: " + info.getMtime() + " to: " + prevMtime);
                remoteObjectServiceClient.notifyUpdate(namespace, object.getName(), prevMtime);
                Log.warn("Updating object complete" + object.getNamespace() + "/" + object.getName() + " from: " + info.getMtime() + " to: " + prevMtime);
            } catch (Exception e) {
                Log.error("Error when notifying remote update:");
                Log.error(e);
                Log.error(e.getCause());
            }
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
