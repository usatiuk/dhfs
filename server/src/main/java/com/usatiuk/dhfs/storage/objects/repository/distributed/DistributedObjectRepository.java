package com.usatiuk.dhfs.storage.objects.repository.distributed;

import com.usatiuk.dhfs.objects.repository.distributed.IndexUpdatePush;
import com.usatiuk.dhfs.storage.objects.repository.ObjectRepository;
import com.usatiuk.dhfs.storage.objects.repository.persistence.ObjectPersistentStore;
import io.quarkus.logging.Log;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import io.vertx.mutiny.core.Vertx;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.apache.commons.lang3.NotImplementedException;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.List;
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
                var prevMtime = objectIndexService.exists(h.getName())
                        ? objectIndexService.getMeta(h.getName()).get().getMtime()
                        : 0;
                syncHandler.handleRemoteUpdate(
                        IndexUpdatePush.newBuilder().setSelfname(selfname).setName(h.getName())
                                .setAssumeUnique(h.getAssumeUnique())
                                .setMtime(h.getMtime()).setPrevMtime(prevMtime).build());
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
    public List<String> findObjects(String prefix) {
        throw new NotImplementedException();
    }

    @Nonnull
    @Override
    public Boolean existsObject(String name) {
        return objectIndexService.exists(name);
    }

    @Nonnull
    @Override
    public byte[] readObject(String name) {
        if (!objectIndexService.exists(name))
            throw new IllegalArgumentException("Object " + name + " doesn't exist");

        var infoOpt = objectIndexService.getMeta(name);
        if (infoOpt.isEmpty()) throw new IllegalArgumentException("Object " + name + " doesn't exist");

        var info = infoOpt.get();

        Optional<byte[]> read = info.runReadLocked(() -> {
            if (objectPersistentStore.existsObject(name))
                return Optional.of(objectPersistentStore.readObject(name));
            return Optional.empty();
        });
        if (read.isPresent()) return read.get();
        // Race?

        return info.runWriteLocked(() -> {
            var obj = remoteObjectServiceClient.getObject(name);
            objectPersistentStore.writeObject(name, obj);
            return obj;
        });
    }

    @Nonnull
    @Override
    public void writeObject(String name, byte[] data, Boolean canIgnoreConflict) {
        var info = objectIndexService.getOrCreateMeta(name, canIgnoreConflict);

        info.runWriteLocked(() -> {
            objectPersistentStore.writeObject(name, data);
            var prevMtime = info.getMtime();
            info.setMtime(System.currentTimeMillis());
            try {
                remoteObjectServiceClient.notifyUpdate(name, prevMtime);
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
    public void deleteObject(String name) {
        throw new NotImplementedException();
    }
}
