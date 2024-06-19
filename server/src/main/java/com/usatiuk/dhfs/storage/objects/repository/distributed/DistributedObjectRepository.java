package com.usatiuk.dhfs.storage.objects.repository.distributed;

import com.usatiuk.dhfs.storage.objects.repository.ObjectRepository;
import com.usatiuk.dhfs.storage.objects.repository.persistence.ObjectPersistentStore;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
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

    @Inject
    InvalidationQueueService invalidationQueueService;

    void init(@Observes @Priority(400) StartupEvent event) throws IOException {

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
        var infoOpt = objectIndexService.getMeta(name);
        if (infoOpt.isEmpty())
            throw new StatusRuntimeException(Status.NOT_FOUND.withDescription("Object " + name + " doesn't exist"));

        var info = infoOpt.get();

        Optional<byte[]> read = info.runReadLocked((data) -> {
            if (objectPersistentStore.existsObject(name))
                return Optional.of(objectPersistentStore.readObject(name));
            return Optional.empty();
        });
        if (read.isPresent()) return read.get();

        // Possible race if it got deleted?
        return info.runWriteLocked((data) -> {
            var obj = remoteObjectServiceClient.getObject(name);
            objectPersistentStore.writeObject(name, obj);
            return obj;
        });
    }

    @Nonnull
    @Override
    public void writeObject(String name, byte[] data, String conflictResolver) {
        var info = objectIndexService.getOrCreateMeta(name, conflictResolver);

        info.runWriteLocked((metaData) -> {
            objectPersistentStore.writeObject(name, data);
            metaData.getChangelog().merge(selfname, 1L, Long::sum);
            return null;
        });
        // FIXME: Race?
        try {
            invalidationQueueService.pushInvalidationToAll(name);
        } catch (Exception e) {
            Log.error("Error when notifying remote update:");
            Log.error(e);
            Log.error(e.getCause());
        }
    }

    @Nonnull
    @Override
    public void deleteObject(String name) {
        throw new NotImplementedException();
    }
}
