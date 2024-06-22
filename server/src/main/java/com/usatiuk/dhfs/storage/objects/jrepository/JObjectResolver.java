package com.usatiuk.dhfs.storage.objects.jrepository;

import com.usatiuk.dhfs.storage.SerializationHelper;
import com.usatiuk.dhfs.storage.objects.repository.distributed.InvalidationQueueService;
import com.usatiuk.dhfs.storage.objects.repository.distributed.RemoteObjectServiceClient;
import com.usatiuk.dhfs.storage.objects.repository.persistence.ObjectPersistentStore;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.logging.Log;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.Optional;

@Singleton
public class JObjectResolver {
    @Inject
    ObjectPersistentStore objectPersistentStore;

    @Inject
    RemoteObjectServiceClient remoteObjectServiceClient;

    @Inject
    InvalidationQueueService invalidationQueueService;

    @Inject
    JObjectWriteback jObjectWriteback;

    @ConfigProperty(name = "dhfs.objects.distributed.selfname")
    String selfname;

    public <T extends JObjectData> Optional<T> resolveDataLocal(JObject<T> jObject) {
        jObject.assertRWLock();
        if (objectPersistentStore.existsObject(jObject.getName()))
            return Optional.of(SerializationHelper.deserialize(objectPersistentStore.readObject(jObject.getName())));
        return Optional.empty();
    }

    public <T extends JObjectData> T resolveData(JObject<T> jObject) {
        jObject.assertRWLock();
        var local = resolveDataLocal(jObject);
        if (local.isPresent()) return local.get();

        var obj = remoteObjectServiceClient.getObject(jObject);
        objectPersistentStore.writeObject(jObject.getName(), obj);
        return SerializationHelper.deserialize(obj);
    }

    public void removeLocal(JObject<?> jObject, String name) {
        jObject.assertRWLock();
        try {
//            Log.info("Deleting " + name);
            jObjectWriteback.remove(name);
            objectPersistentStore.deleteObject(name);
        } catch (StatusRuntimeException sx) {
            if (sx.getStatus() != Status.NOT_FOUND)
                Log.info("Couldn't delete object from persistent store: ", sx);
        } catch (Exception e) {
            Log.info("Couldn't delete object from persistent store: ", e);
        }
    }

    public void notifyWrite(JObject<?> self) {
        self.assertRWLock();
        jObjectWriteback.markDirty(self.getName(), self);
        if (self.isResolved()) {
            // FIXME:?
            invalidationQueueService.pushInvalidationToAll(self.getName());
        }
    }

    public void bumpVersionSelf(JObject<?> self) {
        self.assertRWLock();
        self.runWriteLockedMeta((m, bump, invalidate) -> {
            m.bumpVersion(selfname);
            return null;
        });
    }
}
