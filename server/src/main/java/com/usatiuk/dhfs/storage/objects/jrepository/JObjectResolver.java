package com.usatiuk.dhfs.storage.objects.jrepository;

import com.usatiuk.dhfs.storage.DeserializationHelper;
import com.usatiuk.dhfs.storage.objects.repository.distributed.InvalidationQueueService;
import com.usatiuk.dhfs.storage.objects.repository.distributed.RemoteObjectServiceClient;
import com.usatiuk.dhfs.storage.objects.repository.persistence.ObjectPersistentStore;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.logging.Log;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.eclipse.microprofile.config.inject.ConfigProperty;

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

    public <T extends JObjectData> T resolveData(JObject<T> jObject) {
        if (objectPersistentStore.existsObject(jObject.getName()))
            return DeserializationHelper.deserialize(objectPersistentStore.readObject(jObject.getName()));

        var obj = remoteObjectServiceClient.getObject(jObject);
        objectPersistentStore.writeObject(jObject.getName(), obj);
        return DeserializationHelper.deserialize(obj);
    }

    public void removeLocal(JObject<?> jObject, String name) {
        jObject.assertRWLock();
        try {
            Log.info("Deleting " + name);
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
        jObjectWriteback.markDirty(self.getName(), self);
        if (self.isResolved()) {
            // FIXME:?
            invalidationQueueService.pushInvalidationToAll(self.getName());
        }
    }

    public void bumpVersionSelf(JObject<?> self) {
        self.runWriteLockedMeta((m, bump, invalidate) -> {
            m.bumpVersion(selfname);
            return null;
        });
    }
}
