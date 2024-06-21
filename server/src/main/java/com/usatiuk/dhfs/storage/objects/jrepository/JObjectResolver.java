package com.usatiuk.dhfs.storage.objects.jrepository;

import com.usatiuk.dhfs.storage.DeserializationHelper;
import com.usatiuk.dhfs.storage.objects.repository.distributed.InvalidationQueueService;
import com.usatiuk.dhfs.storage.objects.repository.distributed.RemoteObjectServiceClient;
import com.usatiuk.dhfs.storage.objects.repository.persistence.ObjectPersistentStore;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.commons.lang3.SerializationUtils;

@ApplicationScoped
public class JObjectResolver {
    @Inject
    ObjectPersistentStore objectPersistentStore;

    @Inject
    RemoteObjectServiceClient remoteObjectServiceClient;

    @Inject
    InvalidationQueueService invalidationQueueService;

    public <T extends JObjectData> T resolveData(JObject<T> jObject) {
        if (objectPersistentStore.existsObject(jObject.getName()))
            return DeserializationHelper.deserialize(objectPersistentStore.readObject(jObject.getName()));

        var obj = remoteObjectServiceClient.getObject(jObject);
        objectPersistentStore.writeObject(jObject.getName(), obj);
        return DeserializationHelper.deserialize(obj);
    }

    public void notifyWrite(JObject<?> self) {
        objectPersistentStore.writeObject("meta_" + self.getName(), self.runReadLocked((m) -> SerializationUtils.serialize(m)));
        objectPersistentStore.writeObject(self.getName(), self.runReadLocked((m, d) -> SerializationUtils.serialize(d)));
        invalidationQueueService.pushInvalidationToAll(self.getName());
    }
}
