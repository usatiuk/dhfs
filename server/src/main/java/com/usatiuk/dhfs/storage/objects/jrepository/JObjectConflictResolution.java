package com.usatiuk.dhfs.storage.objects.jrepository;

import com.usatiuk.dhfs.objects.repository.distributed.ObjectHeader;
import com.usatiuk.dhfs.storage.DeserializationHelper;
import com.usatiuk.dhfs.storage.objects.repository.distributed.ConflictResolver;
import com.usatiuk.dhfs.storage.objects.repository.distributed.ObjectMetaData;
import com.usatiuk.dhfs.storage.objects.repository.persistence.ObjectPersistentStore;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.inject.Named;

@ApplicationScoped
@Named("JObjectConflictResolution")
public class JObjectConflictResolution implements ConflictResolver {
    @Inject
    Instance<ConflictResolver> conflictResolvers;

    @Inject
    ObjectPersistentStore objectPersistentStore;

    @Override
    public ConflictResolutionResult
    resolve(String conflictHost, ObjectHeader conflictSource, ObjectMetaData localMeta) {
        var oursData = objectPersistentStore.readObject(localMeta.getName());
        var ours = (JObject) DeserializationHelper.deserialize(oursData);
        return conflictResolvers.select(ours.getConflictResolver()).get().resolve(conflictHost, conflictSource, localMeta);
    }
}
