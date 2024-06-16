package com.usatiuk.dhfs.storage.objects.repository;

import com.usatiuk.dhfs.storage.objects.data.Object;
import com.usatiuk.dhfs.storage.objects.repository.persistence.ObjectPersistentStore;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import org.apache.commons.lang3.NotImplementedException;

import javax.annotation.Nonnull;

//@ApplicationScoped
public class SimplePersistentObjectRepository implements ObjectRepository {
    @Inject
    ObjectPersistentStore objectPersistentStore;

    @Nonnull
    @Override
    public Multi<String> findObjects(String namespace, String prefix) {
        return objectPersistentStore.findObjects(namespace, prefix);
    }

    @Nonnull
    @Override
    public Uni<Boolean> existsObject(String namespace, String name) {
        return objectPersistentStore.existsObject(namespace, name);
    }

    @Nonnull
    @Override
    public Object readObject(String namespace, String name) {
        return objectPersistentStore.readObject(namespace, name).await().indefinitely();
    }

    @Nonnull
    @Override
    public void writeObject(String namespace, Object object, Boolean canIgnoreConflict) {
        objectPersistentStore.writeObject(namespace, object).await().indefinitely();
    }

    @Nonnull
    @Override
    public void deleteObject(String namespace, String name) {
        objectPersistentStore.deleteObject(namespace, name).await().indefinitely();
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
