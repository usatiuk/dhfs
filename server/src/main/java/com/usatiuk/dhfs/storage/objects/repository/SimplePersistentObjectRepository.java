package com.usatiuk.dhfs.storage.objects.repository;

import com.usatiuk.dhfs.storage.objects.data.Object;
import com.usatiuk.dhfs.storage.objects.repository.persistence.ObjectPersistentStore;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import javax.annotation.Nonnull;

@ApplicationScoped
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
    public Uni<Object> readObject(String namespace, String name) {
        return objectPersistentStore.readObject(namespace, name);
    }

    @Nonnull
    @Override
    public Uni<Void> writeObject(String namespace, Object object) {
        return objectPersistentStore.writeObject(namespace, object);
    }

    @Nonnull
    @Override
    public Uni<Void> deleteObject(String namespace, String name) {
        return objectPersistentStore.deleteObject(namespace, name);
    }

    @Nonnull
    @Override
    public Uni<Void> createNamespace(String namespace) {
        return objectPersistentStore.createNamespace(namespace);
    }

    @Nonnull
    @Override
    public Uni<Void> deleteNamespace(String namespace) {
        return objectPersistentStore.deleteNamespace(namespace);
    }
}
