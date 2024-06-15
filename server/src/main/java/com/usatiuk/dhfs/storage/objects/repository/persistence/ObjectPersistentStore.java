package com.usatiuk.dhfs.storage.objects.repository.persistence;

import com.usatiuk.dhfs.storage.objects.data.Object;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;

import javax.annotation.Nonnull;

public interface ObjectPersistentStore {
    @Nonnull
    Multi<String> findObjects(String namespace, String prefix);
    @Nonnull
    Uni<Boolean> existsObject(String namespace, String name);

    @Nonnull
    Uni<Object> readObject(String namespace, String name);
    @Nonnull
    Uni<Void> writeObject(String namespace, Object object);
    @Nonnull
    Uni<Void> deleteObject(String namespace, String name);

    @Nonnull
    Uni<Void> createNamespace(String namespace);
    @Nonnull
    Uni<Void> deleteNamespace(String namespace);
}
