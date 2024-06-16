package com.usatiuk.dhfs.storage.objects.repository;

import com.usatiuk.dhfs.storage.objects.data.Object;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;

import javax.annotation.Nonnull;

public interface ObjectRepository {
    @Nonnull
    Multi<String> findObjects(String namespace, String prefix);
    @Nonnull
    Uni<Boolean> existsObject(String namespace, String name);

    @Nonnull
    Object readObject(String namespace, String name);
    @Nonnull
    void writeObject(String namespace, Object object, Boolean canIgnoreConflict);
    @Nonnull
    void deleteObject(String namespace, String name);

    @Nonnull
    Uni<Void> createNamespace(String namespace);
    @Nonnull
    Uni<Void> deleteNamespace(String namespace);
}
