package com.usatiuk.dhfs.storage.objects.repository.persistence;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;

import javax.annotation.Nonnull;

public interface ObjectPersistentStore {
    @Nonnull
    Multi<String> findObjects(String prefix);
    @Nonnull
    Uni<Boolean> existsObject(String name);

    @Nonnull
    Uni<byte[]> readObject(String name);
    @Nonnull
    Uni<Void> writeObject(String name, byte[] data);
    @Nonnull
    Uni<Void> deleteObject(String name);
}
