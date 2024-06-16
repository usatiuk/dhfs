package com.usatiuk.dhfs.storage.objects.repository;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;

import javax.annotation.Nonnull;

public interface ObjectRepository {
    @Nonnull
    Multi<String> findObjects(String prefix);
    @Nonnull
    Uni<Boolean> existsObject(String name);

    @Nonnull
    byte[] readObject(String name);
    @Nonnull
    void writeObject(String name, byte[] data, Boolean canIgnoreConflict);
    @Nonnull
    void deleteObject(String name);
}
