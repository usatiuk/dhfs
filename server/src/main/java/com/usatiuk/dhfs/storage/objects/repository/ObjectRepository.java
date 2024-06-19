package com.usatiuk.dhfs.storage.objects.repository;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;

import javax.annotation.Nonnull;
import java.util.List;

public interface ObjectRepository {
    @Nonnull
    List<String> findObjects(String prefix);
    @Nonnull
    Boolean existsObject(String name);

    @Nonnull
    byte[] readObject(String name);
    @Nonnull
    void writeObject(String name, byte[] data, String conflictResolver);
    @Nonnull
    void deleteObject(String name);
}
