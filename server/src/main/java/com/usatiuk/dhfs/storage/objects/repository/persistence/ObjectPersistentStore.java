package com.usatiuk.dhfs.storage.objects.repository.persistence;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;

import javax.annotation.Nonnull;
import java.util.List;

public interface ObjectPersistentStore {
    @Nonnull
    List<String> findObjects(String prefix);
    @Nonnull
    Boolean existsObject(String name);

    @Nonnull
    byte[] readObject(String name);
    @Nonnull
    void writeObject(String name, byte[] data);
    @Nonnull
    void deleteObject(String name);
}
