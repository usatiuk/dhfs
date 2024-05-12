package com.usatiuk.dhfs.storage.repository;

import com.usatiuk.dhfs.storage.data.Object;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;

public interface ObjectRepository {
    @Nonnull
    public Multi<String> findObjects(String namespace, String prefix);

    @Nonnull
    public Uni<Object> readObject(String namespace, String name);
    @Nonnull
    public Uni<Void> writeObject(String namespace, String name, ByteBuffer data);
    @Nonnull
    public Uni<Void> deleteObject(String namespace, String name);

    @Nonnull
    public Uni<Void> createNamespace(String namespace);
    @Nonnull
    public Uni<Void> deleteNamespace(String namespace);
}
