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
    public Uni<Void> writeObject(String namespace, String name, Object data);
}
