package com.usatiuk.dhfs.storage.objects.repository.persistence;

import com.google.protobuf.ByteString;

import javax.annotation.Nonnull;
import java.util.List;

public interface ObjectPersistentStore {
    @Nonnull
    List<String> findObjects(String prefix);
    @Nonnull
    Boolean existsObject(String name);

    @Nonnull
    ByteString readObject(String name);
    @Nonnull
    void writeObject(String name, ByteString data);
    @Nonnull
    void deleteObject(String name);
}
