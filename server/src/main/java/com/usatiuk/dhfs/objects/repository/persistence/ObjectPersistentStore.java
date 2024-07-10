package com.usatiuk.dhfs.objects.repository.persistence;

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

    void writeObject(String name, ByteString data);

    void deleteObject(String name);
}
