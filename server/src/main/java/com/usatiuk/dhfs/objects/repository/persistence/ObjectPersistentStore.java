package com.usatiuk.dhfs.objects.repository.persistence;

import com.google.protobuf.ByteString;
import com.usatiuk.dhfs.objects.persistence.BlobP;

import javax.annotation.Nonnull;
import java.util.List;

public interface ObjectPersistentStore {
    @Nonnull
    List<String> findObjects(String prefix);

    @Nonnull
    Boolean existsObject(String name);

    @Nonnull
    BlobP readObject(String name);

    void writeObject(String name, BlobP data);

    void deleteObject(String name);
}
