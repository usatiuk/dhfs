package com.usatiuk.dhfs.objects.persistence;

import com.usatiuk.dhfs.objects.JData;
import com.usatiuk.dhfs.objects.JObjectKey;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Optional;

public interface ObjectPersistentStore {
    @Nonnull
    Collection<JObjectKey> findAllObjects();

    @Nonnull
    Optional<JData> readObject(JObjectKey name);

    void writeObjectDirect(JObjectKey name, JData object);

    void writeObject(JObjectKey name, JData object);



    void commitTx(TxManifest names);

    // Deletes object metadata and data
    void deleteObjectDirect(JObjectKey name);

    long getTotalSpace();

    long getFreeSpace();

    long getUsableSpace();
}
