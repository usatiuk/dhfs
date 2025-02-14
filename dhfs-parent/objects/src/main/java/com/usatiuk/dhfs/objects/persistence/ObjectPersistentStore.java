package com.usatiuk.dhfs.objects.persistence;

import com.google.protobuf.ByteString;
import com.usatiuk.dhfs.objects.JObjectKey;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Optional;

// Persistent storage of objects
// All changes are written as sequential transactions
public interface ObjectPersistentStore {
    @Nonnull
    Collection<JObjectKey> findAllObjects();

    @Nonnull
    Optional<ByteString> readObject(JObjectKey name);

    void commitTx(TxManifestRaw names);

    long getTotalSpace();

    long getFreeSpace();

    long getUsableSpace();
}
