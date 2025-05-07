package com.usatiuk.objects.stores;

import com.google.protobuf.ByteString;
import com.usatiuk.objects.JObjectKey;
import com.usatiuk.objects.snapshot.Snapshot;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;
import java.util.Optional;

// Persistent storage of objects
// All changes are written as sequential transactions
public interface ObjectPersistentStore {
    Snapshot<JObjectKey, ByteBuffer> getSnapshot();

    void commitTx(TxManifestRaw names, long txId);

    long getTotalSpace();

    long getFreeSpace();

    long getUsableSpace();
}
