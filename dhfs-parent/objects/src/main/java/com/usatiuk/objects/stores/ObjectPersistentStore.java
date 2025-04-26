package com.usatiuk.objects.stores;

import com.google.protobuf.ByteString;
import com.usatiuk.objects.JObjectKey;
import com.usatiuk.objects.snapshot.Snapshot;

import javax.annotation.Nonnull;
import java.util.Optional;

// Persistent storage of objects
// All changes are written as sequential transactions
public interface ObjectPersistentStore {
    Snapshot<JObjectKey, ByteString> getSnapshot();

    Runnable prepareTx(TxManifestRaw names, long txId);

    long getTotalSpace();

    long getFreeSpace();

    long getUsableSpace();
}
