package com.usatiuk.dhfs.objects.snapshot;

import com.usatiuk.dhfs.objects.JDataVersionedWrapper;
import com.usatiuk.dhfs.objects.JObjectKey;
import com.usatiuk.dhfs.objects.stores.WritebackObjectPersistentStore;
import com.usatiuk.dhfs.objects.transaction.TxRecord;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Optional;
import java.util.function.Consumer;

@ApplicationScoped
public class SnapshotManager {
    @Inject
    WritebackObjectPersistentStore writebackStore;

    public Snapshot<JObjectKey, JDataVersionedWrapper> createSnapshot() {
        return writebackStore.getSnapshot();
    }

    // This should not be called for the same objects concurrently
    public Consumer<Runnable> commitTx(Collection<TxRecord.TxObjectRecord<?>> writes) {
        // TODO: FIXME:
        synchronized (this) {
            return writebackStore.commitTx(writes, (id, commit) -> {
                commit.run();
            });
        }
    }

    @Nonnull
    public Optional<JDataVersionedWrapper> readObjectDirect(JObjectKey name) {
        return writebackStore.readObject(name);
    }
}
