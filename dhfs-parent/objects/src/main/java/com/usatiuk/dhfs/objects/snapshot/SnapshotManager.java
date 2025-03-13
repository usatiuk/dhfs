package com.usatiuk.dhfs.objects.snapshot;

import com.usatiuk.dhfs.objects.*;
import com.usatiuk.dhfs.objects.persistence.IteratorStart;
import com.usatiuk.dhfs.objects.transaction.TxRecord;
import com.usatiuk.dhfs.utils.AutoCloseableNoThrow;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.commons.lang3.tuple.Pair;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.pcollections.TreePMap;

import javax.annotation.Nonnull;
import java.lang.ref.Cleaner;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;
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
