package com.usatiuk.dhfs.objects;

import com.usatiuk.dhfs.objects.persistence.CachingObjectPersistentStore;
import com.usatiuk.dhfs.objects.persistence.IteratorStart;
import com.usatiuk.dhfs.objects.transaction.TxRecord;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.HashSet;
import java.util.Optional;
import java.util.function.Consumer;

@ApplicationScoped
public class WritebackObjectPersistentStore {
    @Inject
    CachingObjectPersistentStore delegate;
    @Inject
    TxWriteback txWriteback;

    @Nonnull
    public Collection<JObjectKey> findAllObjects() {
        var pending = txWriteback.getPendingWrites();
        var found = new HashSet<>(delegate.findAllObjects());
        for (var p : pending) {
            switch (p) {
                case TxWriteback.PendingWrite write -> found.add(write.data().data().key());
                case TxWriteback.PendingDelete deleted -> found.remove(deleted.key());
                default -> throw new IllegalStateException("Unexpected value: " + p);
            }
        }
        return found;
    }

    @Nonnull
    Optional<JDataVersionedWrapper> readObject(JObjectKey name) {
        var pending = txWriteback.getPendingWrite(name).orElse(null);
        return switch (pending) {
            case TxWriteback.PendingWrite write -> Optional.of(write.data());
            case TxWriteback.PendingDelete ignored -> Optional.empty();
            case null -> delegate.readObject(name);
            default -> throw new IllegalStateException("Unexpected value: " + pending);
        };
    }

    Consumer<Runnable> commitTx(Collection<TxRecord.TxObjectRecord<?>> writes, long id) {
        var bundle = txWriteback.createBundle();
        try {
            for (var action : writes) {
                switch (action) {
                    case TxRecord.TxObjectRecordWrite<?> write -> {
                        Log.trace("Flushing object " + write.key());
                        bundle.commit(new JDataVersionedWrapper(write.data(), id));
                    }
                    case TxRecord.TxObjectRecordDeleted deleted -> {
                        Log.trace("Deleting object " + deleted.key());
                        bundle.delete(deleted.key());
                    }
                    default -> {
                        throw new TxCommitException("Unexpected value: " + action.key());
                    }
                }
            }
        } catch (Throwable t) {
            txWriteback.dropBundle(bundle);
            throw new TxCommitException(t.getMessage(), t);
        }

        Log.tracef("Committing transaction %d to storage", id);
        txWriteback.commitBundle(bundle);

        long bundleId = bundle.getId();

        return r -> txWriteback.asyncFence(bundleId, r);
    }

    // Returns an iterator with a view of all commited objects
    // Does not have to guarantee consistent view, snapshots are handled by upper layers
    public CloseableKvIterator<JObjectKey, JDataVersionedWrapper> getIterator(IteratorStart start, JObjectKey key) {
        return new MergingKvIterator<>(delegate.getIterator(start, key), txWriteback.getIterator(start, key));
    }

    public CloseableKvIterator<JObjectKey, JDataVersionedWrapper> getIterator(JObjectKey key) {
        return getIterator(IteratorStart.GE, key);
    }
}
