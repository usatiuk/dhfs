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
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;

@ApplicationScoped
public class WritebackObjectPersistentStore {
    @Inject
    CachingObjectPersistentStore delegate;
    @Inject
    TxWriteback txWriteback;
    private final AtomicLong _commitCounter = new AtomicLong(0);
    private final ReentrantReadWriteLock _lock = new ReentrantReadWriteLock();

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

    public interface VerboseReadResult {
    }

    public record VerboseReadResultPersisted(Optional<JDataVersionedWrapper> data) implements VerboseReadResult {
    }

    public record VerboseReadResultPending(TxWriteback.PendingWriteEntry pending) implements VerboseReadResult {
    }

    @Nonnull
    VerboseReadResult readObjectVerbose(JObjectKey key) {
        var pending = txWriteback.getPendingWrite(key).orElse(null);
        if (pending != null) {
            return new VerboseReadResultPending(pending);
        }
        return new VerboseReadResultPersisted(delegate.readObject(key));
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
        _commitCounter.incrementAndGet();

        long bundleId = bundle.getId();

        return r -> txWriteback.asyncFence(bundleId, r);
    }

    // Returns an iterator with a view of all commited objects
    // Does not have to guarantee consistent view, snapshots are handled by upper layers
    // Should be refreshed after each commit
    public CloseableKvIterator<JObjectKey, TombstoneMergingKvIterator.DataType<JDataVersionedWrapper>> getIterator(IteratorStart start, JObjectKey key) {
        _lock.readLock().lock();
        try {
            return new InvalidatableKvIterator<>(new MergingKvIterator<>(txWriteback.getIterator(start, key),
                    new MappingKvIterator<>(delegate.getIterator(start, key), TombstoneMergingKvIterator.Data::new)),
                    _commitCounter::get, _lock.readLock());
        } finally {
            _lock.readLock().unlock();
        }
    }

    public CloseableKvIterator<JObjectKey, TombstoneMergingKvIterator.DataType<JDataVersionedWrapper>> getIterator(JObjectKey key) {
        return getIterator(IteratorStart.GE, key);
    }
}
