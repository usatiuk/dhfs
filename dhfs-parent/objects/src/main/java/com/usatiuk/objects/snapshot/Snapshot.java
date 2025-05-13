package com.usatiuk.objects.snapshot;

import com.usatiuk.objects.iterators.CloseableKvIterator;
import com.usatiuk.objects.iterators.IteratorStart;
import com.usatiuk.objects.iterators.MaybeTombstone;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Optional;

/**
 * Interface for a snapshot of a database.
 * Represents a point-in-time view of a storage, with a unique ID.
 *
 * @param <K> the type of the key
 * @param <V> the type of the value
 */
public interface Snapshot<K extends Comparable<K>, V> extends AutoCloseable {
    /**
     * Get a list of iterators representing the snapshot.
     * The iterators have priority: the first one in the list is the highest.
     * The data type of the iterator is a tombstone: a tombstone represents a deleted value that does not exist anymore.
     * The list of iterators is intended to be consumed by {@link com.usatiuk.objects.iterators.TombstoneSkippingIterator}
     *
     * @return a list of iterators
     */
    List<CloseableKvIterator<K, MaybeTombstone<V>>> getIterator(IteratorStart start, K key);

    /**
     * Read an object from the snapshot.
     * @param name the name of the object
     * @return an optional containing the object if it exists, or an empty optional if it does not
     */
    @Nonnull
    Optional<V> readObject(K name);

    /**
     * Get the ID of the snapshot.
     * @return the ID of the snapshot
     */
    long id();

    @Override
    void close();

}
