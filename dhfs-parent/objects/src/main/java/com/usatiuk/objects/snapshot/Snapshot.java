package com.usatiuk.objects.snapshot;

import com.usatiuk.objects.iterators.CloseableKvIterator;
import com.usatiuk.objects.iterators.IteratorStart;
import com.usatiuk.objects.iterators.MaybeTombstone;
import com.usatiuk.objects.iterators.Tombstone;
import com.usatiuk.utils.AutoCloseableNoThrow;

import javax.annotation.Nonnull;
import java.util.Optional;
import java.util.stream.Stream;

public interface Snapshot<K extends Comparable<K>, V> extends AutoCloseableNoThrow {
    Stream<CloseableKvIterator<K, MaybeTombstone<V>>> getIterator(IteratorStart start, K key);

    @Nonnull
    Optional<V> readObject(K name);

    long id();
}
