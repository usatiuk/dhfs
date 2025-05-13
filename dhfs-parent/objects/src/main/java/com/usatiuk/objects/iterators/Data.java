package com.usatiuk.objects.iterators;

/**
 * Interface indicating that data is present.
 * @param <V> the type of the value
 */
public interface Data<V> extends MaybeTombstone<V> {
    /**
     * Get the value.
     * @return the value
     */
    V value();
}
