package com.usatiuk.objects.iterators;

/**
 * Simple implementation of the Data interface.
 * @param value the value
 * @param <V> the type of the value
 */
public record DataWrapper<V>(V value) implements Data<V> {
}
