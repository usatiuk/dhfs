package com.usatiuk.objects.iterators;

public interface Data<V> extends MaybeTombstone<V> {
    V value();
}
