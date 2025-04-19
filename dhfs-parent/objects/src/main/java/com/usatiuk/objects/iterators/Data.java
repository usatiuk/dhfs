package com.usatiuk.objects.iterators;

import java.util.Optional;

public interface Data<V> extends MaybeTombstone<V> {
    V value();
}
