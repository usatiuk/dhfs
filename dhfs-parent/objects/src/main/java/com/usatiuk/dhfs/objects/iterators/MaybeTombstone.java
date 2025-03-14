package com.usatiuk.dhfs.objects.iterators;

import java.util.Optional;

public interface MaybeTombstone<T> {
    Optional<T> opt();
}
