package com.usatiuk.dhfs.objects;

import java.util.Optional;

public interface MaybeTombstone<T> {
    Optional<T> opt();
}
