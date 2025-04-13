package com.usatiuk.objects;

import java.io.Serializable;

public interface JData extends Serializable {
    JObjectKey key();

    default int estimateSize() {
        return 100;
    }
}
