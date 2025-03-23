package com.usatiuk.dhfs.objects.jmap;

import javax.annotation.Nonnull;
import java.io.Serializable;

public record JMapLongKey(long key) implements JMapKey, Comparable<JMapKey>, Serializable {
    public static JMapLongKey of(long key) {
        return new JMapLongKey(key);
    }

    public static JMapLongKey max() {
        return new JMapLongKey(Long.MAX_VALUE);
    }

    @Override
    public String toString() {
        return String.format("%016d", key);
    }

    @Override
    public int compareTo(@Nonnull JMapKey o) {
        if (!(o instanceof JMapLongKey lk)) {
            throw new IllegalArgumentException("Unknown type of JMapKey");
        }
        return Long.compare(key, lk.key);
    }
}
