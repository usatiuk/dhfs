package com.usatiuk.dhfs.objects.jmap;

import javax.annotation.Nonnull;
import java.io.Serializable;

public record JMapLongKey(long key) implements JMapKey, Comparable<JMapLongKey>, Serializable {
    public static JMapLongKey of(long key) {
        return new JMapLongKey(key);
    }

    @Override
    public String toString() {
        return String.format("%016d", key);
    }

    public static JMapLongKey max() {
        return new JMapLongKey(Long.MAX_VALUE);
    }

    @Override
    public int compareTo(@Nonnull JMapLongKey o) {
        return Long.compare(key, o.key);
    }
}
