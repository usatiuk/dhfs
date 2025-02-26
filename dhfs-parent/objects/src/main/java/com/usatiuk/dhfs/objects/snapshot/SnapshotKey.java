package com.usatiuk.dhfs.objects.snapshot;

import com.usatiuk.dhfs.objects.JObjectKey;

import javax.annotation.Nonnull;
import java.util.Comparator;

record SnapshotKey(JObjectKey key, long version) implements Comparable<SnapshotKey> {
    @Override
    public int compareTo(@Nonnull SnapshotKey o) {
        return Comparator.comparing(SnapshotKey::key)
                .thenComparing(SnapshotKey::version)
                .compare(this, o);
    }
}
