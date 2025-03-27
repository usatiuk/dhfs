package com.usatiuk.objects.snapshot;

public interface SnapshotEntry {
    long whenToRemove();

    SnapshotEntry withWhenToRemove(long whenToRemove);
}
