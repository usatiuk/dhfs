package com.usatiuk.dhfs.objects.snapshot;

public interface SnapshotEntry {
    long whenToRemove();

    SnapshotEntry withWhenToRemove(long whenToRemove);
}
