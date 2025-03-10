package com.usatiuk.dhfs.objects.snapshot;

import com.usatiuk.dhfs.objects.JDataVersionedWrapper;

public record SnapshotEntryObject(JDataVersionedWrapper data, long whenToRemove) implements SnapshotEntry {
    @Override
    public SnapshotEntryObject withWhenToRemove(long whenToRemove) {
        return new SnapshotEntryObject(data, whenToRemove);
    }
}
