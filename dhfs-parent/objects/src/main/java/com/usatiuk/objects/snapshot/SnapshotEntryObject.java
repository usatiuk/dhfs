package com.usatiuk.objects.snapshot;

import com.usatiuk.objects.JDataVersionedWrapper;

public record SnapshotEntryObject(JDataVersionedWrapper data, long whenToRemove) implements SnapshotEntry {
    @Override
    public SnapshotEntryObject withWhenToRemove(long whenToRemove) {
        return new SnapshotEntryObject(data, whenToRemove);
    }
}
