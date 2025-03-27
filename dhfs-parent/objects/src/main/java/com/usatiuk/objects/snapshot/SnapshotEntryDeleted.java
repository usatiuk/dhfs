package com.usatiuk.objects.snapshot;

public record SnapshotEntryDeleted(long whenToRemove) implements SnapshotEntry {
    @Override
    public SnapshotEntryDeleted withWhenToRemove(long whenToRemove) {
        return new SnapshotEntryDeleted(whenToRemove);
    }
}
