package com.usatiuk.dhfs.objects.snapshot;

public record SnapshotEntryDeleted(long whenToRemove) implements SnapshotEntry {
}
