package com.usatiuk.dhfs.objects.snapshot;

import com.usatiuk.dhfs.objects.JDataVersionedWrapper;

record SnapshotEntryObject(JDataVersionedWrapper data, long whenToRemove) implements SnapshotEntry {
}
