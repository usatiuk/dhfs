package com.usatiuk.objects.stores;

import com.usatiuk.objects.JDataVersionedWrapper;
import com.usatiuk.objects.JObjectKey;
import com.usatiuk.objects.iterators.Tombstone;

public record PendingDelete(JObjectKey key,
                            long bundleId) implements PendingWriteEntry, Tombstone<JDataVersionedWrapper> {
}
