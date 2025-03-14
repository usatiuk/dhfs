package com.usatiuk.dhfs.objects.stores;

import com.usatiuk.dhfs.objects.JDataVersionedWrapper;

public record PendingWrite(JDataVersionedWrapper data, long bundleId) implements PendingWriteEntry {
}
