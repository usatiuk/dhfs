package com.usatiuk.objects.stores;

import com.usatiuk.objects.JDataVersionedWrapper;

public record PendingWrite(JDataVersionedWrapper data, long bundleId) implements PendingWriteEntry {
}
