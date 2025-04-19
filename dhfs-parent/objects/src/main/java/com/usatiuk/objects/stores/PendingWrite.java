package com.usatiuk.objects.stores;

import com.usatiuk.objects.JDataVersionedWrapper;
import com.usatiuk.objects.iterators.Data;

public record PendingWrite(JDataVersionedWrapper value, long bundleId) implements PendingWriteEntry, Data<JDataVersionedWrapper> {
}
