package com.usatiuk.objects.stores;

import com.usatiuk.objects.JDataVersionedWrapper;
import com.usatiuk.objects.iterators.MaybeTombstone;

public interface PendingWriteEntry extends MaybeTombstone<JDataVersionedWrapper> {
    long bundleId();
}
