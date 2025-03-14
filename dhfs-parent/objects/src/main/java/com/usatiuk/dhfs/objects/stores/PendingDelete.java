package com.usatiuk.dhfs.objects.stores;

import com.usatiuk.dhfs.objects.JObjectKey;

public record PendingDelete(JObjectKey key, long bundleId) implements PendingWriteEntry {
}
