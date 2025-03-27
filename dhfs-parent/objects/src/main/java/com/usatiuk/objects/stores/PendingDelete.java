package com.usatiuk.objects.stores;

import com.usatiuk.objects.JObjectKey;

public record PendingDelete(JObjectKey key, long bundleId) implements PendingWriteEntry {
}
