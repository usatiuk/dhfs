package com.usatiuk.dhfs.objects;

public record PendingDelete(JObjectKey key, long bundleId) implements PendingWriteEntry {
}
