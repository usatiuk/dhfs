package com.usatiuk.dhfs.objects;

public record PendingWrite(JDataVersionedWrapper data, long bundleId) implements PendingWriteEntry {
}
