package com.usatiuk.dhfs.objects.repository.invalidation;

import com.usatiuk.dhfs.objects.JObjectKey;
import com.usatiuk.dhfs.objects.PeerId;

public record InvalidationQueueEntry(PeerId peer, JObjectKey key, boolean forced) {
}
