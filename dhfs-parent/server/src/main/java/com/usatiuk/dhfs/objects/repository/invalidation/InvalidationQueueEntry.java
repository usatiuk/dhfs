package com.usatiuk.dhfs.objects.repository.invalidation;

import com.usatiuk.dhfs.objects.JObjectKey;
import com.usatiuk.dhfs.objects.PeerId;

import java.io.Serializable;

public record InvalidationQueueEntry(PeerId peer, JObjectKey key) implements Serializable {
}
