package com.usatiuk.dhfs.repository.invalidation;

import com.usatiuk.dhfs.objects.JObjectKey;
import com.usatiuk.dhfs.PeerId;

import java.io.Serializable;

public record InvalidationQueueEntry(PeerId peer, JObjectKey key) implements Serializable {
}
