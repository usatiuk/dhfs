package com.usatiuk.dhfs.invalidation;

import com.usatiuk.dhfs.peersync.PeerId;
import com.usatiuk.objects.JObjectKey;

import java.io.Serializable;

public record InvalidationQueueEntry(PeerId peer, JObjectKey key) implements Serializable {
}
