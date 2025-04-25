package com.usatiuk.dhfs.invalidation;

import com.usatiuk.dhfs.peersync.PeerId;
import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.collections4.multimap.HashSetValuedHashMap;

import java.io.Serial;
import java.io.Serializable;

public class DeferredInvalidationQueueData implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    public final MultiValuedMap<PeerId, InvalidationQueueEntry> deferredInvalidations = new HashSetValuedHashMap<>();
}
