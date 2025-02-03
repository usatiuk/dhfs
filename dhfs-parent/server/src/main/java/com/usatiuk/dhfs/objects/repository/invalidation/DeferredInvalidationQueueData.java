package com.usatiuk.dhfs.objects.repository.invalidation;

import com.usatiuk.dhfs.objects.JObjectKey;
import com.usatiuk.dhfs.objects.PeerId;
import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.collections4.multimap.HashSetValuedHashMap;

import java.io.Serial;
import java.io.Serializable;
import java.util.UUID;

public class DeferredInvalidationQueueData implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    public final MultiValuedMap<PeerId, JObjectKey> deferredInvalidations = new HashSetValuedHashMap<>();
}
