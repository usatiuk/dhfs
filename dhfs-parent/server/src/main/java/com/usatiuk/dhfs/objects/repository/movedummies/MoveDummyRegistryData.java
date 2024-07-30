package com.usatiuk.dhfs.objects.repository.movedummies;

import lombok.Getter;
import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.collections4.multimap.HashSetValuedHashMap;

import java.io.Serial;
import java.io.Serializable;
import java.util.UUID;

public class MoveDummyRegistryData implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    @Getter
    private final MultiValuedMap<UUID, MoveDummyEntry> _moveDummiesPending = new HashSetValuedHashMap<>();
}
