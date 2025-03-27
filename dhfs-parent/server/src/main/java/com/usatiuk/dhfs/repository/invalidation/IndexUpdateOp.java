package com.usatiuk.dhfs.repository.invalidation;

import com.usatiuk.dhfs.objects.JObjectKey;
import com.usatiuk.dhfs.PeerId;
import com.usatiuk.dhfs.repository.JDataRemoteDto;
import org.pcollections.PMap;

import java.util.Collection;
import java.util.List;

public record IndexUpdateOp(JObjectKey key, PMap<PeerId, Long> changelog, JDataRemoteDto data) implements Op {
    @Override
    public Collection<JObjectKey> getEscapedRefs() {
        return List.of(key);
    }
}
