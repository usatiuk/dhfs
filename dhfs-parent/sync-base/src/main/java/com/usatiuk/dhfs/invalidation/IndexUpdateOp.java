package com.usatiuk.dhfs.invalidation;

import com.usatiuk.objects.JObjectKey;
import com.usatiuk.dhfs.peersync.PeerId;
import com.usatiuk.dhfs.remoteobj.JDataRemoteDto;
import org.pcollections.PMap;

import java.util.Collection;
import java.util.List;

public record IndexUpdateOp(JObjectKey key, PMap<PeerId, Long> changelog, JDataRemoteDto data) implements Op {
    @Override
    public Collection<JObjectKey> getEscapedRefs() {
        return List.of(key);
    }
}
