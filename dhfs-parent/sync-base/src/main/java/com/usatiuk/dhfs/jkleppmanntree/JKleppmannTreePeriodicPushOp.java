package com.usatiuk.dhfs.jkleppmanntree;

import com.usatiuk.dhfs.invalidation.Op;
import com.usatiuk.dhfs.peersync.PeerId;
import com.usatiuk.objects.JObjectKey;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;

public record JKleppmannTreePeriodicPushOp(JObjectKey treeName, PeerId from,
                                           long timestamp) implements Op, Serializable {
    @Override
    public Collection<JObjectKey> getEscapedRefs() {
        return List.of();
    }
}
