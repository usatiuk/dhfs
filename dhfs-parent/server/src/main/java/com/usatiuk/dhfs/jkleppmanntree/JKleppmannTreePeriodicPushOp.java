package com.usatiuk.dhfs.jkleppmanntree;

import com.usatiuk.dhfs.objects.JObjectKey;
import com.usatiuk.dhfs.PeerId;
import com.usatiuk.dhfs.repository.invalidation.Op;

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
