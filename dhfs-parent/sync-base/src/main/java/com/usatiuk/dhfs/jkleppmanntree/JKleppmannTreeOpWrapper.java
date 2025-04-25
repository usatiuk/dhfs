package com.usatiuk.dhfs.jkleppmanntree;

import com.usatiuk.dhfs.invalidation.Op;
import com.usatiuk.dhfs.jkleppmanntree.structs.JKleppmannTreeNodeMeta;
import com.usatiuk.dhfs.peersync.PeerId;
import com.usatiuk.kleppmanntree.OpMove;
import com.usatiuk.objects.JObjectKey;

import java.io.Serializable;
import java.util.Collection;

// Wrapper to avoid having to specify generic types
public record JKleppmannTreeOpWrapper(JObjectKey treeName,
                                      OpMove<Long, PeerId, JKleppmannTreeNodeMeta, JObjectKey> op) implements Op, Serializable {
    @Override
    public Collection<JObjectKey> getEscapedRefs() {
        return op.newMeta().collectRefsTo();
    }
}
