package com.usatiuk.dhfs.jkleppmanntree;

import com.usatiuk.dhfs.objects.JObjectKey;
import com.usatiuk.dhfs.PeerId;
import com.usatiuk.dhfs.jkleppmanntree.structs.JKleppmannTreeNodeMeta;
import com.usatiuk.dhfs.jkleppmanntree.structs.JKleppmannTreeNodeMetaFile;
import com.usatiuk.dhfs.repository.invalidation.Op;
import com.usatiuk.kleppmanntree.OpMove;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;

// Wrapper to avoid having to specify generic types
public record JKleppmannTreeOpWrapper(JObjectKey treeName,
                                      OpMove<Long, PeerId, JKleppmannTreeNodeMeta, JObjectKey> op) implements Op, Serializable {
    @Override
    public Collection<JObjectKey> getEscapedRefs() {
        if (op.newMeta() instanceof JKleppmannTreeNodeMetaFile mf) {
            return List.of(mf.getFileIno());
        }
        return List.of();
    }
}
