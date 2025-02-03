package com.usatiuk.dhfs.objects.jkleppmanntree;

import com.usatiuk.dhfs.objects.JObjectKey;
import com.usatiuk.dhfs.objects.PeerId;
import com.usatiuk.dhfs.objects.jkleppmanntree.structs.JKleppmannTreeNodeMeta;
import com.usatiuk.dhfs.objects.repository.invalidation.Op;
import com.usatiuk.kleppmanntree.OpMove;

import java.io.Serializable;

// Wrapper to avoid having to specify generic types
public record JKleppmannTreeOpWrapper(JObjectKey treeName,
                                      OpMove<Long, PeerId, JKleppmannTreeNodeMeta, JObjectKey> op) implements Op, Serializable {
//    @Override
//    public Collection<JObjectKey> getEscapedRefs() {
//        if (_op.newMeta() instanceof JKleppmannTreeNodeMetaFile mf) {
//            return List.of(mf.getFileIno());
//        }
//        return List.of();
//    }
}
