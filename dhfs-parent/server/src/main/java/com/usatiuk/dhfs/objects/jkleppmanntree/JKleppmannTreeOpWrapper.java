package com.usatiuk.dhfs.objects.jkleppmanntree;

import com.usatiuk.dhfs.objects.jkleppmanntree.structs.JKleppmannTreeNodeMeta;
import com.usatiuk.kleppmanntree.OpMove;
import com.usatiuk.objects.common.runtime.JObjectKey;
import lombok.Getter;

import java.util.UUID;

// Wrapper to avoid having to specify generic types
public class JKleppmannTreeOpWrapper {
    @Getter
    private final OpMove<Long, UUID, JKleppmannTreeNodeMeta, JObjectKey> _op;

    public JKleppmannTreeOpWrapper(OpMove<Long, UUID, JKleppmannTreeNodeMeta, JObjectKey> op) {
        if (op == null) throw new IllegalArgumentException("op shouldn't be null");
        _op = op;
    }

//    @Override
//    public Collection<JObjectKey> getEscapedRefs() {
//        if (_op.newMeta() instanceof JKleppmannTreeNodeMetaFile mf) {
//            return List.of(mf.getFileIno());
//        }
//        return List.of();
//    }
}
