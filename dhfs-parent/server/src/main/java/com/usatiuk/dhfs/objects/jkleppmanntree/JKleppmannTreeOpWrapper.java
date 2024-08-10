package com.usatiuk.dhfs.objects.jkleppmanntree;

import com.usatiuk.dhfs.objects.jkleppmanntree.structs.JKleppmannTreeNodeMeta;
import com.usatiuk.dhfs.objects.jkleppmanntree.structs.JKleppmannTreeNodeMetaFile;
import com.usatiuk.dhfs.objects.repository.opsupport.Op;
import com.usatiuk.kleppmanntree.OpMove;
import lombok.Getter;

import java.util.Collection;
import java.util.List;
import java.util.UUID;

// Wrapper to avoid having to specify generic types
public class JKleppmannTreeOpWrapper implements Op {
    @Getter
    private final OpMove<Long, UUID, JKleppmannTreeNodeMeta, String> _op;

    public JKleppmannTreeOpWrapper(OpMove<Long, UUID, JKleppmannTreeNodeMeta, String> op) {
        if (op == null) throw new IllegalArgumentException("op shouldn't be null");
        _op = op;
    }

    @Override
    public Collection<String> getEscapedRefs() {
        if (_op.newMeta() instanceof JKleppmannTreeNodeMetaFile mf) {
            return List.of(mf.getFileIno());
        }
        return List.of();
    }
}
