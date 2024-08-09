package com.usatiuk.dhfs.objects.jkleppmanntree;

import com.usatiuk.dhfs.objects.jkleppmanntree.structs.JKleppmannTreeNodeMeta;
import com.usatiuk.dhfs.objects.repository.opsupport.Op;
import com.usatiuk.kleppmanntree.OpMove;
import lombok.Getter;

import java.util.UUID;

// Wrapper to avoid having to specify generic types
public class JKleppmannTreeOpWrapper implements Op {
    @Getter
    private final OpMove<Long, UUID, ? extends JKleppmannTreeNodeMeta, String> _op;

    public JKleppmannTreeOpWrapper(OpMove<Long, UUID, ? extends JKleppmannTreeNodeMeta, String> op) {
        if (op == null) throw new IllegalArgumentException("op shouldn't be null");
        _op = op;
    }
}
