package com.usatiuk.dhfs.objects.jkleppmanntree.helpers;

import com.usatiuk.dhfs.objects.jkleppmanntree.structs.JTreeNodeMeta;
import com.usatiuk.dhfs.objects.repository.invalidation.Op;
import com.usatiuk.kleppmanntree.OpMove;
import lombok.Getter;

import java.util.UUID;

// Wrapper to avoid having to specify generic types
public class JOpWrapper implements Op {
    @Getter
    private final OpMove<Long, UUID, ? extends JTreeNodeMeta, String> _op;

    public JOpWrapper(OpMove<Long, UUID, ? extends JTreeNodeMeta, String> op) {
        if (op == null) throw new IllegalArgumentException("op shouldn't be null");
        _op = op;
    }
}
