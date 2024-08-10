package com.usatiuk.dhfs.objects.jkleppmanntree;

import com.usatiuk.dhfs.objects.jkleppmanntree.structs.JKleppmannTreeNodeMeta;
import com.usatiuk.dhfs.objects.repository.opsupport.Op;
import com.usatiuk.kleppmanntree.OpMove;
import lombok.Getter;

import java.util.Collection;
import java.util.List;
import java.util.UUID;

public class JKleppmannTreePeriodicPushOp implements Op {
    @Getter
    private final UUID _from;
    @Getter
    private final long _timestamp;

    public JKleppmannTreePeriodicPushOp(UUID from, long timestamp) {
        _from = from;
        _timestamp = timestamp;
    }

    @Override
    public Collection<String> getEscapedRefs() {
        return List.of();
    }
}
