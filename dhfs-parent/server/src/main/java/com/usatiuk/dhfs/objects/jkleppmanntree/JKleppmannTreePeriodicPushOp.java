package com.usatiuk.dhfs.objects.jkleppmanntree;

import lombok.Getter;

import java.util.Collection;
import java.util.List;
import java.util.UUID;

public class JKleppmannTreePeriodicPushOp  {
    @Getter
    private final UUID _from;
    @Getter
    private final long _timestamp;

    public JKleppmannTreePeriodicPushOp(UUID from, long timestamp) {
        _from = from;
        _timestamp = timestamp;
    }

//    @Override
//    public Collection<String> getEscapedRefs() {
//        return List.of();
//    }
}
