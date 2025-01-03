package com.usatiuk.dhfs.objects.jkleppmanntree;

import java.util.UUID;

public class JKleppmannTreePeriodicPushOp {
    private final UUID _from;
    private final long _timestamp;

    public JKleppmannTreePeriodicPushOp(UUID from, long timestamp) {
        _from = from;
        _timestamp = timestamp;
    }

    public UUID getFrom() {
        return _from;
    }

    public long getTimestamp() {
        return _timestamp;
    }

//    @Override
//    public Collection<String> getEscapedRefs() {
//        return List.of();
//    }
}
