package com.usatiuk.dhfs.objects.jkleppmanntree;

import com.usatiuk.dhfs.objects.PeerId;

public class JKleppmannTreePeriodicPushOp {
    private final PeerId _from;
    private final long _timestamp;

    public JKleppmannTreePeriodicPushOp(PeerId from, long timestamp) {
        _from = from;
        _timestamp = timestamp;
    }

    public PeerId getFrom() {
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
