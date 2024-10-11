package com.usatiuk.kleppmanntree;

import java.io.Serializable;

public class AtomicClock implements Clock<Long>, Serializable {
    private long _max = 0;

    public AtomicClock(long counter) {
        _max = counter;
    }

    @Override
    public Long getTimestamp() {
        return ++_max;
    }

    // FIXME:
    public void ungetTimestamp() {
        --_max;
    }

    public void setTimestamp(Long timestamp) {
        _max = timestamp;
    }

    @Override
    public Long peekTimestamp() {
        return _max;
    }

    @Override
    public Long updateTimestamp(Long receivedTimestamp) {
        var old = _max;
        _max = Math.max(_max, receivedTimestamp) + 1;
        return old;
    }
}
