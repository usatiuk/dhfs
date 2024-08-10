package com.usatiuk.kleppmanntree;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;

public class AtomicClock implements Clock<Long>, Serializable {
    private final AtomicLong _max = new AtomicLong(0L);

    @Override
    public Long getTimestamp() {
        return _max.incrementAndGet();
    }

    @Override
    public Long peekTimestamp() {
        return _max.get();
    }

    @Override
    public void updateTimestamp(Long receivedTimestamp) {
        long exp = _max.get();
        long set = Math.max(exp, receivedTimestamp) + 1;
        // TODO: What is correct memory ordering?
        while (!_max.weakCompareAndSetVolatile(exp, set)) {
            exp = _max.get();
            set = Math.max(exp, set) + 1;
        }
    }
}
