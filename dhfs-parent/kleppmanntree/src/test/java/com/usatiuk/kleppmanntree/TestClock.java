package com.usatiuk.kleppmanntree;

public class TestClock implements Clock<Long> {
    private long max = 0;

    @Override
    public Long getTimestamp() {
        return max++;
    }

    @Override
    public Long peekTimestamp() {
        return max;
    }

    @Override
    public Long updateTimestamp(Long receivedTimestamp) {
        var old = max;
        max = Math.max(max, receivedTimestamp) + 1;
        return old;
    }
}
