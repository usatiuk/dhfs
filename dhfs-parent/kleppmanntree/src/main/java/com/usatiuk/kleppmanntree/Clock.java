package com.usatiuk.kleppmanntree;

public interface Clock<TimestampT extends Comparable<TimestampT>> {
    TimestampT getTimestamp();

    TimestampT peekTimestamp();

    void updateTimestamp(TimestampT receivedTimestamp);
}
