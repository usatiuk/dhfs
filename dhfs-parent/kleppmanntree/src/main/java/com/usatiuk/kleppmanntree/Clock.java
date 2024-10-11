package com.usatiuk.kleppmanntree;

public interface Clock<TimestampT extends Comparable<TimestampT>> {
    TimestampT getTimestamp();

    TimestampT peekTimestamp();

    TimestampT updateTimestamp(TimestampT receivedTimestamp);
}
