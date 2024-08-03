package com.usatiuk.kleppmanntree;

public interface Clock<TimestampT extends Comparable<TimestampT>> {
    TimestampT getTimestamp();

    void updateTimestamp(TimestampT receivedTimestamp);
}
