package com.usatiuk.kleppmanntree;

/**
 * Clock interface
 */
public interface Clock<TimestampT extends Comparable<TimestampT>> {
    /**
     * Increment and get the current timestamp.
     * @return the incremented timestamp
     */
    TimestampT getTimestamp();

    /**
     * Get the current timestamp without incrementing it.
     * @return the current timestamp
     */
    TimestampT peekTimestamp();

    /**
     * Update the timestamp with an externally received timestamp.
     * Will set the currently stored timestamp to <code>max(receivedTimestamp, currentTimestamp) + 1</code>
     * @param receivedTimestamp the received timestamp
     * @return the previous timestamp
     */
    TimestampT updateTimestamp(TimestampT receivedTimestamp);
}
