package com.usatiuk.kleppmanntree;

import org.apache.commons.lang3.tuple.Pair;

import java.util.List;

/**
 * LogInterface is an interface that allows accessing the log of operations
 * @param <TimestampT> the type of the timestamp
 * @param <PeerIdT> the type of the peer ID
 * @param <MetaT> the type of the node metadata
 * @param <NodeIdT> the type of the node ID
 */
public interface LogInterface<
        TimestampT extends Comparable<TimestampT>,
        PeerIdT extends Comparable<PeerIdT>,
        MetaT extends NodeMeta,
        NodeIdT> {
    /**
     * Peek the oldest log entry.
     * @return the oldest log entry
     */
    Pair<CombinedTimestamp<TimestampT, PeerIdT>, LogRecord<TimestampT, PeerIdT, MetaT, NodeIdT>> peekOldest();

    /**
     * Take the oldest log entry.
     * @return the oldest log entry
     */
    Pair<CombinedTimestamp<TimestampT, PeerIdT>, LogRecord<TimestampT, PeerIdT, MetaT, NodeIdT>> takeOldest();

    /**
     * Peek the newest log entry.
     * @return the newest log entry
     */
    Pair<CombinedTimestamp<TimestampT, PeerIdT>, LogRecord<TimestampT, PeerIdT, MetaT, NodeIdT>> peekNewest();

    /**
     * Return all log entries that are newer than the given timestamp.
     * @param since the timestamp to compare with
     * @param inclusive if true, include the log entry with the given timestamp
     * @return a list of log entries that are newer than the given timestamp
     */
    List<Pair<CombinedTimestamp<TimestampT, PeerIdT>, LogRecord<TimestampT, PeerIdT, MetaT, NodeIdT>>>
    newestSlice(CombinedTimestamp<TimestampT, PeerIdT> since, boolean inclusive);

    /**
     * Return all the log entries
     * @return a list of all log entries
     */
    List<Pair<CombinedTimestamp<TimestampT, PeerIdT>, LogRecord<TimestampT, PeerIdT, MetaT, NodeIdT>>> getAll();

    /**
     * Checks if the log is empty.
     * @return true if the log is empty, false otherwise
     */
    boolean isEmpty();

    /**
     * Checks if the log contains the given timestamp.
     * @param timestamp the timestamp to check
     * @return true if the log contains the given timestamp, false otherwise
     */
    boolean containsKey(CombinedTimestamp<TimestampT, PeerIdT> timestamp);

    /**
     * Get the size of the log.
     * @return the size of the log (number of entries)
     */
    long size();

    /**
     * Add a log entry to the log.
     * @param timestamp the timestamp of the log entry
     * @param record the log entry
     * @throws IllegalStateException if the log entry already exists
     */
    void put(CombinedTimestamp<TimestampT, PeerIdT> timestamp, LogRecord<TimestampT, PeerIdT, MetaT, NodeIdT> record);

    /**
     * Replace a log entry in the log.
     * @param timestamp the timestamp of the log entry
     * @param record the log entry
     */
    void replace(CombinedTimestamp<TimestampT, PeerIdT> timestamp, LogRecord<TimestampT, PeerIdT, MetaT, NodeIdT> record);
}
