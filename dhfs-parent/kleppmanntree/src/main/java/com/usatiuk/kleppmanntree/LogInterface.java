package com.usatiuk.kleppmanntree;

import org.apache.commons.lang3.tuple.Pair;

import java.util.List;

public interface LogInterface<
        TimestampT extends Comparable<TimestampT>,
        PeerIdT extends Comparable<PeerIdT>,
        MetaT extends NodeMeta,
        NodeIdT> {
    Pair<CombinedTimestamp<TimestampT, PeerIdT>, LogRecord<TimestampT, PeerIdT, MetaT, NodeIdT>> peekOldest();

    Pair<CombinedTimestamp<TimestampT, PeerIdT>, LogRecord<TimestampT, PeerIdT, MetaT, NodeIdT>> takeOldest();

    Pair<CombinedTimestamp<TimestampT, PeerIdT>, LogRecord<TimestampT, PeerIdT, MetaT, NodeIdT>> peekNewest();

    List<Pair<CombinedTimestamp<TimestampT, PeerIdT>, LogRecord<TimestampT, PeerIdT, MetaT, NodeIdT>>>
    newestSlice(CombinedTimestamp<TimestampT, PeerIdT> since, boolean inclusive);

    List<Pair<CombinedTimestamp<TimestampT, PeerIdT>, LogRecord<TimestampT, PeerIdT, MetaT, NodeIdT>>> getAll();

    boolean isEmpty();

    boolean containsKey(CombinedTimestamp<TimestampT, PeerIdT> timestamp);

    long size();

    void put(CombinedTimestamp<TimestampT, PeerIdT> timestamp, LogRecord<TimestampT, PeerIdT, MetaT, NodeIdT> record);

    void replace(CombinedTimestamp<TimestampT, PeerIdT> timestamp, LogRecord<TimestampT, PeerIdT, MetaT, NodeIdT> record);
}
