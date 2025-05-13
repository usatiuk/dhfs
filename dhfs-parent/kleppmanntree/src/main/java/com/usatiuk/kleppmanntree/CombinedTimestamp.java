package com.usatiuk.kleppmanntree;

import java.io.Serializable;
import java.util.Comparator;

/**
 * CombinedTimestamp is a record that represents a timestamp and a node ID, ordered first by timestamp and then by node ID.
 * @param timestamp the timestamp
 * @param nodeId the node ID. If null, then only the timestamp is used for ordering.
 * @param <TimestampT> the type of the timestamp
 * @param <PeerIdT> the type of the node ID
 */
public record CombinedTimestamp<TimestampT extends Comparable<TimestampT>, PeerIdT extends Comparable<PeerIdT>>
        (TimestampT timestamp,
         PeerIdT nodeId) implements Comparable<CombinedTimestamp<TimestampT, PeerIdT>>, Serializable {

    @Override
    public int compareTo(CombinedTimestamp<TimestampT, PeerIdT> o) {
        if (nodeId == null || o.nodeId == null) {
            return Comparator.comparing((CombinedTimestamp<TimestampT, PeerIdT> t) -> t.timestamp)
                    .compare(this, o);
        }
        return Comparator.comparing((CombinedTimestamp<TimestampT, PeerIdT> t) -> t.timestamp)
                .thenComparing((CombinedTimestamp<TimestampT, PeerIdT> t) -> t.nodeId)
                .compare(this, o);
    }
}
