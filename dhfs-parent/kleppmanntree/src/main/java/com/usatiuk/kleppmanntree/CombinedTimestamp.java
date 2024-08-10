package com.usatiuk.kleppmanntree;

import java.io.Serializable;
import java.util.Comparator;

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
