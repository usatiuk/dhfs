package com.usatiuk.kleppmanntree;

import java.util.Comparator;

public record CombinedTimestamp<TimestampT extends Comparable<TimestampT>, PeerIdT extends Comparable<PeerIdT>>
        (TimestampT timestamp, PeerIdT nodeId) implements Comparable<CombinedTimestamp<TimestampT, PeerIdT>> {


    @Override
    public int compareTo(CombinedTimestamp<TimestampT, PeerIdT> o) {
        return Comparator.comparing((CombinedTimestamp<TimestampT, PeerIdT> t) -> t.timestamp)
                         .thenComparing((CombinedTimestamp<TimestampT, PeerIdT> t) -> t.nodeId)
                         .compare(this, o);
    }
}
