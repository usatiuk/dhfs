package com.usatiuk.kleppmanntree;

public interface PeerTimestampLogInterface<
        TimestampT extends Comparable<TimestampT>,
        PeerIdT extends Comparable<PeerIdT>> {

    TimestampT getForPeer(PeerIdT peerId);

    void putForPeer(PeerIdT peerId, TimestampT timestamp);

}
