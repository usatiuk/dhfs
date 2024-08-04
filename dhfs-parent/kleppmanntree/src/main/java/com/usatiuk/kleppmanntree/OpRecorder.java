package com.usatiuk.kleppmanntree;

public interface OpRecorder<TimestampT extends Comparable<TimestampT>, PeerIdT extends Comparable<PeerIdT>, MetaT extends NodeMeta, NodeIdT> {
    void recordOp(OpMove<TimestampT, PeerIdT, ? extends MetaT, NodeIdT> op);
}
