package com.usatiuk.kleppmanntree;

import java.util.Collection;
import java.util.List;

public class TestPeerInterface implements PeerInterface<Long> {
    private final long selfId;

    public TestPeerInterface(long selfId) {this.selfId = selfId;}

    @Override
    public Long getSelfId() {
        return selfId;
    }

    @Override
    public Collection<Long> getAllPeers() {
        return List.of(1L, 2L);
    }
}
