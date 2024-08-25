package com.usatiuk.kleppmanntree;

import java.util.HashMap;
import java.util.Map;

public class TestPeerLog implements PeerTimestampLogInterface<Long, Long> {
    private final Map<Long, Long> _peerTimestampLog = new HashMap<>();

    @Override
    public Long getForPeer(Long peerId) {
        return _peerTimestampLog.get(peerId);
    }

    @Override
    public void putForPeer(Long peerId, Long timestamp) {
        _peerTimestampLog.put(peerId, timestamp);
    }
}
