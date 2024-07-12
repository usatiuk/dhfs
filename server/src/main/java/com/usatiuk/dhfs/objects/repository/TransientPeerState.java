package com.usatiuk.dhfs.objects.repository;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@AllArgsConstructor
@NoArgsConstructor
public class TransientPeerState {
    @Getter
    @Setter
    private boolean _reachable = false;
    @Getter
    @Setter
    private String _addr;
    @Getter
    @Setter
    private int _port;
    @Getter
    @Setter
    private int _securePort;

    public TransientPeerState(boolean reachable) {
        _reachable = reachable;
    }

    public TransientPeerState(TransientPeerState source) {
        _reachable = source._reachable;
        _addr = source._addr;
        _port = source._port;
        _securePort = source._securePort;
    }
}
