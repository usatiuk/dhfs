package com.usatiuk.dhfs.storage.objects.repository.distributed;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@AllArgsConstructor
@NoArgsConstructor
public class TransientPeerState {
    public TransientPeerState(ConnectionState connectionState) {
        _state = connectionState;
    }

    public enum ConnectionState {
        NOT_SEEN,
        REACHABLE,
        UNREACHABLE
    }

    @Getter
    @Setter
    private ConnectionState _state = ConnectionState.NOT_SEEN;

    @Getter
    @Setter
    private String _addr;

    @Getter
    @Setter
    private int _port;

    @Getter
    @Setter
    private int _securePort;
}
