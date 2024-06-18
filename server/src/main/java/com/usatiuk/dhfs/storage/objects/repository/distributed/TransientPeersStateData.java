package com.usatiuk.dhfs.storage.objects.repository.distributed;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.LinkedHashMap;
import java.util.Map;

public class TransientPeersStateData {

    @AllArgsConstructor
    @NoArgsConstructor
    public static class TransientPeerState {
        public enum ConnectionState {
            NOT_SEEN,
            REACHABLE,
            UNREACHABLE
        }

        @Getter
        @Setter
        private ConnectionState _state = ConnectionState.NOT_SEEN;
    }

    @Getter
    private final Map<String, TransientPeerState> _states = new LinkedHashMap<>();
}
