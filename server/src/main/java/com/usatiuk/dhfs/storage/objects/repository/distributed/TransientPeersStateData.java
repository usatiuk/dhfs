package com.usatiuk.dhfs.storage.objects.repository.distributed;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

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
    private final Map<UUID, TransientPeerState> _states = new LinkedHashMap<>();
}
