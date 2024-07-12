package com.usatiuk.dhfs.objects.repository;

import lombok.Getter;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;


public class TransientPeersStateData {

    @Getter
    private final Map<UUID, TransientPeerState> _states = new LinkedHashMap<>();

    TransientPeerState get(UUID host) {
        return _states.computeIfAbsent(host, k -> new TransientPeerState());
    }

    TransientPeerState getCopy(UUID host) {
        return new TransientPeerState(get(host));
    }
}
