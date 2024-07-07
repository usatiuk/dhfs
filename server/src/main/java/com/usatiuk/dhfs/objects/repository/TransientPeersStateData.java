package com.usatiuk.dhfs.objects.repository;

import lombok.Getter;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;


public class TransientPeersStateData {

    @Getter
    private final Map<UUID, TransientPeerState> _states = new LinkedHashMap<>();
}
