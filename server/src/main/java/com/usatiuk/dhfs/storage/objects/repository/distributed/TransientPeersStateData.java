package com.usatiuk.dhfs.storage.objects.repository.distributed;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;


public class TransientPeersStateData {

    @Getter
    private final Map<UUID, TransientPeerState> _states = new LinkedHashMap<>();
}
