package com.usatiuk.dhfs.storage.objects.repository.distributed;

import com.usatiuk.dhfs.objects.repository.distributed.peersync.PeerInfo;
import jakarta.json.bind.annotation.JsonbCreator;
import jakarta.json.bind.annotation.JsonbProperty;
import lombok.Getter;

import java.io.Serializable;
import java.util.UUID;

@Getter
public class HostInfo implements Serializable {
    private final UUID _uuid;

    @JsonbCreator
    public HostInfo(@JsonbProperty("uuid") String uuid) {
        _uuid = UUID.fromString(uuid);
    }

    public PeerInfo toPeerInfo() {
        return PeerInfo.newBuilder().setUuid(_uuid.toString()).build();
    }
}
