package com.usatiuk.dhfs.storage.objects.repository.distributed;

import jakarta.json.bind.annotation.JsonbCreator;
import jakarta.json.bind.annotation.JsonbProperty;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.UUID;

@Getter
public class HostInfo implements Serializable {
    private final UUID _uuid;

    @Setter
    private String _addr;
    @Setter
    private Integer _port;

    @JsonbCreator
    public HostInfo(@JsonbProperty("uuid") String uuid,
                    @JsonbProperty("addr") String addr,
                    @JsonbProperty("port") Integer port) {
        _uuid = UUID.fromString(uuid);
        _addr = addr;
        _port = port;
    }
}
