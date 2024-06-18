package com.usatiuk.dhfs.storage.objects.repository.distributed;

import jakarta.json.bind.annotation.JsonbCreator;
import jakarta.json.bind.annotation.JsonbProperty;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

@Getter
public class HostInfo implements Serializable {
    private final String _name;

    @Setter
    private String _addr;
    @Setter
    private Integer _port;

    @JsonbCreator
    public HostInfo(@JsonbProperty("_name") String name,
                    @JsonbProperty("_addr") String addr,
                    @JsonbProperty("_port") Integer port) {
        _name = name;
        _addr = addr;
        _port = port;
    }
}
