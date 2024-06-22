package com.usatiuk.dhfs.storage.files.objects;

import lombok.Getter;
import lombok.Setter;

import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.UUID;

public class File extends FsNode {
    public File(UUID uuid, long mode, UUID parent) {
        super(uuid, mode);
        _parent = parent;
    }

    @Getter
    private final NavigableMap<Long, String> _chunks = new TreeMap<>();

    @Getter
    private final UUID _parent;
}
