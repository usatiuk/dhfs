package com.usatiuk.dhfs.storage.files.objects;

import lombok.Getter;

import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.UUID;

public class File extends FsNode {
    public File(UUID uuid) {
        super(uuid);
    }

    public File(UUID uuid, long mode) {
        super(uuid, mode);
    }

    @Getter
    private final NavigableMap<Long, String> _chunks = new TreeMap<>();
}
