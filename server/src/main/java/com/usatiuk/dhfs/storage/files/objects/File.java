package com.usatiuk.dhfs.storage.files.objects;

import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.UUID;

@Accessors(chain = true)
@Getter
@Setter
public class File extends DirEntry {
    public File(UUID uuid) {
        super(uuid);
    }

    NavigableMap<Long, String> chunks = new TreeMap<>();
}
