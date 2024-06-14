package com.usatiuk.dhfs.storage.files.objects;

import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

@Accessors(chain = true)
@Getter
@Setter
public class Directory extends DirEntry {
    public Directory(UUID uuid) {
        super(uuid);
    }

    Map<String, UUID> children = new TreeMap<>();
}
