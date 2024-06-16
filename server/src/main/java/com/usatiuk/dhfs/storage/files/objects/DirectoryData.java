package com.usatiuk.dhfs.storage.files.objects;

import lombok.Getter;

import java.io.Serializable;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

@Getter
public class DirectoryData implements Serializable {
    private final Map<String, UUID> _children = new TreeMap<>();
}
