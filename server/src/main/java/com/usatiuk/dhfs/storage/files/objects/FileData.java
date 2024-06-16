package com.usatiuk.dhfs.storage.files.objects;

import lombok.Getter;

import java.io.Serializable;
import java.util.NavigableMap;
import java.util.TreeMap;

@Getter
public class FileData implements Serializable {
    private final NavigableMap<Long, String> _chunks = new TreeMap<>();
}
