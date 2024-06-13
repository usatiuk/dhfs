package com.usatiuk.dhfs.storage.files.objects;

import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

import java.util.NavigableMap;
import java.util.TreeMap;

@Accessors(chain = true)
@Getter
@Setter
public class File extends DirEntry {
    NavigableMap<Long, String> chunks = new TreeMap<>();
}
