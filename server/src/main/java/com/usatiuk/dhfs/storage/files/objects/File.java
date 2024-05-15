package com.usatiuk.dhfs.storage.files.objects;

import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

import java.util.Map;

@Accessors(chain = true)
@Getter
@Setter
public class File extends DirEntry {
    Map<Long, String> chunks;
}
