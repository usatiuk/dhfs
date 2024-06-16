package com.usatiuk.dhfs.storage.files.objects;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

@Getter
@Setter
public  class FsNodeData implements Serializable {
    private long _mode;
    private long _ctime;
    private long _mtime;
}
