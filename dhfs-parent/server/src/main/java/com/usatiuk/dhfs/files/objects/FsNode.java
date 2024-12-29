package com.usatiuk.dhfs.files.objects;

import com.usatiuk.dhfs.objects.JDataRefcounted;
import com.usatiuk.objects.common.runtime.JData;

import java.io.Serializable;

public interface FsNode extends JDataRefcounted, Serializable {
    long getMode();

    void setMode(long mode);

    long getCtime();

    void setCtime(long ctime);

    long getMtime();

    void setMtime(long mtime);
}
