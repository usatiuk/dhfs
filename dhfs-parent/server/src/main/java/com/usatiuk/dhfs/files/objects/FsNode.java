package com.usatiuk.dhfs.files.objects;

import com.usatiuk.dhfs.objects.JDataRefcounted;

public interface FsNode extends JDataRefcounted {
    long mode();

    long cTime();

    long mTime();
}
