package com.usatiuk.dhfs.files.objects;

import com.usatiuk.dhfs.objects.JDataRemote;

public interface FsNode extends JDataRemote {
    long mode();

    long cTime();

    long mTime();
}
