package org.acme.files.objects;

import com.usatiuk.objects.common.runtime.JData;
import com.usatiuk.objects.common.runtime.JObjectKey;

import java.util.NavigableMap;

public interface File extends JData {
    NavigableMap<Long, JObjectKey> getChunks();

    void setChunks(NavigableMap<Long, JObjectKey> chunk);

    long getSize();

    void setSize(long size);
}
