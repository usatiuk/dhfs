package com.usatiuk.dhfs.files.objects;

import com.usatiuk.objects.common.runtime.JObjectKey;

import java.util.Collection;
import java.util.NavigableMap;

public interface File extends FsNode {
    NavigableMap<Long, JObjectKey> getChunks();

    void setChunks(NavigableMap<Long, JObjectKey> chunks);

    boolean getSymlink();

    void setSymlink(boolean symlink);

    long getSize();

    void setSize(long size);

    @Override
    default Collection<JObjectKey> collectRefsTo() {
        return getChunks().values().stream().toList();
    }
}
