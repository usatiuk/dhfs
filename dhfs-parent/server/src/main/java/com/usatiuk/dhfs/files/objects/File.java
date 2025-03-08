package com.usatiuk.dhfs.files.objects;

import com.usatiuk.dhfs.objects.JDataRemote;
import com.usatiuk.dhfs.objects.JObjectKey;
import com.usatiuk.dhfs.objects.jmap.JMapHolder;
import com.usatiuk.dhfs.objects.jmap.JMapLongKey;

import java.util.Collection;
import java.util.Set;

public record File(JObjectKey key, long mode, long cTime, long mTime,
                   boolean symlink, long size
) implements JDataRemote, JMapHolder<JMapLongKey> {
    public File withSymlink(boolean symlink) {
        return new File(key, mode, cTime, mTime, symlink, size);
    }

    public File withSize(long size) {
        return new File(key, mode, cTime, mTime, symlink, size);
    }

    public File withMode(long mode) {
        return new File(key, mode, cTime, mTime, symlink, size);
    }

    public File withCTime(long cTime) {
        return new File(key, mode, cTime, mTime, symlink, size);
    }

    public File withMTime(long mTime) {
        return new File(key, mode, cTime, mTime, symlink, size);
    }

    @Override
    public Collection<JObjectKey> collectRefsTo() {
        return Set.of();
//        return Set.copyOf(chunks().values());
    }

    @Override
    public int estimateSize() {
        return 64;
//        return chunks.size() * 64;
    }
}
