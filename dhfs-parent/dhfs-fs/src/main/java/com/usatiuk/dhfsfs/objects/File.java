package com.usatiuk.dhfsfs.objects;

import com.usatiuk.dhfs.jmap.JMapHolder;
import com.usatiuk.dhfs.jmap.JMapLongKey;
import com.usatiuk.dhfs.remoteobj.JDataRemote;
import com.usatiuk.dhfs.remoteobj.JDataRemoteDto;
import com.usatiuk.objects.JObjectKey;

import java.util.Collection;
import java.util.Set;

/**
 * File is a data structure that represents a file in the file system
 *
 * @param key     unique key
 * @param mode    file mode
 * @param cTime   inode modification time
 * @param mTime   modification time
 * @param symlink true if the file is a symlink, false otherwise
 */
public record File(JObjectKey key, long mode, long cTime, long mTime,
                   boolean symlink
) implements JDataRemote, JMapHolder<JMapLongKey> {
    public File withSymlink(boolean symlink) {
        return new File(key, mode, cTime, mTime, symlink);
    }

    public File withMode(long mode) {
        return new File(key, mode, cTime, mTime, symlink);
    }

    public File withCTime(long cTime) {
        return new File(key, mode, cTime, mTime, symlink);
    }

    public File withMTime(long mTime) {
        return new File(key, mode, cTime, mTime, symlink);
    }

    public File withCurrentMTime() {
        return new File(key, mode, cTime, System.currentTimeMillis(), symlink);
    }

    public File withCurrentCTime() {
        return new File(key, mode, System.currentTimeMillis(), mTime, symlink);
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

    @Override
    public Class<? extends JDataRemoteDto> dtoClass() {
        return FileDto.class;
    }
}
