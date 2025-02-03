package com.usatiuk.dhfs.files.objects;

import com.usatiuk.autoprotomap.runtime.ProtoMirror;
import com.usatiuk.dhfs.objects.JObjectKey;
import com.usatiuk.dhfs.objects.persistence.ChunkDataP;
import org.pcollections.TreePMap;

import java.util.Collection;
import java.util.Set;

//@ProtoMirror(ChunkDataP.class)
public record File(JObjectKey key, long mode, long cTime, long mTime,
                   TreePMap<Long, JObjectKey> chunks, boolean symlink, long size
) implements FsNode {
    public File withChunks(TreePMap<Long, JObjectKey> chunks) {
        return new File(key, mode, cTime, mTime, chunks, symlink, size);
    }

    public File withSymlink(boolean symlink) {
        return new File(key, mode, cTime, mTime, chunks, symlink, size);
    }

    public File withSize(long size) {
        return new File(key, mode, cTime, mTime, chunks, symlink, size);
    }

    public File withMode(long mode) {
        return new File(key, mode, cTime, mTime, chunks, symlink, size);
    }

    public File withCTime(long cTime) {
        return new File(key, mode, cTime, mTime, chunks, symlink, size);
    }

    public File withMTime(long mTime) {
        return new File(key, mode, cTime, mTime, chunks, symlink, size);
    }

    @Override
    public Collection<JObjectKey> collectRefsTo() {
        return Set.copyOf(chunks().values());
    }

    @Override
    public int estimateSize() {
        return chunks.size() * 64;
    }
}
