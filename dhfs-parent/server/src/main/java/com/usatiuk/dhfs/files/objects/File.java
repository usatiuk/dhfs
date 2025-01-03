package com.usatiuk.dhfs.files.objects;

import com.usatiuk.dhfs.objects.JObjectKey;
import org.pcollections.PCollection;
import org.pcollections.TreePMap;

import java.util.Collection;

public record File(JObjectKey key, PCollection<JObjectKey> refsFrom, boolean frozen,
                   long mode, long cTime, long mTime,
                   TreePMap<Long, JObjectKey> chunks, boolean symlink, long size
) implements FsNode {
    @Override
    public File withRefsFrom(PCollection<JObjectKey> refs) {
        return new File(key, refs, frozen, mode, cTime, mTime, chunks, symlink, size);
    }

    @Override
    public File withFrozen(boolean frozen) {
        return new File(key, refsFrom, frozen, mode, cTime, mTime, chunks, symlink, size);
    }

    public File withChunks(TreePMap<Long, JObjectKey> chunks) {
        return new File(key, refsFrom, frozen, mode, cTime, mTime, chunks, symlink, size);
    }

    public File withSymlink(boolean symlink) {
        return new File(key, refsFrom, frozen, mode, cTime, mTime, chunks, symlink, size);
    }

    public File withSize(long size) {
        return new File(key, refsFrom, frozen, mode, cTime, mTime, chunks, symlink, size);
    }

    public File withMode(long mode) {
        return new File(key, refsFrom, frozen, mode, cTime, mTime, chunks, symlink, size);
    }

    public File withCTime(long cTime) {
        return new File(key, refsFrom, frozen, mode, cTime, mTime, chunks, symlink, size);
    }

    public File withMTime(long mTime) {
        return new File(key, refsFrom, frozen, mode, cTime, mTime, chunks, symlink, size);
    }

    @Override
    public Collection<JObjectKey> collectRefsTo() {
        return chunks().values().stream().toList();
    }
}
