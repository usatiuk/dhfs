package com.usatiuk.dhfs.files.objects;

import com.usatiuk.objects.common.runtime.JObjectKey;
import lombok.Builder;

import java.util.Collection;
import java.util.NavigableMap;

@Builder(toBuilder = true)
public record File(JObjectKey key, Collection<JObjectKey> refsFrom, boolean frozen, long mode, long cTime, long mTime,
                   NavigableMap<Long, JObjectKey> chunks, boolean symlink, long size
) implements FsNode {
    @Override
    public File withRefsFrom(Collection<JObjectKey> refs) {
        return this.toBuilder().refsFrom(refs).build();
    }

    @Override
    public File withFrozen(boolean frozen) {
        return this.toBuilder().frozen(frozen).build();
    }

    @Override
    public Collection<JObjectKey> collectRefsTo() {
        return chunks().values().stream().toList();
    }
}
