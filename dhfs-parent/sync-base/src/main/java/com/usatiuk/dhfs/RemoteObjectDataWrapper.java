package com.usatiuk.dhfs;

import com.usatiuk.objects.JObjectKey;
import org.pcollections.HashTreePSet;
import org.pcollections.PCollection;

import java.util.Collection;

public record RemoteObjectDataWrapper<T extends JDataRemote>(
        JObjectKey key,
        PCollection<JDataRef> refsFrom,
        boolean frozen,
        T data) implements JDataRefcounted {
    public RemoteObjectDataWrapper(T data) {
        this(RemoteObjectMeta.ofDataKey(data.key()), HashTreePSet.empty(), false, data);
    }

    @Override
    public RemoteObjectDataWrapper<T> withRefsFrom(PCollection<JDataRef> refs) {
        return new RemoteObjectDataWrapper<>(key, refs, frozen, data);
    }

    @Override
    public RemoteObjectDataWrapper<T> withFrozen(boolean frozen) {
        return new RemoteObjectDataWrapper<>(key, refsFrom, frozen, data);
    }

    public RemoteObjectDataWrapper<T> withData(T data) {
        return new RemoteObjectDataWrapper<>(key, refsFrom, frozen, data);
    }

    @Override
    public Collection<JObjectKey> collectRefsTo() {
        return data.collectRefsTo();
    }

    @Override
    public int estimateSize() {
        return data.estimateSize();
    }
}
