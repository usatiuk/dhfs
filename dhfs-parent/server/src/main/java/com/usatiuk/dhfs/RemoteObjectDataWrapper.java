package com.usatiuk.dhfs;

import com.usatiuk.dhfs.objects.JObjectKey;
import org.pcollections.HashTreePSet;
import org.pcollections.PCollection;

import java.util.Collection;

public record RemoteObjectDataWrapper<T extends JDataRemote>(PCollection<JDataRef> refsFrom,
                                                             boolean frozen,
                                                             T data) implements JDataRefcounted {
    public RemoteObjectDataWrapper(T data) {
        this(HashTreePSet.empty(), false, data);
    }

    @Override
    public RemoteObjectDataWrapper<T> withRefsFrom(PCollection<JDataRef> refs) {
        return new RemoteObjectDataWrapper<>(refs, frozen, data);
    }

    @Override
    public RemoteObjectDataWrapper<T> withFrozen(boolean frozen) {
        return new RemoteObjectDataWrapper<>(refsFrom, frozen, data);
    }

    public RemoteObjectDataWrapper<T> withData(T data) {
        return new RemoteObjectDataWrapper<>(refsFrom, frozen, data);
    }

    @Override
    public JObjectKey key() {
        return RemoteObjectMeta.ofDataKey(data.key());
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
