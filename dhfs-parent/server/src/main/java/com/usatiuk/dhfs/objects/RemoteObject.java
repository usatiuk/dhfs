package com.usatiuk.dhfs.objects;

import org.pcollections.HashTreePSet;
import org.pcollections.PCollection;
import org.pcollections.PMap;
import org.pcollections.TreePMap;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;

public record RemoteObject<T extends JDataRemote>(PCollection<JObjectKey> refsFrom, boolean frozen,
                                                  RemoteObjectMeta meta, @Nullable T data) implements JDataRefcounted {
    public RemoteObject(T data, PeerId initialPeer) {
        this(HashTreePSet.empty(), false, new RemoteObjectMeta(data.key(), data.getClass(), initialPeer), data);
    }

    public RemoteObject(JObjectKey key, PMap<PeerId, Long> remoteChangelog) {
        this(HashTreePSet.empty(), false, new RemoteObjectMeta(key, remoteChangelog), null);
    }

    public RemoteObject(JObjectKey key) {
        this(HashTreePSet.empty(), false, new RemoteObjectMeta(key, TreePMap.empty()), null);
    }

    @Override
    public JObjectKey key() {
        if (data != null && !data.key().equals(meta.key()))
            throw new IllegalStateException("Corrupted object, key mismatch: " + meta.key() + " vs " + data.key());
        return meta.key();
    }

    @Override
    public RemoteObject<T> withRefsFrom(PCollection<JObjectKey> refs) {
        return new RemoteObject<>(refs, frozen, meta, data);
    }

    @Override
    public RemoteObject<T> withFrozen(boolean frozen) {
        return new RemoteObject<>(refsFrom, frozen, meta, data);
    }

    public RemoteObject<T> withMeta(RemoteObjectMeta meta) {
        return new RemoteObject<>(refsFrom, frozen, meta, data);
    }

    public RemoteObject<T> withData(T data) {
        return new RemoteObject<>(refsFrom, frozen, meta, data);
    }

    public RemoteObject<T> withRefsFrom(PCollection<JObjectKey> refs, boolean frozen) {
        return new RemoteObject<>(refs, frozen, meta, data);
    }

    public ReceivedObject toReceivedObject() {
        if (data == null)
            throw new IllegalStateException("Cannot convert to ReceivedObject without data: " + meta.key());
        return new ReceivedObject(meta.key(), meta.changelog(), data);
    }

    @Override
    public Collection<JObjectKey> collectRefsTo() {
        if (data != null) return data.collectRefsTo();
        return List.of();
    }

    @Override
    public int estimateSize() {
        return data == null ? 1000 : data.estimateSize();
    }
}
