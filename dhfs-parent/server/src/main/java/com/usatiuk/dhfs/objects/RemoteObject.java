package com.usatiuk.dhfs.objects;

import org.pcollections.PCollection;
import org.pcollections.PMap;
import org.pcollections.PSet;

import java.util.Collection;
import java.util.List;

public record RemoteObject<T>(
        JObjectKey key, PCollection<JObjectKey> refsFrom, boolean frozen,
        PMap<PeerId, Long> knownRemoteVersions,
        Class<? extends JData> knownType,
        PSet<PeerId> confirmedDeletes,
        boolean seen,
        PMap<PeerId, Long> changelog,
        boolean haveLocal
) implements JDataRefcounted {
    @Override
    public RemoteObject<T> withRefsFrom(PCollection<JObjectKey> refs) {
        return new RemoteObject<>(key, refs, frozen, knownRemoteVersions, knownType, confirmedDeletes, seen, changelog, haveLocal);
    }

    @Override
    public RemoteObject<T> withFrozen(boolean frozen) {
        return new RemoteObject<>(key, refsFrom, frozen, knownRemoteVersions, knownType, confirmedDeletes, seen, changelog, haveLocal);
    }

    public RemoteObject<T> withKnownRemoteVersions(PMap<PeerId, Long> knownRemoteVersions) {
        return new RemoteObject<>(key, refsFrom, frozen, knownRemoteVersions, knownType, confirmedDeletes, seen, changelog, haveLocal);
    }

    public RemoteObject<T> withKnownType(Class<? extends JData> knownType) {
        return new RemoteObject<>(key, refsFrom, frozen, knownRemoteVersions, knownType, confirmedDeletes, seen, changelog, haveLocal);
    }

    public RemoteObject<T> withConfirmedDeletes(PSet<PeerId> confirmedDeletes) {
        return new RemoteObject<>(key, refsFrom, frozen, knownRemoteVersions, knownType, confirmedDeletes, seen, changelog, haveLocal);
    }

    public RemoteObject<T> withSeen(boolean seen) {
        return new RemoteObject<>(key, refsFrom, frozen, knownRemoteVersions, knownType, confirmedDeletes, seen, changelog, haveLocal);
    }

    public RemoteObject<T> withChangelog(PMap<PeerId, Long> changelog) {
        return new RemoteObject<>(key, refsFrom, frozen, knownRemoteVersions, knownType, confirmedDeletes, seen, changelog, haveLocal);
    }

    public RemoteObject<T> withHaveLocal(boolean haveLocal) {
        return new RemoteObject<>(key, refsFrom, frozen, knownRemoteVersions, knownType, confirmedDeletes, seen, changelog, haveLocal);
    }

    public static JObjectKey keyFrom(JObjectKey key) {
        return new JObjectKey(key + "_remote");
    }

    public JObjectKey localKey() {
        if (!haveLocal) throw new IllegalStateException("No local key");
        return JObjectKey.of(key.name().substring(0, key.name().length() - "_remote".length()));
    }

    @Override
    public Collection<JObjectKey> collectRefsTo() {
        if (haveLocal) return List.of(localKey());
        return List.of();
    }
}
