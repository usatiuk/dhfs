package com.usatiuk.dhfs;

import com.usatiuk.objects.JObjectKey;
import org.pcollections.*;

import java.util.Collection;
import java.util.List;

public record RemoteObjectMeta(PCollection<JDataRef> refsFrom, boolean frozen,
                               JObjectKey key,
                               PMap<PeerId, Long> knownRemoteVersions,
                               Class<? extends JDataRemote> knownType,
                               PSet<PeerId> confirmedDeletes,
                               boolean seen,
                               PMap<PeerId, Long> changelog,
                               boolean hasLocalData) implements JDataRefcounted {
    // Self put
    public RemoteObjectMeta(JDataRemote data, PeerId initialPeer) {
        this(HashTreePSet.empty(), false,
                data.key(), HashTreePMap.empty(), data.getClass(), HashTreePSet.empty(), false,
                HashTreePMap.<PeerId, Long>empty().plus(initialPeer, 1L),
                true);
    }

    public RemoteObjectMeta(JObjectKey key, PMap<PeerId, Long> remoteChangelog) {
        this(HashTreePSet.empty(), false,
                key, HashTreePMap.empty(), JDataRemote.class, HashTreePSet.empty(), true,
                remoteChangelog,
                false);
    }

    public RemoteObjectMeta(JObjectKey key) {
        this(HashTreePSet.empty(), false,
                key, HashTreePMap.empty(), JDataRemote.class, HashTreePSet.empty(), true,
                TreePMap.empty(),
                false);
    }

    public static JObjectKey ofMetaKey(JObjectKey key) {
        return key;
    }

    public static JObjectKey ofDataKey(JObjectKey key) {
        return JObjectKey.of("data_" + key.name());
    }

    @Override
    public JObjectKey key() {
        return ofMetaKey(key);
    }

    public JObjectKey dataKey() {
        return ofDataKey(key);
    }

    @Override
    public RemoteObjectMeta withRefsFrom(PCollection<JDataRef> refs) {
        return new RemoteObjectMeta(refs, frozen, key, knownRemoteVersions, knownType, confirmedDeletes, seen, changelog, hasLocalData);
    }

    @Override
    public RemoteObjectMeta withFrozen(boolean frozen) {
        return new RemoteObjectMeta(refsFrom, frozen, key, knownRemoteVersions, knownType, confirmedDeletes, seen, changelog, hasLocalData);
    }

    public RemoteObjectMeta withKnownRemoteVersions(PMap<PeerId, Long> knownRemoteVersions) {
        return new RemoteObjectMeta(refsFrom, frozen, key, knownRemoteVersions, knownType, confirmedDeletes, seen, changelog, hasLocalData);
    }

    public RemoteObjectMeta withKnownType(Class<? extends JDataRemote> knownType) {
        return new RemoteObjectMeta(refsFrom, frozen, key, knownRemoteVersions, knownType, confirmedDeletes, seen, changelog, hasLocalData);
    }

    public RemoteObjectMeta withConfirmedDeletes(PSet<PeerId> confirmedDeletes) {
        return new RemoteObjectMeta(refsFrom, frozen, key, knownRemoteVersions, knownType, confirmedDeletes, seen, changelog, hasLocalData);
    }

    public RemoteObjectMeta withSeen(boolean seen) {
        return new RemoteObjectMeta(refsFrom, frozen, key, knownRemoteVersions, knownType, confirmedDeletes, seen, changelog, hasLocalData);
    }

    public RemoteObjectMeta withChangelog(PMap<PeerId, Long> changelog) {
        return new RemoteObjectMeta(refsFrom, frozen, key, knownRemoteVersions, knownType, confirmedDeletes, seen, changelog, hasLocalData);
    }

    public RemoteObjectMeta withHaveLocal(boolean haveLocal) {
        return new RemoteObjectMeta(refsFrom, frozen, key, knownRemoteVersions, knownType, confirmedDeletes, seen, changelog, haveLocal);
    }

    public long versionSum() {
        return changelog.values().stream().mapToLong(Long::longValue).sum();
    }

    @Override
    public Collection<JObjectKey> collectRefsTo() {
        if (hasLocalData) return List.of(dataKey());
        return List.of();
    }

    @Override
    public int estimateSize() {
        return 1000;
    }
}
