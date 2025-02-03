package com.usatiuk.dhfs.objects;

import org.pcollections.HashTreePMap;
import org.pcollections.HashTreePSet;
import org.pcollections.PMap;
import org.pcollections.PSet;

import java.io.Serializable;

public record RemoteObjectMeta(
        JObjectKey key,
        PMap<PeerId, Long> knownRemoteVersions,
        Class<? extends JDataRemote> knownType,
        PSet<PeerId> confirmedDeletes,
        boolean seen,
        PMap<PeerId, Long> changelog) implements Serializable {
    public RemoteObjectMeta(JObjectKey key, Class<? extends JDataRemote> type, PeerId initialPeer) {
        this(key, HashTreePMap.empty(), type, HashTreePSet.empty(), true,
                HashTreePMap.<PeerId, Long>empty().plus(initialPeer, 1L));
    }

    public RemoteObjectMeta(JObjectKey key, PMap<PeerId, Long> remoteChangelog) {
        this(key, HashTreePMap.empty(), JDataRemote.class, HashTreePSet.empty(), true, remoteChangelog);
    }

    public RemoteObjectMeta withKnownRemoteVersions(PMap<PeerId, Long> knownRemoteVersions) {
        return new RemoteObjectMeta(key, knownRemoteVersions, knownType, confirmedDeletes, seen, changelog);
    }

    public RemoteObjectMeta withKnownType(Class<? extends JDataRemote> knownType) {
        return new RemoteObjectMeta(key, knownRemoteVersions, knownType, confirmedDeletes, seen, changelog);
    }

    public RemoteObjectMeta withConfirmedDeletes(PSet<PeerId> confirmedDeletes) {
        return new RemoteObjectMeta(key, knownRemoteVersions, knownType, confirmedDeletes, seen, changelog);
    }

    public RemoteObjectMeta withSeen(boolean seen) {
        return new RemoteObjectMeta(key, knownRemoteVersions, knownType, confirmedDeletes, seen, changelog);
    }

    public RemoteObjectMeta withChangelog(PMap<PeerId, Long> changelog) {
        return new RemoteObjectMeta(key, knownRemoteVersions, knownType, confirmedDeletes, seen, changelog);
    }

    public RemoteObjectMeta withHaveLocal(boolean haveLocal) {
        return new RemoteObjectMeta(key, knownRemoteVersions, knownType, confirmedDeletes, seen, changelog);
    }

    public long versionSum() {
        return changelog.values().stream().mapToLong(Long::longValue).sum();
    }
}
