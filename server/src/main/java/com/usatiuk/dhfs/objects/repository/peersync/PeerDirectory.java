package com.usatiuk.dhfs.objects.repository.peersync;

import com.usatiuk.dhfs.objects.jrepository.JObjectData;
import com.usatiuk.dhfs.objects.repository.ConflictResolver;
import lombok.Getter;

import java.io.Serial;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.UUID;

public class PeerDirectory extends JObjectData {
    @Serial
    private static final long serialVersionUID = 1;

    @Getter
    private final Set<UUID> _peers = new LinkedHashSet<>();

    public static final String PeerDirectoryObjName = "peer_directory";

    @Override
    public String getName() {
        return PeerDirectoryObjName;
    }

    @Override
    public Class<? extends ConflictResolver> getConflictResolver() {
        return PeerDirectoryConflictResolver.class;
    }

    @Override
    public Class<? extends JObjectData> getRefType() {
        return PersistentPeerInfo.class;
    }

    @Override
    public boolean pushResolution() {
        return true;
    }

    @Override
    public Collection<String> extractRefs() {
        return _peers.stream().map(PersistentPeerInfo::getNameFromUuid).toList();
    }

    @Override
    public long estimateSize() {
        return _peers.size() * 16L;
    }
}
