package com.usatiuk.dhfs.objects.repository.peersync;

import com.usatiuk.dhfs.objects.jrepository.JObjectData;
import com.usatiuk.dhfs.objects.jrepository.OnlyLocal;
import lombok.Getter;

import java.util.HashSet;
import java.util.UUID;

@OnlyLocal
public class PeerDirectoryLocal extends JObjectData {
    public static final String PeerDirectoryLocalObjName = "peer_directory_local";
    @Getter
    private final HashSet<UUID> _initialOpSyncDone = new HashSet<>();
    @Getter
    private final HashSet<UUID> _initialObjSyncDone = new HashSet<>();

    @Override
    public String getName() {
        return PeerDirectoryLocalObjName;
    }

    @Override
    public int estimateSize() {
        return 1024; //FIXME:
    }
}
