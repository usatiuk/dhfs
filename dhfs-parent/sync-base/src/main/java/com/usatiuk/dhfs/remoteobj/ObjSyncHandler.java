package com.usatiuk.dhfs.remoteobj;

import com.usatiuk.objects.JObjectKey;
import com.usatiuk.dhfs.peersync.PeerId;
import org.pcollections.PMap;

import javax.annotation.Nullable;

public interface ObjSyncHandler<T extends JDataRemote, D extends JDataRemoteDto> {
    void handleRemoteUpdate(PeerId from, JObjectKey key,
                            PMap<PeerId, Long> receivedChangelog,
                            @Nullable D receivedData);
}
