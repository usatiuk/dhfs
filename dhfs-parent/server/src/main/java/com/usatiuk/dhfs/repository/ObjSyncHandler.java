package com.usatiuk.dhfs.repository;

import com.usatiuk.dhfs.JDataRemote;
import com.usatiuk.dhfs.objects.JObjectKey;
import com.usatiuk.dhfs.PeerId;
import org.pcollections.PMap;

import javax.annotation.Nullable;

public interface ObjSyncHandler<T extends JDataRemote, D extends JDataRemoteDto> {
    void handleRemoteUpdate(PeerId from, JObjectKey key,
                            PMap<PeerId, Long> receivedChangelog,
                            @Nullable D receivedData);
}
