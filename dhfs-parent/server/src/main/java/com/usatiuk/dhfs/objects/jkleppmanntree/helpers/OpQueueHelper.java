package com.usatiuk.dhfs.objects.jkleppmanntree.helpers;

import com.usatiuk.dhfs.objects.jkleppmanntree.JKleppmannTreePersistentData;
import com.usatiuk.dhfs.objects.repository.PersistentRemoteHostsService;
import com.usatiuk.dhfs.objects.repository.RemoteHostManager;
import com.usatiuk.dhfs.objects.repository.invalidation.InvalidationQueueService;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.util.Collection;
import java.util.UUID;

@Singleton
public class OpQueueHelper {
    @Inject
    RemoteHostManager remoteHostManager;
    @Inject
    InvalidationQueueService invalidationQueueService;
    @Inject
    PersistentRemoteHostsService persistentRemoteHostsService;

    public void registerOnConnection(JKleppmannTreePersistentData self) {
        remoteHostManager.registerConnectEventListener(h -> notifyInvQueue(self));
    }

    public void notifyInvQueue(JKleppmannTreePersistentData self) {
        invalidationQueueService.pushInvalidationToAll(self);
    }

    public UUID getSelfUUid() {
        return persistentRemoteHostsService.getSelfUuid();
    }

    public Collection<UUID> getHostList() {
        return persistentRemoteHostsService.getHostUuids();
    }
}
