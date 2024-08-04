package com.usatiuk.dhfs.objects.jkleppmanntree.helpers;

import com.usatiuk.dhfs.objects.jkleppmanntree.JKleppmannTreePersistentData;
import com.usatiuk.dhfs.objects.repository.PersistentRemoteHostsService;
import com.usatiuk.dhfs.objects.repository.RemoteHostManager;
import com.usatiuk.dhfs.objects.repository.invalidation.OpSender;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.util.Collection;
import java.util.UUID;

@Singleton
public class OpQueueHelper {
    @Inject
    RemoteHostManager remoteHostManager;
    @Inject
    OpSender opSender;
    @Inject
    PersistentRemoteHostsService persistentRemoteHostsService;

    public void onRestore(JKleppmannTreePersistentData self) {
        remoteHostManager.registerConnectEventListener(h -> notifyOpSender(self));
    }

    public void notifyOpSender(JKleppmannTreePersistentData self) {
        opSender.push(self);
    }

    public UUID getSelfUUid() {
        return persistentRemoteHostsService.getSelfUuid();
    }

    public Collection<UUID> getHostList() {
        return persistentRemoteHostsService.getHostUuids();
    }
}
