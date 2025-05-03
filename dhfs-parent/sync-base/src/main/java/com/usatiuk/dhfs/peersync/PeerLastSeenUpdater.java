package com.usatiuk.dhfs.peersync;

import com.usatiuk.dhfs.remoteobj.RemoteTransaction;
import com.usatiuk.objects.transaction.Transaction;
import com.usatiuk.objects.transaction.TransactionManager;
import io.quarkus.scheduler.Scheduled;
import io.smallrye.common.annotation.Blocking;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

@ApplicationScoped
public class PeerLastSeenUpdater {
    @Inject
    PeerManager peerManager;
    @Inject
    PeerInfoService peerInfoService;
    @Inject
    Transaction curTx;
    @Inject
    TransactionManager txm;
    @Inject
    RemoteTransaction remoteTransaction;

    @ConfigProperty(name = "dhfs.objects.last-seen.timeout")
    long lastSeenTimeout;
    @Inject
    PersistentPeerDataService persistentPeerDataService;

    @Scheduled(every = "${dhfs.objects.last-seen.update}", concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
    @Blocking
    void update() {
        var snapshot = peerManager.getHostStateSnapshot();
        for (var a : snapshot.available()) {
            txm.run(() -> {
                var curInfo = remoteTransaction.getData(PeerInfo.class, a.id()).orElse(null);
                if (curInfo == null) return;

                var newInfo = curInfo.withLastSeenTimestamp(System.currentTimeMillis());
                remoteTransaction.putData(newInfo);
            });
        }

        for (var u : snapshot.unavailable()) {
            txm.run(() -> {
                if (!persistentPeerDataService.isInitialSyncDone(u))
                    return;

                var curInfo = remoteTransaction.getData(PeerInfo.class, u.id()).orElse(null);
                if (curInfo == null) return;

                var lastSeen = curInfo.lastSeenTimestamp();
                if (System.currentTimeMillis() - lastSeen > (lastSeenTimeout * 1000)) {
                    var kicked = curInfo.withIncrementedKickCounter(persistentPeerDataService.getSelfUuid());
                    remoteTransaction.putData(kicked);
                }
            });
        }
    }
}
