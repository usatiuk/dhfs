package com.usatiuk.dhfs.storage.objects.repository.distributed;

import io.quarkus.logging.Log;
import io.quarkus.scheduler.Scheduled;
import io.smallrye.common.annotation.Blocking;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class InvalidationQueueService {
    private final InvalidationQueue _data = new InvalidationQueue();

    @Inject
    RemoteHostManager remoteHostManager;

    @Inject
    RemoteObjectServiceClient remoteObjectServiceClient;

    @Scheduled(every = "1s", concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
    @Blocking
    public void trySend() {
        var data = _data.runReadLocked(InvalidationQueueData::pullAll);
        for (var forHost : data.entrySet()) {
            for (var obj : forHost.getValue()) {
                try {
                    remoteObjectServiceClient.notifyUpdate(forHost.getKey(), obj);
                } catch (Exception e) {
                    Log.info("Failed to send invalidation to " + forHost.getKey() + " of " + obj + ": " + e.getMessage() + " will retry");
                    pushInvalidationToOne(forHost.getKey(), obj);
                }
            }
        }
    }

    public void pushInvalidationToAll(String name) {
        _data.runWriteLocked(d -> {
            for (var h : remoteHostManager.getSeenHosts()) {
                d.getSetForHost(h).add(name);
            }
            return null;
        });
    }

    public void pushInvalidationToOne(String host, String name) {
        _data.runWriteLocked(d -> {
            d.getSetForHost(host).add(name);
            return null;
        });
    }
}
