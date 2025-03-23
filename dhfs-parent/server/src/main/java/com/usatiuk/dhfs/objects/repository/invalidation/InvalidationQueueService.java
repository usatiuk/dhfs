package com.usatiuk.dhfs.objects.repository.invalidation;

import com.usatiuk.dhfs.objects.JObjectKey;
import com.usatiuk.dhfs.objects.PeerId;
import com.usatiuk.dhfs.objects.repository.PeerManager;
import com.usatiuk.dhfs.objects.repository.PersistentPeerDataService;
import com.usatiuk.dhfs.objects.repository.peersync.PeerInfoService;
import com.usatiuk.dhfs.utils.HashSetDelayedBlockingQueue;
import io.quarkus.logging.Log;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import io.vertx.core.impl.ConcurrentHashSet;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

@ApplicationScoped
public class InvalidationQueueService {
    private final HashSetDelayedBlockingQueue<InvalidationQueueEntry> _queue;
    private final AtomicReference<ConcurrentHashSet<JObjectKey>> _toAllQueue = new AtomicReference<>(new ConcurrentHashSet<>());
    @Inject
    PeerManager remoteHostManager;
    @Inject
    DeferredInvalidationQueueService deferredInvalidationQueueService;
    @Inject
    PeerInfoService peerInfoService;
    @Inject
    OpPusher opPusher;
    @ConfigProperty(name = "dhfs.objects.invalidation.threads")
    int threads;
    @Inject
    PersistentPeerDataService persistentPeerDataService;

    private ExecutorService _executor;
    private volatile boolean _shutdown = false;

    public InvalidationQueueService(@ConfigProperty(name = "dhfs.objects.invalidation.delay") int delay) {
        _queue = new HashSetDelayedBlockingQueue<>(delay);
    }

    void init(@Observes @Priority(300) StartupEvent event) throws InterruptedException {
        BasicThreadFactory factory = new BasicThreadFactory.Builder()
                .namingPattern("invalidation-%d")
                .build();

        _executor = Executors.newFixedThreadPool(threads, factory);

        for (int i = 0; i < threads; i++) {
            _executor.submit(this::sender);
        }
    }

    void shutdown(@Observes @Priority(10) ShutdownEvent event) throws InterruptedException {
        _shutdown = true;
        _executor.shutdownNow();
        if (!_executor.awaitTermination(30, TimeUnit.SECONDS)) {
            Log.error("Failed to shut down invalidation sender thread");
        }
        var data = _queue.close();
        Log.info("Will defer " + data.size() + " invalidations on shutdown");
        for (var e : data)
            deferredInvalidationQueueService.defer(e);
    }

    private void sender() {
        while (!_shutdown) {
            try {
                try {
                    if (!_queue.hasImmediate()) {
                        ConcurrentHashSet<JObjectKey> toAllQueue;

                        while (true) {
                            toAllQueue = _toAllQueue.get();
                            if (toAllQueue != null) {
                                if (_toAllQueue.compareAndSet(toAllQueue, null))
                                    break;
                            } else {
                                break;
                            }
                        }

                        if (toAllQueue != null) {
                            var hostInfo = remoteHostManager.getHostStateSnapshot();
                            for (var o : toAllQueue) {
                                for (var h : hostInfo.available())
                                    _queue.add(new InvalidationQueueEntry(h, o));
                                for (var u : hostInfo.unavailable())
                                    deferredInvalidationQueueService.defer(new InvalidationQueueEntry(u, o));
                            }
                        }
                    }

                    var data = _queue.getAllWait(100, _queue.getDelay()); // TODO: config?
                    if (data.isEmpty()) continue;
                    String stats = "Sent invalidation: ";
                    long success = 0;

                    for (var e : data) {
                        // TODO: Race?
                        if (!peerInfoService.existsPeer(e.peer())) {
                            Log.warnv("Will ignore invalidation of {0} to {1}, peer not found", e.key(), e.peer());
                            continue;
                        }

                        if (!remoteHostManager.isReachable(e.peer())) {
                            deferredInvalidationQueueService.defer(e);
                            continue;
                        }

                        if (!persistentPeerDataService.isInitialSyncDone(e.peer())) {
                            pushInvalidationToOne(e);
                            continue;
                        }

                        try {
                            opPusher.doPush(e);
                            success++;
                        } catch (Exception ex) {
                            Log.warnv("Failed to send invalidation to {0}, will retry: {1}", e, ex);
                            pushInvalidationToOne(e);
                        }
                        if (_shutdown) {
                            Log.info("Invalidation sender exiting");
                            break;
                        }
                    }

                    stats += success + "/" + data.size() + " ";
                    Log.info(stats);
                } catch (InterruptedException ie) {
                    throw ie;
                } catch (Exception e) {
                    Log.error("Exception in invalidation sender thread: ", e);
                }
            } catch (InterruptedException ignored) {
            }
        }
        Log.info("Invalidation sender exiting");
    }

    public void pushInvalidationToAll(JObjectKey key) {
        while (true) {
            var queue = _toAllQueue.get();
            if (queue == null) {
                var nq = new ConcurrentHashSet<JObjectKey>();
                if (!_toAllQueue.compareAndSet(null, nq)) continue;
                queue = nq;
            }

            queue.add(key);

            if (_toAllQueue.get() == queue) break;
        }
    }

    void pushInvalidationToOne(InvalidationQueueEntry entry) {
        if (remoteHostManager.isReachable(entry.peer()))
            _queue.add(entry);
        else
            deferredInvalidationQueueService.defer(entry);
    }

    public void pushInvalidationToOne(PeerId host, JObjectKey obj) {
        var entry = new InvalidationQueueEntry(host, obj);
        pushInvalidationToOne(entry);
    }

    void pushDeferredInvalidations(InvalidationQueueEntry entry) {
        _queue.add(entry);
    }
}
