package com.usatiuk.dhfs.objects.repository.invalidation;

import com.usatiuk.dhfs.objects.jrepository.DeletedObjectAccessException;
import com.usatiuk.dhfs.objects.jrepository.JObject;
import com.usatiuk.dhfs.objects.jrepository.JObjectManager;
import com.usatiuk.dhfs.objects.repository.PeerManager;
import com.usatiuk.dhfs.objects.repository.PersistentPeerDataService;
import com.usatiuk.dhfs.objects.repository.RemoteObjectServiceClient;
import com.usatiuk.utils.HashSetDelayedBlockingQueue;
import io.quarkus.logging.Log;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import io.vertx.core.impl.ConcurrentHashSet;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.commons.lang3.tuple.Pair;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

@ApplicationScoped
public class InvalidationQueueService {
    private final HashSetDelayedBlockingQueue<Pair<UUID, String>> _queue;
    private final AtomicReference<ConcurrentHashSet<String>> _toAllQueue = new AtomicReference<>(new ConcurrentHashSet<>());
    @Inject
    PeerManager remoteHostManager;
    @Inject
    RemoteObjectServiceClient remoteObjectServiceClient;
    @Inject
    JObjectManager jObjectManager;
    @Inject
    PersistentPeerDataService persistentPeerDataService;
    @Inject
    DeferredInvalidationQueueService deferredInvalidationQueueService;
    @ConfigProperty(name = "dhfs.objects.invalidation.threads")
    int threads;
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
            deferredInvalidationQueueService.defer(e.getLeft(), e.getRight());
    }

    private void sender() {
        while (!_shutdown) {
            try {
                try {
                    if (!_queue.hasImmediate()) {
                        ConcurrentHashSet<String> toAllQueue;

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
                                    _queue.add(Pair.of(h, o));
                                for (var u : hostInfo.unavailable())
                                    deferredInvalidationQueueService.defer(u, o);
                            }
                        }
                    }

                    var data = _queue.getAllWait(100, _queue.getDelay()); // TODO: config?
                    if (data.isEmpty()) continue;
                    String stats = "Sent invalidation: ";
                    long success = 0;

                    for (var e : data) {
                        if (!persistentPeerDataService.existsHost(e.getLeft())) continue;

                        if (!remoteHostManager.isReachable(e.getLeft())) {
                            deferredInvalidationQueueService.defer(e.getLeft(), e.getRight());
                            continue;
                        }

                        try {
                            jObjectManager.get(e.getRight()).ifPresent(obj -> {
                                remoteObjectServiceClient.notifyUpdate(obj, e.getLeft());
                            });
                            success++;
                        } catch (DeletedObjectAccessException ignored) {
                        } catch (Exception ex) {
                            Log.info("Failed to send invalidation to " + e.getLeft() + ", will retry", ex);
                            pushInvalidationToOne(e.getLeft(), e.getRight());
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

    public void pushInvalidationToAll(JObject<?> obj) {
        if (obj.getMeta().isOnlyLocal()) return;
        while (true) {
            var queue = _toAllQueue.get();
            if (queue == null) {
                var nq = new ConcurrentHashSet<String>();
                if (!_toAllQueue.compareAndSet(null, nq)) continue;
                queue = nq;
            }

            queue.add(obj.getMeta().getName());

            if (_toAllQueue.get() == queue) break;
        }
    }

    public void pushInvalidationToOne(UUID host, JObject<?> obj) {
        if (obj.getMeta().isOnlyLocal()) return;
        if (remoteHostManager.isReachable(host))
            _queue.add(Pair.of(host, obj.getMeta().getName()));
        else
            deferredInvalidationQueueService.defer(host, obj.getMeta().getName());
    }

    public void pushInvalidationToAll(String name) {
        pushInvalidationToAll(jObjectManager.get(name).orElseThrow(() -> new IllegalArgumentException("Object " + name + " not found")));
    }

    public void pushInvalidationToOne(UUID host, String name) {
        pushInvalidationToOne(host, jObjectManager.get(name).orElseThrow(() -> new IllegalArgumentException("Object " + name + " not found")));
    }

    protected void pushDeferredInvalidations(UUID host, String name) {
        _queue.add(Pair.of(host, name));
    }
}
