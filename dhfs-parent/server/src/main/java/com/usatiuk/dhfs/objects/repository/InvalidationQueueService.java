package com.usatiuk.dhfs.objects.repository;

import com.usatiuk.dhfs.objects.jrepository.DeletedObjectAccessException;
import com.usatiuk.dhfs.objects.jrepository.JObjectManager;
import com.usatiuk.utils.HashSetDelayedBlockingQueue;
import io.quarkus.logging.Log;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.Startup;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.apache.commons.lang3.tuple.Pair;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@ApplicationScoped
public class InvalidationQueueService {
    @Inject
    RemoteHostManager remoteHostManager;

    @Inject
    RemoteObjectServiceClient remoteObjectServiceClient;

    @Inject
    JObjectManager jObjectManager;

    @Inject
    PersistentRemoteHostsService persistentRemoteHostsService;

    @Inject
    DeferredInvalidationQueueService deferredInvalidationQueueService;

    @ConfigProperty(name = "dhfs.objects.invalidation.threads")
    int threads;

    private final HashSetDelayedBlockingQueue<Pair<UUID, String>> _queue;
    private final HashSetDelayedBlockingQueue<String> _toAllQueue = new HashSetDelayedBlockingQueue<>(0);
    private ExecutorService _executor;

    public InvalidationQueueService(@ConfigProperty(name = "dhfs.objects.invalidation.delay") int delay) {
        _queue = new HashSetDelayedBlockingQueue<>(delay);
    }

    @Startup
    void init() {
        _executor = Executors.newFixedThreadPool(threads);

        for (int i = 0; i < threads; i++) {
            _executor.submit(this::sender);
        }
    }

    void shutdown(@Observes @Priority(10) ShutdownEvent event) throws InterruptedException {
        _executor.shutdownNow();
        if (!_executor.awaitTermination(30, TimeUnit.SECONDS)) {
            Log.error("Failed to shut down invalidation sender thread");
        }
    }

    private void sender() {
        try {
            while (!Thread.interrupted()) {
                try {
                    var toAllProcess = _toAllQueue.getAll();

                    if (!toAllProcess.isEmpty()) {
                        var hostInfo = remoteHostManager.getHostStateSnapshot();
                        for (var o : toAllProcess) {
                            for (var h : hostInfo.available())
                                _queue.add(Pair.of(h, o));
                            for (var u : hostInfo.unavailable())
                                deferredInvalidationQueueService.defer(u, o);
                        }
                    }

                    var data = _queue.getAllWait(100); // TODO: config?
                    String stats = "Sent invalidation: ";
                    long success = 0;

                    for (var e : data) {
                        if (!persistentRemoteHostsService.existsHost(e.getLeft())) continue;

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
                        if (Thread.interrupted()) {
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
            }
        } catch (InterruptedException ignored) {
        }
        Log.info("Invalidation sender exiting");
        var data = _queue.close();
        for (var e : data)
            deferredInvalidationQueueService.defer(e.getLeft(), e.getRight());
        Log.info("Invalidation sender exited");
    }

    public void pushInvalidationToAll(String name) {
        _toAllQueue.add(name);
    }

    public void pushInvalidationToOne(UUID host, String name) {
        if (remoteHostManager.isReachable(host))
            _queue.add(Pair.of(host, name));
        else
            deferredInvalidationQueueService.returnForHost(host);
    }

    protected void pushDeferredInvalidations(UUID host, String name) {
        _queue.add(Pair.of(host, name));
    }
}
