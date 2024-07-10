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
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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

    @ConfigProperty(name = "dhfs.objects.invalidation.threads")
    int threads;

    private record QueueEntry(UUID host, String obj) {
    }

    private final HashSetDelayedBlockingQueue<QueueEntry> _queue;
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
    }

    private void sender() {
        try {
            while (!Thread.interrupted()) {
                try {
                    var data = _queue.getAllWait();
                    String stats = "Sent invalidation: ";
                    long success = 0;

                    for (var e : data) {
                        if (!persistentRemoteHostsService.existsHost(e.host)) continue;

                        try {
                            jObjectManager.get(e.obj).ifPresent(obj -> {
                                remoteObjectServiceClient.notifyUpdate(obj, e.host);
                            });
                            success++;
                        } catch (DeletedObjectAccessException ignored) {
                        } catch (Exception ex) {
                            Log.info("Failed to send invalidation to " + e.host + ", will retry", ex);
                            pushInvalidationToOne(e.host, e.obj);
                        }
                        if (Thread.interrupted()) {
                            Log.info("Invalidation sender exiting");
                            return;
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
    }

    public void pushInvalidationToAll(String name) {
        var hosts = remoteHostManager.getSeenHosts();
        for (var h : hosts) {
            _queue.add(new QueueEntry(h, name));
        }
    }

    public void pushInvalidationToOne(UUID host, String name) {
        _queue.add(new QueueEntry(host, name));
    }
}
