package com.usatiuk.dhfs.storage.objects.repository.distributed;

import io.quarkus.logging.Log;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.Startup;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.*;

@ApplicationScoped
public class InvalidationQueueService {
    @Inject
    RemoteHostManager remoteHostManager;

    @Inject
    RemoteObjectServiceClient remoteObjectServiceClient;

    @ConfigProperty(name = "dhfs.objects.distributed.invalidation.batch_size")
    Integer batchSize;

    @ConfigProperty(name = "dhfs.objects.distributed.invalidation.delay")
    Integer delay;

    private Map<UUID, SequencedSet<String>> _hostToInvObj = new LinkedHashMap<>();

    private Thread _senderThread;

    @Startup
    void init() {
        _senderThread = new Thread(this::sender);
        _senderThread.setName("Invalidation sender");
        _senderThread.start();
    }

    void shutdown(@Observes @Priority(10) ShutdownEvent event) throws InterruptedException {
        _senderThread.interrupt();
        _senderThread.join();
    }

    private SequencedSet<String> getSetForHost(UUID host) {
        synchronized (this) {
            return _hostToInvObj.computeIfAbsent(host, k -> new LinkedHashSet<>());
        }
    }

    public Map<UUID, SequencedSet<String>> pullAll() throws InterruptedException {
        synchronized (this) {
            while (_hostToInvObj.isEmpty())
                this.wait();
            var ret = _hostToInvObj;
            _hostToInvObj = new LinkedHashMap<>();
            return ret;
        }
    }

    private void sender() {
        try {
            while (!Thread.interrupted()) {
                Thread.sleep(delay);
                var data = pullAll();
                String stats = "Sent invalidation: ";
                for (var forHost : data.entrySet()) {
                    long sent = 0;
                    while (!forHost.getValue().isEmpty()) {
                        ArrayList<String> chunk = new ArrayList<>();

                        while (chunk.size() < batchSize && !forHost.getValue().isEmpty()) {
                            chunk.add(forHost.getValue().removeFirst());
                        }

                        sent += chunk.size();

                        try {
                            var errs = remoteObjectServiceClient.notifyUpdate(forHost.getKey(), chunk);
                            for (var v : errs) {
                                Log.info("Failed to send invalidation to " + forHost.getKey() +
                                        " of " + v.getObjectName() + ": " + v.getError() + ", will retry");
                                pushInvalidationToOne(forHost.getKey(), v.getObjectName());
                                sent--;
                            }
                        } catch (Exception e) {
                            Log.info("Failed to send invalidation to " + forHost.getKey() + ": " + e.getMessage() + ", will retry");
                            for (var c : chunk)
                                pushInvalidationToOne(forHost.getKey(), c);
                        }
                        if (Thread.interrupted()) {
                            Log.info("Invalidation sender exiting");
                            return;
                        }
                    }
                    stats += forHost.getKey() + ": " + sent + " ";
                    Log.info(stats);
                }
            }
        } catch (InterruptedException ignored) {
        }
        Log.info("Invalidation sender exiting");
    }

    public void pushInvalidationToAll(String name) {
        synchronized (this) {
            for (var h : remoteHostManager.getSeenHosts()) {
                getSetForHost(h).add(name);
            }
            this.notifyAll();
        }
    }

    public void pushInvalidationToOne(UUID host, String name) {
        synchronized (this) {
            getSetForHost(host).add(name);
            this.notifyAll();
        }
    }
}
