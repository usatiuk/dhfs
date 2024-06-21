package com.usatiuk.dhfs.storage.objects.repository.distributed;

import io.quarkus.logging.Log;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.Startup;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

@ApplicationScoped
public class InvalidationQueueService {
    @Inject
    RemoteHostManager remoteHostManager;

    @Inject
    RemoteObjectServiceClient remoteObjectServiceClient;

    private Map<String, Set<String>> _hostToInvObj = new LinkedHashMap<>();

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

    private Set<String> getSetForHost(String host) {
        synchronized (this) {
            return _hostToInvObj.computeIfAbsent(host, k -> new LinkedHashSet<>());
        }
    }

    public Map<String, Set<String>> pullAll() throws InterruptedException {
        synchronized (this) {
            if (_hostToInvObj.isEmpty())
                this.wait();
            var ret = _hostToInvObj;
            _hostToInvObj = new LinkedHashMap<>();
            return ret;
        }
    }

    private void sender() {
        try {
            while (true) {
                var data = pullAll();
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
        } catch (InterruptedException e) {
            Log.info("Invalidation sender exiting");
        }
    }

    public void pushInvalidationToAll(String name) {
        synchronized (this) {
            for (var h : remoteHostManager.getSeenHosts()) {
                getSetForHost(h).add(name);
            }
            this.notifyAll();
        }
    }

    public void pushInvalidationToOne(String host, String name) {
        synchronized (this) {
            getSetForHost(host).add(name);
            this.notifyAll();
        }
    }
}
