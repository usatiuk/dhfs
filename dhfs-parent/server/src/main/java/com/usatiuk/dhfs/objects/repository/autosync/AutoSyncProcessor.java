package com.usatiuk.dhfs.objects.repository.autosync;

import com.usatiuk.dhfs.objects.jrepository.*;
import com.usatiuk.dhfs.objects.repository.peersync.PeerDirectory;
import com.usatiuk.dhfs.objects.repository.peersync.PersistentPeerInfo;
import com.usatiuk.utils.HashSetDelayedBlockingQueue;
import io.quarkus.logging.Log;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.Startup;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@ApplicationScoped
public class AutoSyncProcessor {
    private final HashSetDelayedBlockingQueue<String> _pending = new HashSetDelayedBlockingQueue<>(0);
    private final HashSetDelayedBlockingQueue<String> _retries = new HashSetDelayedBlockingQueue<>(1000); //FIXME:
    @Inject
    JObjectResolver jObjectResolver;
    @Inject
    JObjectManager jObjectManager;
    @ConfigProperty(name = "dhfs.objects.autosync.threads")
    int autosyncThreads;
    @ConfigProperty(name = "dhfs.objects.autosync.download-all")
    boolean downloadAll;
    @Inject
    ExecutorService executorService;
    private ExecutorService _autosyncExcecutor;

    @Startup
    void init() {
        _autosyncExcecutor = Executors.newFixedThreadPool(autosyncThreads);
        for (int i = 0; i < autosyncThreads; i++) {
            _autosyncExcecutor.submit(this::autosync);
        }

        if (downloadAll) {
            jObjectResolver.registerMetaWriteListener(JObjectData.class, this::alwaysSaveCallback);
        } else {
            jObjectResolver.registerMetaWriteListener(PersistentPeerInfo.class, this::alwaysSaveCallback);
            jObjectResolver.registerMetaWriteListener(PeerDirectory.class, this::alwaysSaveCallback);
        }

        if (downloadAll)
            executorService.submit(() -> {
                for (var obj : jObjectManager.find("")) {
                    if (!obj.hasLocalCopy())
                        add(obj.getName());
                }
            });
    }

    private void alwaysSaveCallback(JObject<?> obj) {
        obj.assertRWLock();
        if (obj.getMeta().isDeleted()) return;
        if (obj.getData() != null) return;
        if (obj.hasLocalCopy()) return;

        add(obj.getName());
    }

    void shutdown(@Observes @Priority(10) ShutdownEvent event) {
        _autosyncExcecutor.shutdownNow();
    }

    public void add(String name) {
        _pending.add(name);
    }

    private void autosync() {
        try {
            while (!Thread.interrupted()) {
                String name = null;

                while (name == null) {
                    name = _retries.tryGet();
                    if (name == null)
                        name = _pending.get(1000L); //FIXME:
                }

                try {
                    jObjectManager.get(name).ifPresent(obj -> {
                        // FIXME: does this double lock make sense?
                        if (obj.runReadLocked(JObject.ResolutionStrategy.NO_RESOLUTION, (m, d) -> d != null || m.isDeleted() || obj.hasLocalCopy()))
                            return;
                        boolean ok = obj.runWriteLocked(JObject.ResolutionStrategy.NO_RESOLUTION, (m, d, i, v) -> {
                            if (obj.hasLocalCopy()) return true;
                            return obj.tryResolve(JObject.ResolutionStrategy.REMOTE);
                        });
                        if (!ok) {
                            Log.warn("Failed downloading object " + obj.getName() + ", will retry.");
                            _retries.add(obj.getName());
                        }
                    });
                } catch (DeletedObjectAccessException ignored) {
                } catch (Exception e) {
                    Log.error("Failed downloading object " + name + ", will retry.", e);
                    _retries.add(name);
                }
            }
        } catch (InterruptedException ignored) {
        }
        Log.info("Autosync thread exiting");

    }
}