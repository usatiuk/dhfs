package com.usatiuk.dhfs.objects.repository.autosync;

import com.usatiuk.dhfs.objects.jrepository.JObject;
import com.usatiuk.dhfs.objects.jrepository.JObjectData;
import com.usatiuk.dhfs.objects.jrepository.JObjectManager;
import com.usatiuk.dhfs.objects.jrepository.JObjectResolver;
import com.usatiuk.dhfs.objects.repository.peersync.PeerDirectory;
import com.usatiuk.dhfs.objects.repository.peersync.PersistentPeerInfo;
import io.quarkus.logging.Log;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.Startup;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.LinkedHashSet;
import java.util.SequencedSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@ApplicationScoped
public class AutoSyncProcessor {
    @Inject
    JObjectResolver jObjectResolver;

    @Inject
    JObjectManager jObjectManager;

    @ConfigProperty(name = "dhfs.objects.autosync.threads")
    int autosyncThreads;

    @ConfigProperty(name = "dhfs.objects.autosync.download-all")
    boolean downloadAll;

    private ExecutorService _autosyncExcecutor;

    @Inject
    ExecutorService executorService;

    private final SequencedSet<String> _pending = new LinkedHashSet<>();

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

        executorService.submit(() -> {
            for (var obj : jObjectManager.find("")) {
                if (!obj.hasLocalCopy())
                    add(obj.getName());
            }
        });
    }

    private void alwaysSaveCallback(JObject<?> obj) {
        obj.assertRWLock();
        if (obj.getData() != null) return;
        if (obj.hasLocalCopy()) return;

        add(obj.getName());
    }

    void shutdown(@Observes @Priority(10) ShutdownEvent event) {
        _autosyncExcecutor.shutdownNow();
    }

    private void add(String name) {
        synchronized (_pending) {
            _pending.add(name);
            _pending.notify(); // FIXME: Delay?
        }

    }

    private void autosync() {
        try {
            while (!Thread.interrupted()) {
                String name;
                synchronized (_pending) {
                    while (_pending.isEmpty())
                        _pending.wait();

                    name = _pending.removeFirst();
                }

                try {
                    jObjectManager.get(name).ifPresent(obj -> {
                        // FIXME: does this double lock make sense?
                        if (obj.runReadLocked(JObject.ResolutionStrategy.NO_RESOLUTION, (m, d) -> obj.hasLocalCopy() || m.isDeleted()))
                            return;
                        boolean ok = obj.runWriteLocked(JObject.ResolutionStrategy.NO_RESOLUTION, (m, d, i, v) -> {
                            if (obj.hasLocalCopy()) return true;
                            return obj.tryResolve(JObject.ResolutionStrategy.REMOTE);
                        });
                        if (!ok) {
                            Log.warn("Failed downloading object " + name + ", will retry.");
                            add(name);
                        }
                    });
                } catch (JObject.DeletedObjectAccessException ignored) {
                } catch (Exception e) {
                    Log.error("Failed downloading object " + name + ", will retry.", e);
                    add(name);
                    // Delay?
                }
            }
        } catch (InterruptedException ignored) {
        }
        Log.info("Autosync thread exiting");

    }
}
