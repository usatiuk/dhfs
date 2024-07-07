package com.usatiuk.dhfs.objects.repository.autosync;

import com.usatiuk.dhfs.objects.jrepository.JObject;
import com.usatiuk.dhfs.objects.jrepository.JObjectData;
import com.usatiuk.dhfs.objects.jrepository.JObjectManager;
import com.usatiuk.dhfs.objects.jrepository.JObjectResolver;
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
            jObjectResolver.registerMetaWriteListener(JObjectData.class, obj -> {
                obj.assertRWLock();
                if (obj.getData() != null) return;
                if (obj.hasLocalCopy()) return;

                synchronized (_pending) {
                    _pending.add(obj.getName());
                    _pending.notify();
                }
            });
        }

        executorService.submit(() -> {
            for (var obj : jObjectManager.find("")) {
                if (!obj.hasLocalCopy())
                    synchronized (_pending) {
                        _pending.add(obj.getName());
                        _pending.notify();
                    }
            }
        });
    }

    void shutdown(@Observes @Priority(10) ShutdownEvent event) {
        _autosyncExcecutor.shutdownNow();
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
                        if (obj.runReadLocked(JObject.ResolutionStrategy.NO_RESOLUTION, (m, d) -> obj.hasLocalCopy()))
                            return;
                        obj.runWriteLocked(JObject.ResolutionStrategy.NO_RESOLUTION, (m, d, i, v) -> {
                            if (obj.hasLocalCopy()) return null;
                            obj.tryResolve(JObject.ResolutionStrategy.REMOTE);
                            return null;
                        });
                    });
                } catch (JObject.DeletedObjectAccessException ignored) {
                } catch (Exception e) {
                    Log.error("Failed downloading object " + name + ", will retry.", e);
                    synchronized (_pending) {
                        _pending.add(name);
                        _pending.notify(); // FIXME: Delay?
                    }
                }
            }
        } catch (InterruptedException ignored) {
        }
        Log.info("Autosync thread exiting");

    }
}
