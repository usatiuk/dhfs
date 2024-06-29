package com.usatiuk.dhfs.storage.objects.jrepository;

import com.usatiuk.dhfs.storage.objects.repository.persistence.ObjectPersistentStore;
import io.quarkus.logging.Log;
import io.quarkus.runtime.Shutdown;
import io.quarkus.runtime.Startup;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.util.LinkedHashMap;

@ApplicationScoped
public class JObjectRefProcessor {

    private Thread _refProcessorThread;

    private Long deletionDelay = 0L;

    @Inject
    JObjectManager jObjectManager;

    @Inject
    ObjectPersistentStore objectPersistentStore;

    @Startup
    void init() {
        _refProcessorThread = new Thread(this::refProcessorThread);
        _refProcessorThread.setName("JObject Refcounter thread");
        _refProcessorThread.start();
    }

    @Shutdown
    void shutdown() throws InterruptedException {
        _refProcessorThread.interrupt();
        _refProcessorThread.join();
    }

    private final LinkedHashMap<String, Long> _candidates = new LinkedHashMap<>();

    public void putDeletionCandidate(String name) {
        synchronized (this) {
            _candidates.putIfAbsent(name, System.currentTimeMillis());
            this.notify();
        }
    }

    private void refProcessorThread() {
        try {
            while (!Thread.interrupted()) {
                String next;
                Long nextTime;

                synchronized (this) {
                    while (_candidates.isEmpty())
                        this.wait();

                    var e = _candidates.firstEntry();
                    next = e.getKey();
                    nextTime = e.getValue();
                    _candidates.remove(next);
                }

                if ((System.currentTimeMillis() - nextTime) < deletionDelay) {
                    Thread.sleep(deletionDelay);
                }

                var got = jObjectManager.get(next);
                if (got.isEmpty()) continue;
                try {
                    got.get().runWriteLocked(JObject.ResolutionStrategy.LOCAL_ONLY, (m, d, v, i) -> {
                        if (m.isDeleted()) return null;
                        if (m.getRefcount() > 0) return null;
                        if (!m.isSeen()) {
                            jObjectManager.tryQuickDelete(got.get());
                            return null;
                        }

                        Log.info("Deleting " + m.getName());
                        m.delete();
                        //FIXME:
                        if (!m.getSavedRefs().isEmpty()) {
                            for (var c : m.getSavedRefs()) {
                                jObjectManager.get(c).ifPresent(ref -> ref.runWriteLocked(JObject.ResolutionStrategy.LOCAL_ONLY, (mc, dc, bc, ic) -> {
                                    mc.removeRef(m.getName());
                                    return null;
                                }));
                            }
                        }
                        if (d != null)
                            for (var c : d.extractRefs()) {
                                jObjectManager.get(c).ifPresent(ref -> ref.runWriteLocked(JObject.ResolutionStrategy.LOCAL_ONLY, (mc, dc, bc, ic) -> {
                                    mc.removeRef(m.getName());
                                    return null;
                                }));
                            }

                        return null;
                    });
                } catch (Exception ex) {
                    Log.error("Error when deleting: " + next, ex);
                }
            }
        } catch (InterruptedException ignored) {
        }
        Log.info("JObject Refcounter thread exiting");
    }
}
