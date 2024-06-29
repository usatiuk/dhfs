package com.usatiuk.dhfs.storage.objects.jrepository;

import com.google.common.collect.Streams;
import com.usatiuk.dhfs.storage.objects.repository.persistence.ObjectPersistentStore;
import io.quarkus.logging.Log;
import io.quarkus.runtime.Shutdown;
import io.quarkus.runtime.Startup;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.LinkedHashMap;
import java.util.stream.Stream;

@ApplicationScoped
public class JObjectRefProcessor {

    private Thread _refProcessorThread;

    @ConfigProperty(name = "dhfs.objects.deletion.delay")
    Long deletionDelay;

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
                    got.get().runWriteLocked(JObject.ResolutionStrategy.NO_RESOLUTION, (m, d, v, i) -> {
                        if (m.isDeleted()) return null;
                        if (m.getRefcount() > 0) return null;
                        if (!m.isSeen()) {
                            jObjectManager.tryQuickDelete(got.get());
                            return null;
                        }

                        got.get().tryResolve(JObject.ResolutionStrategy.LOCAL_ONLY);

                        Log.info("Deleting " + m.getName());
                        m.delete();

                        Stream<String> refs = Stream.empty();

                        if (!m.getSavedRefs().isEmpty())
                            refs = m.getSavedRefs().stream();
                        if (d != null)
                            refs = Streams.concat(refs, d.extractRefs().stream());

                        refs.forEach(c -> {
                            jObjectManager.get(c).ifPresent(ref -> ref.runWriteLocked(JObject.ResolutionStrategy.NO_RESOLUTION, (mc, dc, bc, ic) -> {
                                mc.removeRef(m.getName());
                                return null;
                            }));
                        });

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
