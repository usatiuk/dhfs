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
    JObjectWriteback jObjectWriteback;

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
        synchronized (_candidates) {
            if (_candidates.putIfAbsent(name, System.currentTimeMillis()) == null)
                _candidates.notify();
        }
    }

    private void refProcessorThread() {
        try {
            while (!Thread.interrupted()) {
                String next;
                Long nextTime;

                synchronized (_candidates) {
                    while (_candidates.isEmpty())
                        _candidates.wait();

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

                        got.get().tryResolve(JObject.ResolutionStrategy.LOCAL_ONLY);

                        Log.trace("Deleting " + m.getName());
                        m.delete();

                        Stream<String> refs = Stream.empty();

                        if (!m.getSavedRefs().isEmpty())
                            refs = m.getSavedRefs().stream();
                        if (got.get().getData() != null)
                            refs = Streams.concat(refs, got.get().getData().extractRefs().stream());

                        got.get().discardData();

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
