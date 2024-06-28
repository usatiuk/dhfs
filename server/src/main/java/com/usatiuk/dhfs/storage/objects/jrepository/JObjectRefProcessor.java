package com.usatiuk.dhfs.storage.objects.jrepository;

import com.usatiuk.dhfs.storage.objects.repository.persistence.ObjectPersistentStore;
import io.quarkus.logging.Log;
import io.quarkus.runtime.Shutdown;
import io.quarkus.runtime.Startup;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.commons.lang3.tuple.Pair;

import java.util.concurrent.LinkedBlockingQueue;

@ApplicationScoped
public class JObjectRefProcessor {

    private Thread _refProcessorThread;

    private Long deletionDelay = 0L;

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

    private LinkedBlockingQueue<Pair<Long, String>> _candidates = new LinkedBlockingQueue<>();

    public void putDeletionCandidate(String name) {
        try {
            _candidates.put(Pair.of(System.currentTimeMillis(), name));
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private void refProcessorThread() {
        try {
            while (true) {
                var next = _candidates.take();
                if ((System.currentTimeMillis() - next.getLeft()) < deletionDelay) {
                    Thread.sleep(deletionDelay);
                }
//
//                var got = jObjectManager.get(next.getRight());
//                if (got.isEmpty()) continue;
//
//                got.get().runWriteLocked(JObject.ResolutionStrategy.LOCAL_ONLY, (m, d, v, i) -> {
//                    if (m.isInvalid()) return;
//                    if (m.getRefcount() > 0) return;
//
//                    Log.info("Deleting " + next.getRight());
//                    jObjectWriteback.remove(m.getName());
//                    objectPersistentStore.deleteObject("meta_" + m.getName());
//                    objectPersistentStore.deleteObject(m.getName());
//
//                    if (d != null) {
//                        for (var c : d.extractRefs()) {
//                            jObjectManager.get(c).ifPresent(ref -> ref.runWriteLocked(JObject.ResolutionStrategy.LOCAL_ONLY, (mc, dc, bc, ic) -> {
//                                mc.removeRef(m.getName());
//                                if (mc.getRefcount() <= 0)
//                                    putDeletionCandidate(mc.getName());
//                                return null;
//                            }));
//                        }
//                    }
//
//                });
            }
        } catch (InterruptedException ignored) {
        }
        Log.info("JObject Refcounter thread exiting");
    }
}
