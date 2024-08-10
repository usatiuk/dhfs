package com.usatiuk.dhfs.objects.jrepository;

import com.usatiuk.dhfs.objects.repository.PersistentPeerDataService;
import com.usatiuk.dhfs.objects.repository.RemoteObjectServiceClient;
import com.usatiuk.dhfs.objects.repository.autosync.AutoSyncProcessor;
import com.usatiuk.utils.HashSetDelayedBlockingQueue;
import io.quarkus.logging.Log;
import io.quarkus.runtime.Shutdown;
import io.quarkus.runtime.Startup;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@ApplicationScoped
public class JObjectRefProcessor {
    private final HashSetDelayedBlockingQueue<String> _candidates;
    private final HashSetDelayedBlockingQueue<String> _canDeleteRetries;
    private final HashSet<String> _movablesInProcessing = new HashSet<>();
    @Inject
    JObjectManager jObjectManager;
    @Inject
    PersistentPeerDataService persistentPeerDataService;
    @Inject
    RemoteObjectServiceClient remoteObjectServiceClient;
    @Inject
    AutoSyncProcessor autoSyncProcessor;
    @ConfigProperty(name = "dhfs.objects.move-processor.threads")
    int moveProcessorThreads;
    @ConfigProperty(name = "dhfs.objects.ref-processor.threads")
    int refProcessorThreads;
    @ConfigProperty(name = "dhfs.objects.deletion.can-delete-retry-delay")
    long canDeleteRetryDelay;
    private ExecutorService _movableProcessorExecutorService;
    private ExecutorService _refProcessorExecutorService;

    @Inject
    ExecutorService executorService;

    public JObjectRefProcessor(@ConfigProperty(name = "dhfs.objects.deletion.delay") long deletionDelay,
                               @ConfigProperty(name = "dhfs.objects.deletion.can-delete-retry-delay") long canDeleteRetryDelay) {
        _candidates = new HashSetDelayedBlockingQueue<>(deletionDelay);
        _canDeleteRetries = new HashSetDelayedBlockingQueue<>(canDeleteRetryDelay);
    }

    @Startup
    void init() {
        BasicThreadFactory factory = new BasicThreadFactory.Builder()
                .namingPattern("move-proc-%d")
                .build();
        _movableProcessorExecutorService = Executors.newFixedThreadPool(moveProcessorThreads, factory);

        BasicThreadFactory factoryRef = new BasicThreadFactory.Builder()
                .namingPattern("ref-proc-%d")
                .build();
        _refProcessorExecutorService = Executors.newFixedThreadPool(refProcessorThreads, factoryRef);
        for (int i = 0; i < refProcessorThreads; i++) {
            _refProcessorExecutorService.submit(this::refProcessor);
        }

        // Continue GC from last shutdown
        executorService.submit(() ->
                jObjectManager.findAll().forEach(n -> {
                    jObjectManager.get(n).ifPresent(o -> o.runWriteLocked(JObject.ResolutionStrategy.NO_RESOLUTION, (m, d, b, v) -> {
                        return null;
                    }));
                }));
    }

    @Shutdown
    void shutdown() throws InterruptedException {
        _refProcessorExecutorService.shutdownNow();
        if (!_refProcessorExecutorService.awaitTermination(30, TimeUnit.SECONDS)) {
            Log.error("Refcounting threads didn't exit in 30 seconds");
        }
    }

    public void putDeletionCandidate(String name) {
        synchronized (_movablesInProcessing) {
            if (_movablesInProcessing.contains(name)) return;
            if (_candidates.add(name))
                Log.debug("Deletion candidate: " + name);
        }
    }

    private void asyncProcessMovable(String objName) {
        synchronized (_movablesInProcessing) {
            if (_movablesInProcessing.contains(objName)) return;
            _movablesInProcessing.add(objName);
        }

        _movableProcessorExecutorService.submit(() -> {
            var obj = jObjectManager.get(objName).orElse(null);
            if (obj == null || obj.isDeleted()) return;
            boolean delay = false;
            try {
                var knownHosts = persistentPeerDataService.getHostUuids();
                List<UUID> missing = new ArrayList<>();

                var ourReferrers = obj.runReadLocked(JObject.ResolutionStrategy.NO_RESOLUTION, (m, d) -> {
                    for (var x : knownHosts)
                        if (!m.getConfirmedDeletes().contains(x)) missing.add(x);
                    return m.getReferrers();
                });
                var ret = remoteObjectServiceClient.canDelete(missing, obj.getName(), ourReferrers);

                long ok = 0;

                for (var r : ret) {
                    if (!r.getDeletionCandidate())
                        for (var rr : r.getReferrersList())
                            autoSyncProcessor.add(rr);
                    else
                        ok++;
                }

                if (ok != missing.size()) {
                    Log.debug("Delaying deletion check of " + obj.getName());
                    delay = true;
                }

                obj.runWriteLocked(JObject.ResolutionStrategy.NO_RESOLUTION, (m, d, b, i) -> {
                    for (var r : ret)
                        if (r.getDeletionCandidate())
                            m.getConfirmedDeletes().add(UUID.fromString(r.getSelfUuid()));
                    return null;
                });
            } catch (Exception e) {
                Log.warn("When processing deletion of movable object " + obj.getName(), e);
            } finally {
                synchronized (_movablesInProcessing) {
                    _movablesInProcessing.remove(obj.getName());
                    if (!delay)
                        _candidates.add(obj.getName());
                    else
                        _canDeleteRetries.add(obj.getName());
                }
            }
        });
    }

    private boolean processMovable(JObject<?> obj) {
        obj.assertRWLock();
        var knownHosts = persistentPeerDataService.getHostUuids();
        boolean missing = false;
        for (var x : knownHosts)
            if (!obj.getMeta().getConfirmedDeletes().contains(x)) {
                missing = true;
                break;
            }

        if (!missing) return true;
        asyncProcessMovable(obj.getName());
        return false;
    }

    private void deleteRef(JObject<?> self, String name) {
        jObjectManager.get(name).ifPresent(ref -> ref.runWriteLocked(JObject.ResolutionStrategy.NO_RESOLUTION, (mc, dc, bc, ic) -> {
            mc.removeRef(self.getName());
            return null;
        }));
    }

    private void refProcessor() {
        try {
            while (!Thread.interrupted()) {
                String next = null;

                while (next == null) {
                    next = _candidates.tryGet();
                    if (next == null)
                        next = _canDeleteRetries.tryGet();
                    if (next == null)
                        next = _candidates.get(canDeleteRetryDelay);
                }

                var got = jObjectManager.get(next).orElse(null);
                if (got == null) continue;
                try {
                    got.runWriteLocked(JObject.ResolutionStrategy.NO_RESOLUTION, (m, d, v, i) -> {
                        if (m.isLocked()) return null;
                        if (m.isDeleted()) return null;
                        if (!m.isDeletionCandidate()) return null;
                        if (m.isSeen()) {
                            if (!processMovable(got))
                                return null;
                        }

                        if (!got.getKnownClass().isAnnotationPresent(Leaf.class))
                            got.tryResolve(JObject.ResolutionStrategy.LOCAL_ONLY);

                        Log.debug("Deleting " + m.getName());
                        m.markDeleted();

                        Collection<String> extracted = null;
                        if (!got.getKnownClass().isAnnotationPresent(Leaf.class) && got.getData() != null)
                            extracted = got.getData().extractRefs();
                        Collection<String> saved = got.getMeta().getSavedRefs();

                        got.discardData();

                        if (saved != null)
                            for (var r : saved) deleteRef(got, r);
                        if (extracted != null)
                            for (var r : extracted) deleteRef(got, r);

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
