package com.usatiuk.dhfs.objects.jrepository;

import com.usatiuk.dhfs.objects.repository.PersistentPeerDataService;
import com.usatiuk.dhfs.objects.repository.RemoteObjectServiceClient;
import com.usatiuk.dhfs.objects.repository.autosync.AutoSyncProcessor;
import com.usatiuk.utils.HashSetDelayedBlockingQueue;
import io.quarkus.logging.Log;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.commons.lang3.tuple.Pair;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@ApplicationScoped
public class JObjectRefProcessor {
    private final HashSetDelayedBlockingQueue<Pair<SoftJObject<?>, SoftJObject<?>>> _quickCandidates = new HashSetDelayedBlockingQueue<>(0);
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
    @Inject
    JObjectTxManager jObjectTxManager;
    @ConfigProperty(name = "dhfs.objects.move-processor.threads")
    int moveProcessorThreads;
    @ConfigProperty(name = "dhfs.objects.ref-processor.threads")
    int refProcessorThreads;
    @ConfigProperty(name = "dhfs.objects.deletion.can-delete-retry-delay")
    long canDeleteRetryDelay;
    @Inject
    ExecutorService executorService;

    private ExecutorService _movableProcessorExecutorService;
    private ExecutorService _refProcessorExecutorService;

    public JObjectRefProcessor(@ConfigProperty(name = "dhfs.objects.deletion.delay") long deletionDelay,
                               @ConfigProperty(name = "dhfs.objects.deletion.can-delete-retry-delay") long canDeleteRetryDelay) {
        _candidates = new HashSetDelayedBlockingQueue<>(deletionDelay);
        _canDeleteRetries = new HashSetDelayedBlockingQueue<>(canDeleteRetryDelay);
    }

    void init(@Observes @Priority(200) StartupEvent event) throws IOException {
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
        //FIXME
//        executorService.submit(() ->
//                jObjectManager.findAll().forEach(n -> {
//                    jObjectManager.get(n).ifPresent(o -> o.runWriteLocked(JObjectManager.ResolutionStrategy.NO_RESOLUTION, (m, d, b, v) -> {
//                        return null;
//                    }));
//                }));
    }

    void shutdown(@Observes @Priority(800) ShutdownEvent event) throws InterruptedException {
        _refProcessorExecutorService.shutdownNow();
        if (!_refProcessorExecutorService.awaitTermination(30, TimeUnit.SECONDS)) {
            Log.error("Refcounting threads didn't exit in 30 seconds");
        }
    }

    public void putQuickDeletionCandidate(SoftJObject<?> from, SoftJObject<?> obj) {
        _quickCandidates.add(Pair.of(from, obj));
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
            if (obj == null || obj.getMeta().isDeleted()) return;
            boolean delay = false;
            try {
                var knownHosts = persistentPeerDataService.getHostUuids();
                List<UUID> missing = new ArrayList<>();

                var ourReferrers = obj.runReadLocked(JObjectManager.ResolutionStrategy.NO_RESOLUTION, (m, d) -> {
                    for (var x : knownHosts)
                        if (!m.getConfirmedDeletes().contains(x)) missing.add(x);
                    return m.getReferrers();
                });
                var ret = remoteObjectServiceClient.canDelete(missing, obj.getMeta().getName(), ourReferrers);

                long ok = 0;

                for (var r : ret) {
                    if (!r.getDeletionCandidate())
                        for (var rr : r.getReferrersList())
                            autoSyncProcessor.add(rr);
                    else
                        ok++;
                }

                if (ok != missing.size()) {
                    Log.debug("Delaying deletion check of " + obj.getMeta().getName());
                    delay = true;
                }

                jObjectTxManager.executeTx(() -> {
                    obj.runWriteLocked(JObjectManager.ResolutionStrategy.NO_RESOLUTION, (m, d, b, i) -> {
                        for (var r : ret)
                            if (r.getDeletionCandidate())
                                m.getConfirmedDeletes().add(UUID.fromString(r.getSelfUuid()));
                        return null;
                    });
                });
            } catch (Exception e) {
                Log.warn("When processing deletion of movable object " + obj.getMeta().getName(), e);
            } finally {
                synchronized (_movablesInProcessing) {
                    _movablesInProcessing.remove(obj.getMeta().getName());
                    if (!delay)
                        _candidates.add(obj.getMeta().getName());
                    else
                        _canDeleteRetries.add(obj.getMeta().getName());
                }
            }
        });
    }

    private boolean processMovable(JObject<?> obj) {
        obj.assertRwLock();
        var knownHosts = persistentPeerDataService.getHostUuids();
        boolean missing = false;
        for (var x : knownHosts)
            if (!obj.getMeta().getConfirmedDeletes().contains(x)) {
                missing = true;
                break;
            }

        if (!missing) return true;
        asyncProcessMovable(obj.getMeta().getName());
        return false;
    }

    private void deleteRef(JObject<?> self, String name) {
        jObjectManager.get(name).ifPresent(ref -> ref.runWriteLocked(JObjectManager.ResolutionStrategy.NO_RESOLUTION, (mc, dc, bc, ic) -> {
            mc.removeRef(self.getMeta().getName());
            return null;
        }));
    }

    private void refProcessor() {
        try {
            while (!Thread.interrupted()) {
                String next = null;
                Pair<SoftJObject<?>, SoftJObject<?>> nextQuick = null;

                while (next == null && nextQuick == null) {
                    nextQuick = _quickCandidates.tryGet();

                    if (nextQuick != null) break;

                    next = _canDeleteRetries.tryGet();
                    if (next == null)
                        next = _candidates.tryGet();
                    if (next == null)
                        nextQuick = _quickCandidates.get(canDeleteRetryDelay);
                }

                JObject<?> target;

                if (nextQuick != null) {
                    var fromSoft = nextQuick.getLeft();
                    var toSoft = nextQuick.getRight();

                    var from = nextQuick.getLeft().get();
                    var to = nextQuick.getRight().get();

                    if (from != null && !from.getMeta().isDeleted()) {
                        Log.warn("Quick delete failed for " + from.getMeta().getName() + " -> " + toSoft.getName());
                        continue;
                    }

                    if (to == null) {
                        Log.warn("Quick delete object missing: " + toSoft.getName());
                        continue;
                    }

                    target = to;

                    jObjectTxManager.executeTx(() -> {
                        if (from != null)
                            from.rwLock();
                        try {
                            try {
                                to.rwLock();
                                to.getMeta().removeRef(fromSoft.getName());
                            } finally {
                                to.rwUnlock();
                            }
                        } finally {
                            if (from != null)
                                from.rwUnlock();
                        }
                    });
                } else {
                    target = jObjectManager.get(next).orElse(null);
                }

                if (target == null) continue;
                try {
                    jObjectTxManager.executeTx(() -> {
                        target.runWriteLocked(JObjectManager.ResolutionStrategy.NO_RESOLUTION, (m, d, v, i) -> {
                            if (m.isLocked()) return null;
                            if (m.isDeleted()) return null;
                            if (!m.isDeletionCandidate()) return null;
                            if (m.isSeen() && !m.isOnlyLocal()) {
                                if (!processMovable(target))
                                    return null;
                            }
                            if (m.isSeen() && m.isOnlyLocal())
                                Log.warn("Seen only-local object: " + m.getName());


                            if (!target.getMeta().getKnownClass().isAnnotationPresent(Leaf.class))
                                target.tryResolve(JObjectManager.ResolutionStrategy.LOCAL_ONLY);

                            Log.debug("Deleting " + m.getName());

                            Collection<String> extracted = null;
                            if (!target.getMeta().getKnownClass().isAnnotationPresent(Leaf.class) && target.getData() != null)
                                extracted = target.getData().extractRefs();
                            Collection<String> saved = target.getMeta().getSavedRefs();

                            target.doDelete();

                            if (saved != null)
                                for (var r : saved) deleteRef(target, r);
                            if (extracted != null)
                                for (var r : extracted) deleteRef(target, r);

                            return null;
                        });
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
