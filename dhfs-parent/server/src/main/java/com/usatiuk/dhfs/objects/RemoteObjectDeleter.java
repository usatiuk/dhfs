package com.usatiuk.dhfs.objects;

import com.usatiuk.dhfs.objects.repository.RemoteObjectServiceClient;
import com.usatiuk.dhfs.objects.repository.peersync.PeerInfo;
import com.usatiuk.dhfs.objects.repository.peersync.PeerInfoService;
import com.usatiuk.dhfs.objects.transaction.Transaction;
import com.usatiuk.dhfs.utils.HashSetDelayedBlockingQueue;
import io.quarkus.logging.Log;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

@ApplicationScoped
public class RemoteObjectDeleter {
    private final HashSetDelayedBlockingQueue<JObjectKey> _quickCandidates = new HashSetDelayedBlockingQueue<>(0);
    private final HashSetDelayedBlockingQueue<JObjectKey> _candidates;
    private final HashSetDelayedBlockingQueue<JObjectKey> _canDeleteRetries;
    private final HashSet<JObjectKey> _movablesInProcessing = new HashSet<>();

    @Inject
    TransactionManager txm;
    @Inject
    Transaction curTx;
    @Inject
    RemoteTransaction remoteTx;
    @Inject
    PeerInfoService peerInfoService;
    @Inject
    RemoteObjectServiceClient remoteObjectServiceClient;

    @ConfigProperty(name = "dhfs.objects.move-processor.threads")
    int moveProcessorThreads;
    @ConfigProperty(name = "dhfs.objects.ref-processor.threads")
    int refProcessorThreads;
    @ConfigProperty(name = "dhfs.objects.deletion.can-delete-retry-delay")
    long canDeleteRetryDelay;

    private ExecutorService _movableProcessorExecutorService;
    private ExecutorService _refProcessorExecutorService;

    public RemoteObjectDeleter(@ConfigProperty(name = "dhfs.objects.deletion.delay") long deletionDelay,
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

//    public void putQuickDeletionCandidate(JObjectKey obj) {
//        _quickCandidates.add(obj);
//    }

    public void putDeletionCandidate(RemoteObjectMeta obj) {
        if (!obj.seen()) {
            if (_quickCandidates.add(obj.key()))
                Log.debug("Quick deletion candidate: " + obj.key());
            return;
        }
        if (_candidates.add(obj.key()))
            Log.debug("Deletion candidate: " + obj.key());
    }

    private void asyncProcessMovable(JObjectKey objName) {
        synchronized (_movablesInProcessing) {
            if (_movablesInProcessing.contains(objName)) return;
            _movablesInProcessing.add(objName);
        }
        Log.debugv("Async processing of remote obj del: {0}", objName);

        _movableProcessorExecutorService.submit(() -> {
            boolean delay = true;
            try {
                delay = txm.run(() -> {
                    Log.debugv("Starting async processing of remote obj del: {0}", objName);
                    RemoteObjectMeta target = curTx.get(RemoteObjectMeta.class, objName).orElse(null);
                    if (target == null) return true;
                    if (!canDelete(target)) return true;

                    if (canDeleteImmediately(target)) {
                        Log.debugv("Async processing of remote obj del: immediate {0}", objName);
                        curTx.delete(objName);
                        return true;
                    }

                    var knownHosts = peerInfoService.getPeersNoSelf();
                    RemoteObjectMeta finalTarget = target;
                    List<PeerId> missing = knownHosts.stream()
                            .map(PeerInfo::id)
                            .filter(id -> !finalTarget.confirmedDeletes().contains(id)).toList();

                    var ret = remoteObjectServiceClient.canDelete(missing, objName, target.refsFrom());

                    long ok = 0;

                    for (var r : ret) {
                        if (!r.getValue().getDeletionCandidate()) {
//                            for (var rr : r.getReferrersList())
//                                autoSyncProcessor.add(rr);
                        } else {
                            target = target.withConfirmedDeletes(target.confirmedDeletes().plus(r.getKey()));
                            ok++;
                        }
                    }

                    curTx.put(target);

                    if (ok != missing.size()) {
                        Log.debugv("Delaying deletion check of {0}", objName);
                        return true;
                    } else {
                        assert canDeleteImmediately(target);
                        Log.debugv("Async processing of remote obj del: after query {0}", objName);
                        curTx.delete(objName);
                        return false;
                    }
                });
            } finally {
                synchronized (_movablesInProcessing) {
                    _movablesInProcessing.remove(objName);
                    if (!delay)
                        _candidates.add(objName);
                    else
                        _canDeleteRetries.add(objName);
                }
            }
        });
    }

    // FIXME:
    private boolean canDelete(JDataRefcounted obj) {
        return obj.refsFrom().isEmpty() && !obj.frozen();
    }

    // Returns true if the object can be deleted
    private boolean canDeleteImmediately(RemoteObjectMeta obj) {
        if (!obj.seen())
            return true;

        var knownHosts = peerInfoService.getPeersNoSelf();
        boolean missing = false;
        for (var x : knownHosts) {
            if (!obj.confirmedDeletes().contains(x.id())) {
                missing = true;
                break;
            }
        }
        return !missing;
    }

    private void refProcessor() {
        while (true) {
            try {
                while (!Thread.interrupted()) {
                    JObjectKey next = null;
                    JObjectKey nextQuick = null;

                    while (next == null && nextQuick == null) {
                        nextQuick = _quickCandidates.tryGet();

                        if (nextQuick != null) break;

                        next = _canDeleteRetries.tryGet();
                        if (next == null)
                            next = _candidates.tryGet();
                        if (next == null)
                            nextQuick = _quickCandidates.get(canDeleteRetryDelay);
                    }

                    Stream.of(next, nextQuick).filter(Objects::nonNull).forEach(realNext -> {
                        Log.debugv("Processing remote object deletion candidate: {0}", realNext);
                        var deleted = txm.run(() -> {
                            RemoteObjectMeta target = curTx.get(RemoteObjectMeta.class, realNext).orElse(null);
                            if (target == null) return true;

                            if (!canDelete(target)) return true;

                            if (canDeleteImmediately(target)) {
                                Log.debugv("Immediate deletion of: {0}", realNext);
                                curTx.delete(realNext);
                                return true;
                            }

                            return false;
                        });
                        if (!deleted)
                            asyncProcessMovable(realNext);
                    });
                }
            } catch (InterruptedException ignored) {
                return;
            } catch (Throwable error) {
                Log.error("Exception in refcounter thread", error);
            }
            Log.info("JObject Refcounter thread exiting");
        }
    }

}
