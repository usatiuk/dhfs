package com.usatiuk.dhfs.invalidation;

import com.usatiuk.dhfs.peersync.PeerId;
import com.usatiuk.dhfs.peersync.PeerInfoService;
import com.usatiuk.dhfs.peersync.PersistentPeerDataService;
import com.usatiuk.dhfs.peersync.ReachablePeerManager;
import com.usatiuk.dhfs.rpc.RemoteObjectServiceClient;
import com.usatiuk.objects.JData;
import com.usatiuk.objects.JObjectKey;
import com.usatiuk.objects.transaction.Transaction;
import com.usatiuk.objects.transaction.TransactionManager;
import com.usatiuk.utils.AutoCloseableNoThrow;
import com.usatiuk.utils.DataLocker;
import com.usatiuk.utils.HashSetDelayedBlockingQueue;
import io.quarkus.logging.Log;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import io.vertx.core.impl.ConcurrentHashSet;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.apache.commons.collections4.multimap.ArrayListValuedHashMap;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Service to handle sending operations to remote peers.
 * This service works with objects, containing a queue of them.
 * The operations to be sent to peers are extracted from the objects in the queue.
 */
@ApplicationScoped
public class InvalidationQueueService {
    private final HashSetDelayedBlockingQueue<InvalidationQueueEntry> _queue;
    private final AtomicReference<ConcurrentHashSet<JObjectKey>> _toAllQueue = new AtomicReference<>(new ConcurrentHashSet<>());
    private final DataLocker _locker = new DataLocker();
    @Inject
    ReachablePeerManager reachablePeerManager;
    @Inject
    DeferredInvalidationQueueService deferredInvalidationQueueService;
    @Inject
    PeerInfoService peerInfoService;
    @Inject
    TransactionManager txm;
    @Inject
    Transaction curTx;
    @ConfigProperty(name = "dhfs.objects.invalidation.threads")
    int threads;
    @Inject
    PersistentPeerDataService persistentPeerDataService;
    @Inject
    RemoteObjectServiceClient remoteObjectServiceClient;
    @Inject
    OpExtractorService opExtractorService;
    private ExecutorService _executor;
    private volatile boolean _shutdown = false;

    public InvalidationQueueService(@ConfigProperty(name = "dhfs.objects.invalidation.delay") int delay) {
        _queue = new HashSetDelayedBlockingQueue<>(delay);
    }

    void init(@Observes @Priority(300) StartupEvent event) throws InterruptedException {
        BasicThreadFactory factory = new BasicThreadFactory.Builder()
                .namingPattern("invalidation-%d")
                .build();

        _executor = Executors.newFixedThreadPool(threads, factory);

        for (int i = 0; i < threads; i++) {
            _executor.submit(this::sender);
        }
    }

    void shutdown(@Observes @Priority(10) ShutdownEvent event) throws InterruptedException {
        _shutdown = true;
        _executor.shutdownNow();
        if (!_executor.awaitTermination(30, TimeUnit.SECONDS)) {
            Log.error("Failed to shut down invalidation sender thread");
        }
        var data = _queue.close();
        Log.info("Will defer " + data.size() + " invalidations on shutdown");
        for (var e : data)
            deferredInvalidationQueueService.defer(e);
    }

    private void sender() {
        while (!_shutdown) {
            try {
                try {
                    if (!_queue.hasImmediate()) {
                        ConcurrentHashSet<JObjectKey> toAllQueue;

                        while (true) {
                            toAllQueue = _toAllQueue.get();
                            if (toAllQueue != null) {
                                if (_toAllQueue.compareAndSet(toAllQueue, null))
                                    break;
                            } else {
                                break;
                            }
                        }

                        if (toAllQueue != null) {
                            var hostInfo = reachablePeerManager.getHostStateSnapshot();
                            for (var o : toAllQueue) {
                                for (var h : hostInfo.available())
                                    _queue.add(new InvalidationQueueEntry(h, o));
                                for (var u : hostInfo.unavailable())
                                    deferredInvalidationQueueService.defer(new InvalidationQueueEntry(u, o));
                            }
                        }
                    }

                    var data = _queue.getAllWait(100, _queue.getDelay()); // TODO: config?
                    if (data.isEmpty()) continue;
                    String stats = "Sent invalidation: ";
                    long success = 0;

                    // Don't try to send same object in multiple threads
                    List<AutoCloseableNoThrow> locks = new LinkedList<>();
                    try {
                        ArrayListValuedHashMap<PeerId, Op> ops = new ArrayListValuedHashMap<>();
                        ArrayListValuedHashMap<PeerId, Runnable> commits = new ArrayListValuedHashMap<>();
                        for (var e : data) {
                            // TODO: Race?
                            if (!peerInfoService.existsPeer(e.peer())) {
                                Log.warnv("Will ignore invalidation of {0} to {1}, peer not found", e.key(), e.peer());
                                continue;
                            }

                            if (!reachablePeerManager.isReachable(e.peer())) {
                                deferredInvalidationQueueService.defer(e);
                                continue;
                            }

                            if (!persistentPeerDataService.isInitialSyncDone(e.peer())) {
                                pushInvalidationToOne(e);
                                continue;
                            }

                            var lock = _locker.tryLock(e);
                            if (lock == null) {
                                pushInvalidationToOne(e);
                                continue;
                            }
                            locks.add(lock);
                            try {
                                txm.run(() -> {
                                    var obj = curTx.get(JData.class, e.key()).orElse(null);
                                    if (obj == null) return;

                                    var extracted = opExtractorService.extractOps(obj, e.peer());
                                    if (extracted == null) return;
                                    ops.get(e.peer()).addAll(extracted.getLeft());
                                    commits.get(e.peer()).add(extracted.getRight());
                                });
                                success++;
                            } catch (Exception ex) {
                                Log.warnv("Failed to prepare invalidation to {0}, will retry: {1}", e, ex);
                                pushInvalidationToOne(e);
                            }
                            if (_shutdown) {
                                Log.info("Invalidation sender exiting");
                                break;
                            }
                        }

                        for (var p : ops.keySet()) {
                            var list = ops.get(p);
                            Log.tracev("Pushing invalidations to {0}: {1}", p, list);
                            remoteObjectServiceClient.pushOps(p, list);
                            commits.get(p).forEach(Runnable::run);
                        }
                    } catch (Exception e) {
                        Log.warn("Failed to send invalidations, will retry", e);
                        for (var inv : data) {
                            pushInvalidationToOne(inv);
                        }
                    } finally {
                        locks.forEach(AutoCloseableNoThrow::close);
                    }

                    stats += success + "/" + data.size() + " ";
                    Log.info(stats);
                } catch (InterruptedException ie) {
                    throw ie;
                } catch (Exception e) {
                    Log.error("Exception in invalidation sender thread: ", e);
                }
            } catch (InterruptedException ignored) {
            }
        }
        Log.info("Invalidation sender exiting");
    }

    /**
     * Extract operations from an object for all peers and push them.
     *
     * @param key the object key to process
     */
    public void pushInvalidationToAll(JObjectKey key) {
        while (true) {
            var queue = _toAllQueue.get();
            if (queue == null) {
                var nq = new ConcurrentHashSet<JObjectKey>();
                if (!_toAllQueue.compareAndSet(null, nq)) continue;
                queue = nq;
            }

            queue.add(key);

            if (_toAllQueue.get() == queue) break;
        }
    }


    void pushInvalidationToOne(InvalidationQueueEntry entry) {
        if (reachablePeerManager.isReachable(entry.peer()))
            _queue.add(entry);
        else
            deferredInvalidationQueueService.defer(entry);
    }

    void pushInvalidationToOneNoDelay(InvalidationQueueEntry entry) {
        if (reachablePeerManager.isReachable(entry.peer()))
            _queue.addNoDelay(entry);
        else
            deferredInvalidationQueueService.defer(entry);
    }

    /**
     * Extract operations from an object for some specific peer and push them.
     *
     * @param host the host to extract operations for
     * @param obj  the object key to process
     */
    public void pushInvalidationToOne(PeerId host, JObjectKey obj) {
        var entry = new InvalidationQueueEntry(host, obj);
        pushInvalidationToOne(entry);
    }

    /**
     * Extract operations from an object for some specific peer and push them, without delay.
     *
     * @param host the host to extract operations for
     * @param obj  the object key to process
     */
    public void pushInvalidationToOneNoDelay(PeerId host, JObjectKey obj) {
        var entry = new InvalidationQueueEntry(host, obj);
        pushInvalidationToOneNoDelay(entry);
    }

    void pushDeferredInvalidations(InvalidationQueueEntry entry) {
        _queue.add(entry);
    }
}
