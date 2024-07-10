package com.usatiuk.dhfs.objects.jrepository;

import com.google.common.collect.Streams;
import com.usatiuk.dhfs.objects.repository.PersistentRemoteHostsService;
import com.usatiuk.dhfs.objects.repository.RemoteObjectServiceClient;
import com.usatiuk.dhfs.objects.repository.autosync.AutoSyncProcessor;
import com.usatiuk.utils.HashSetDelayedBlockingQueue;
import io.quarkus.logging.Log;
import io.quarkus.runtime.Shutdown;
import io.quarkus.runtime.Startup;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

@ApplicationScoped
public class JObjectRefProcessor {
    private final HashSetDelayedBlockingQueue<String> _candidates;
    private final HashSet<String> _movablesInProcessing = new HashSet<>();
    @Inject
    JObjectManager jObjectManager;
    @Inject
    PersistentRemoteHostsService persistentRemoteHostsService;
    @Inject
    RemoteObjectServiceClient remoteObjectServiceClient;
    @Inject
    AutoSyncProcessor autoSyncProcessor;
    @ConfigProperty(name = "dhfs.objects.move-processor.threads")
    int moveProcessorThreads;
    ExecutorService _movableProcessorExecutorService;
    private Thread _refProcessorThread;

    public JObjectRefProcessor(@ConfigProperty(name = "dhfs.objects.deletion.delay") long deletionDelay) {
        _candidates = new HashSetDelayedBlockingQueue<>(deletionDelay);
    }

    @Startup
    void init() {
        _movableProcessorExecutorService = Executors.newFixedThreadPool(moveProcessorThreads);

        _refProcessorThread = new Thread(this::refProcessorThread);
        _refProcessorThread.setName("JObject Refcounter thread");
        _refProcessorThread.start();
    }

    @Shutdown
    void shutdown() throws InterruptedException {
        _refProcessorThread.interrupt();
        _refProcessorThread.join();
    }

    public void putDeletionCandidate(String name) {
        synchronized (_movablesInProcessing) {
            if (_movablesInProcessing.contains(name)) return;
            if (_candidates.add(name))
                Log.debug("Deletion candidate: " + name);
        }
    }

    private void asyncProcessMovable(JObject<?> obj) {
        synchronized (_movablesInProcessing) {
            if (_movablesInProcessing.contains(obj.getName())) return;
            _movablesInProcessing.add(obj.getName());
        }

        _movableProcessorExecutorService.submit(() -> {
            try {
                var knownHosts = persistentRemoteHostsService.getHostsUuid();
                List<UUID> missing = new ArrayList<>();

                var ourReferrers = obj.runReadLocked(JObject.ResolutionStrategy.NO_RESOLUTION, (m, d) -> {
                    for (var x : knownHosts)
                        if (!m.getConfirmedDeletes().contains(x)) missing.add(x);
                    return m.getReferrers();
                });
                var ret = remoteObjectServiceClient.canDelete(missing, obj.getName(), ourReferrers);

                for (var r : ret) {
                    if (!r.getDeletionCandidate())
                        for (var rr : r.getReferrersList())
                            autoSyncProcessor.add(rr);
                }

                obj.runWriteLocked(JObject.ResolutionStrategy.NO_RESOLUTION, (m, d, b, i) -> {
                    for (var r : ret)
                        if (r.getDeletionCandidate())
                            m.getConfirmedDeletes().add(UUID.fromString(r.getSelfUuid()));
                    return null;
                });
            } catch (Exception e) {
                Log.error("When processing deletion of movable object " + obj.getName(), e);
            } finally {
                synchronized (_movablesInProcessing) {
                    _movablesInProcessing.remove(obj.getName());
                    putDeletionCandidate(obj.getName());
                }
            }
        });
    }

    private boolean processMovable(JObject<?> obj) {
        obj.assertRWLock();
        var knownHosts = persistentRemoteHostsService.getHostsUuid();
        boolean missing = false;
        for (var x : knownHosts)
            if (!obj.getMeta().getConfirmedDeletes().contains(x)) {
                missing = true;
                break;
            }

        if (!missing) return true;
        asyncProcessMovable(obj);
        return false;
    }

    private void refProcessorThread() {
        try {
            while (!Thread.interrupted()) {
                String next = _candidates.get();

                var got = jObjectManager.get(next);
                if (got.isEmpty()) continue;
                try {
                    got.get().runWriteLocked(JObject.ResolutionStrategy.NO_RESOLUTION, (m, d, v, i) -> {
                        if (m.isLocked()) return null;
                        if (m.isDeleted()) return null;
                        if (!m.isDeletionCandidate()) return null;
                        if (m.isSeen() && m.getKnownClass().isAnnotationPresent(Movable.class)) {
                            if (!processMovable(got.get()))
                                return null;
                        }

                        got.get().tryResolve(JObject.ResolutionStrategy.LOCAL_ONLY);

                        Log.trace("Deleting " + m.getName());
                        m.delete();

                        Stream<String> refs = Stream.empty();

                        if (m.getSavedRefs() != null)
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
