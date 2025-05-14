package com.usatiuk.dhfs.invalidation;

import com.usatiuk.dhfs.peersync.PeerConnectedEventListener;
import com.usatiuk.dhfs.peersync.PeerId;
import com.usatiuk.dhfs.peersync.ReachablePeerManager;
import com.usatiuk.utils.SerializationHelper;
import io.quarkus.logging.Log;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import io.quarkus.scheduler.Scheduled;
import io.smallrye.common.annotation.Blocking;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.apache.commons.lang3.SerializationUtils;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * Service to handle deferred invalidations.
 * It is responsible for storing and returning deferred invalidations to the invalidation queue.
 * It also is responsible for persisting and restoring the deferred invalidations on startup and shutdown.
 */
@ApplicationScoped
public class DeferredInvalidationQueueService implements PeerConnectedEventListener {
    private static final String dataFileName = "invqueue";
    @Inject
    ReachablePeerManager reachablePeerManager;
    @Inject
    InvalidationQueueService invalidationQueueService;
    @ConfigProperty(name = "dhfs.objects.persistence.files.root")
    String dataRoot;
    private DeferredInvalidationQueueData _persistentData = new DeferredInvalidationQueueData();

    void init(@Observes @Priority(290) StartupEvent event) throws IOException {
        Paths.get(dataRoot).toFile().mkdirs();
        Log.info("Initializing with root " + dataRoot);
        if (Paths.get(dataRoot).resolve(dataFileName).toFile().exists()) {
            Log.info("Reading invalidation queue");
            try {
                _persistentData = SerializationHelper.deserialize(Files.readAllBytes(Paths.get(dataRoot).resolve(dataFileName)));
            } catch (Exception e) {
                Log.error("Error reading invalidation queue", e);
            }
        }
    }

    void shutdown(@Observes @Priority(300) ShutdownEvent event) throws IOException {
        Log.info("Saving deferred invalidations");
        writeData();
        Log.info("Saved deferred invalidations");
    }

    private void writeData() {
        try {
            Files.write(Paths.get(dataRoot).resolve(dataFileName), SerializationUtils.serialize(_persistentData));
        } catch (IOException iex) {
            Log.error("Error writing deferred invalidations data", iex);
            throw new RuntimeException(iex);
        }
    }

    /**
     * Periodically returns deferred invalidations to the invalidation queue for all reachable hosts.
     */
    @Scheduled(every = "15s", concurrentExecution = Scheduled.ConcurrentExecution.SKIP, skipExecutionIf = Scheduled.ApplicationNotRunning.class)
    @Blocking
    void periodicReturn() {
        for (var reachable : reachablePeerManager.getAvailableHosts())
            returnForHost(reachable);
    }

    /**
     * Returns deferred invalidations for a specific host.
     *
     * @param host the host to return deferred invalidations for
     */
    void returnForHost(PeerId host) {
        synchronized (this) {
            var col = _persistentData.deferredInvalidations.get(host);
            for (var s : col) {
                Log.tracev("Returning deferred invalidation: {0}", s);
                invalidationQueueService.pushDeferredInvalidations(s);
            }
            col.clear();
        }
    }

    /**
     * Defer a specific invalidation.
     * @param entry the invalidation to defer
     */
    void defer(InvalidationQueueEntry entry) {
        synchronized (this) {
            Log.tracev("Deferred invalidation: {0}", entry);
            _persistentData.deferredInvalidations.put(entry.peer(), entry);
        }
    }

    @Override
    public void handlePeerConnected(PeerId peerId) {
        returnForHost(peerId);
    }
}
