package com.usatiuk.dhfs.objects.repository.invalidation;

import com.usatiuk.dhfs.objects.JObjectKey;
import com.usatiuk.dhfs.objects.PeerId;
import com.usatiuk.dhfs.objects.repository.PeerManager;
import com.usatiuk.dhfs.utils.SerializationHelper;
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
import java.util.UUID;

@ApplicationScoped
public class DeferredInvalidationQueueService {
    private static final String dataFileName = "invqueue";
    @Inject
    PeerManager remoteHostManager;
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
            _persistentData = SerializationHelper.deserialize(Files.readAllBytes(Paths.get(dataRoot).resolve(dataFileName)));
        }
//        remoteHostManager.registerConnectEventListener(this::returnForHost);
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

    // FIXME:
    @Scheduled(every = "15s", concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
    @Blocking
    void periodicReturn() {
        for (var reachable : remoteHostManager.getAvailableHosts())
            returnForHost(reachable);
    }

    void returnForHost(PeerId host) {
        synchronized (this) {
            var col = _persistentData.deferredInvalidations.get(host);
            for (var s : col) {
                Log.trace("Un-deferred invalidation to " + host + " of " + s);
                invalidationQueueService.pushDeferredInvalidations(host, s);
            }
            col.clear();
        }
    }

    void defer(PeerId host, JObjectKey object) {
        synchronized (this) {
            Log.trace("Deferred invalidation to " + host + " of " + object);
            _persistentData.deferredInvalidations.put(host, object);
        }
    }
}
