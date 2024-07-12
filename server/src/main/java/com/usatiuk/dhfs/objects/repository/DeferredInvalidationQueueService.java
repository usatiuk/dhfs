package com.usatiuk.dhfs.objects.repository;

import com.usatiuk.dhfs.SerializationHelper;
import io.quarkus.logging.Log;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
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
    @Inject
    RemoteHostManager remoteHostManager;
    @Inject
    InvalidationQueueService invalidationQueueService;
    @ConfigProperty(name = "dhfs.objects.root")
    String dataRoot;
    private static final String dataFileName = "hosts";

    // FIXME: DB when?
    private DeferredInvalidationQueueData _persistentData = new DeferredInvalidationQueueData();

    void init(@Observes @Priority(300) StartupEvent event) throws IOException {
        Paths.get(dataRoot).toFile().mkdirs();
        Log.info("Initializing with root " + dataRoot);
        if (Paths.get(dataRoot).resolve(dataFileName).toFile().exists()) {
            Log.info("Reading hosts");
            _persistentData = SerializationHelper.deserialize(Files.readAllBytes(Paths.get(dataRoot).resolve(dataFileName)));
        }

        remoteHostManager.registerConnectEventListener(this::returnForHost);
    }

    void shutdown(@Observes @Priority(300) ShutdownEvent event) throws IOException {
        Log.info("Saving deferred invalidations");
        Files.write(Paths.get(dataRoot).resolve(dataFileName), SerializationUtils.serialize(_persistentData));
        Log.info("Saved deferred invalidations");
    }

    void returnForHost(UUID host) {
        synchronized (this) {
            var col = _persistentData.getDeferredInvalidations().get(host);
            for (var s : col) {
                Log.trace("Un-deferred invalidation to " + host + " of " + s);
                invalidationQueueService.pushDeferredInvalidations(host, s);
            }
            col.clear();
        }
    }

    void defer(UUID host, String object) {
        synchronized (this) {
            Log.trace("Deferred invalidation to " + host + " of " + object);
            _persistentData.getDeferredInvalidations().put(host, object);
        }
    }
}
