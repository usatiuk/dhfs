package com.usatiuk.dhfs.objects.repository.invalidation;

import com.usatiuk.dhfs.SerializationHelper;
import com.usatiuk.dhfs.objects.repository.PeerManager;
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

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

@ApplicationScoped
public class DeferredInvalidationQueueService {
    private static final String dataFileName = "invqueue";
    @Inject
    PeerManager remoteHostManager;
    @Inject
    InvalidationQueueService invalidationQueueService;
    @ConfigProperty(name = "dhfs.objects.root")
    String dataRoot;
    // FIXME: DB when?
    private DeferredInvalidationQueueData _persistentData = new DeferredInvalidationQueueData();

    void init(@Observes @Priority(300) StartupEvent event) throws IOException {
        Paths.get(dataRoot).toFile().mkdirs();
        Log.info("Initializing with root " + dataRoot);
        if (Paths.get(dataRoot).resolve(dataFileName).toFile().exists()) {
            Log.info("Reading invalidation queue");
            _persistentData = SerializationHelper.deserialize(Files.readAllBytes(Paths.get(dataRoot).resolve(dataFileName)));
        } else if (Paths.get(dataRoot).resolve(dataFileName + ".bak").toFile().exists()) {
            Log.warn("Reading invalidation queue from backup");
            _persistentData = SerializationHelper.deserialize(Files.readAllBytes(Paths.get(dataRoot).resolve(dataFileName)));
        }

        remoteHostManager.registerConnectEventListener(this::returnForHost);
    }

    void shutdown(@Observes @Priority(300) ShutdownEvent event) throws IOException {
        Log.info("Saving deferred invalidations");
        writeData();
        Log.info("Saved deferred invalidations");
    }


    private void writeData() {
        try {
            if (Paths.get(dataRoot).resolve(dataFileName).toFile().exists())
                Files.move(Paths.get(dataRoot).resolve(dataFileName), Paths.get(dataRoot).resolve(dataFileName + ".bak"), REPLACE_EXISTING);
            Files.write(Paths.get(dataRoot).resolve(dataFileName), SerializationUtils.serialize(_persistentData));
        } catch (IOException iex) {
            Log.error("Error writing deferred invalidations data", iex);
            throw new RuntimeException(iex);
        }
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
