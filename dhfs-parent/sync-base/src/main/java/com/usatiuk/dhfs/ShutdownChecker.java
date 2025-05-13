package com.usatiuk.dhfs;

import io.quarkus.logging.Log;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.io.IOException;
import java.nio.file.Paths;

/**
 * This class checks if the application was shut down cleanly.
 * It creates a file in the specified directory on startup and deletes it on shutdown.
 * If the file exists on startup, it means the application was not shut down cleanly.
 */
@ApplicationScoped
public class ShutdownChecker {
    private static final String dataFileName = "running";
    @ConfigProperty(name = "dhfs.objects.persistence.files.root")
    String dataRoot;
    boolean _cleanShutdown = true;
    boolean _initialized = false;

    void init(@Observes @Priority(2) StartupEvent event) throws IOException {
        Paths.get(dataRoot).toFile().mkdirs();

        if (!Paths.get(dataRoot).toFile().isDirectory())
            throw new IllegalStateException("Could not create directory " + dataRoot);

        Log.info("Initializing with root " + dataRoot);
        if (Paths.get(dataRoot).resolve(dataFileName).toFile().exists()) {
            _cleanShutdown = false;
            Log.error("Unclean shutdown detected!");
        } else {
            Paths.get(dataRoot).resolve(dataFileName).toFile().createNewFile();
        }
        _initialized = true;
    }

    void shutdown(@Observes @Priority(100000) ShutdownEvent event) throws IOException {
        Paths.get(dataRoot).resolve(dataFileName).toFile().delete();
    }

    public boolean lastShutdownClean() {
        if (!_initialized) throw new IllegalStateException("ShutdownChecker not initialized");
        return _cleanShutdown;
    }
}
