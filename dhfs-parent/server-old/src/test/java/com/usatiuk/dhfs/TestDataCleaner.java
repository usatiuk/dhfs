package com.usatiuk.dhfs;

import io.quarkus.logging.Log;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Objects;

@ApplicationScoped
public class TestDataCleaner {
    @ConfigProperty(name = "dhfs.objects.persistence.files.root")
    String tempDirectory;
    @ConfigProperty(name = "dhfs.objects.root")
    String tempDirectoryIdx;

    void init(@Observes @Priority(1) StartupEvent event) throws IOException {
        try {
            purgeDirectory(Path.of(tempDirectory).toFile());
            purgeDirectory(Path.of(tempDirectoryIdx).toFile());
        } catch (Exception ignored) {
            Log.warn("Couldn't cleanup test data on init");
        }
    }

    void shutdown(@Observes @Priority(1000000000) ShutdownEvent event) throws IOException {
        purgeDirectory(Path.of(tempDirectory).toFile());
        purgeDirectory(Path.of(tempDirectoryIdx).toFile());
    }

    void purgeDirectory(File dir) {
        for (File file : Objects.requireNonNull(dir.listFiles())) {
            if (file.isDirectory())
                purgeDirectory(file);
            file.delete();
        }
    }
}
