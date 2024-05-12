package com.usatiuk.dhfs.storage;

import io.quarkus.test.junit.QuarkusTest;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.io.File;
import java.nio.file.Path;
import java.util.Objects;

@QuarkusTest
public abstract class SimpleFileRepoTest {
    @ConfigProperty(name = "dhfs.filerepo.root")
    String tempDirectory;

    void purgeDirectory(File dir) {
        for (File file : Objects.requireNonNull(dir.listFiles())) {
            if (file.isDirectory())
                purgeDirectory(file);
            file.delete();
        }
    }

    @BeforeEach
    void setup() {
        purgeDirectory(Path.of(tempDirectory).toFile());
    }

    @AfterEach
    void teardown() {
        purgeDirectory(Path.of(tempDirectory).toFile());
    }
}
