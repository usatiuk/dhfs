package com.usatiuk.dhfs.storage;

import io.quarkus.test.junit.QuarkusTest;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;

@QuarkusTest
public abstract class SimpleFileRepoTest {
    @ConfigProperty(name = "dhfs.filerepo.root")
    String tempDirectory;

    @BeforeEach
    void setup() {

    }

    @AfterAll
    static void teardown() {

    }
}
