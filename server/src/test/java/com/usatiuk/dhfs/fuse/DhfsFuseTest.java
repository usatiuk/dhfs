package com.usatiuk.dhfs.fuse;

import com.usatiuk.dhfs.storage.SimpleFileRepoTest;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

class Profiles {
    public static class DhfsFuseTestProfile implements QuarkusTestProfile {
    }
}

@QuarkusTest
@TestProfile(Profiles.DhfsFuseTestProfile.class)
public class DhfsFuseTest extends SimpleFileRepoTest {
    @ConfigProperty(name = "dhfs.fuse.root")
    String root;

    @Test
    void readWriteFileTest() throws IOException, InterruptedException {
        byte[] testString = "test file thing".getBytes();
        Path testPath = Path.of(root).resolve("test1");

        Assertions.assertDoesNotThrow(() -> Files.createFile(testPath));
        Assertions.assertDoesNotThrow(() -> Files.write(testPath, testString));
        Assertions.assertDoesNotThrow(() -> Files.readAllBytes(testPath));
        Assertions.assertArrayEquals(Files.readAllBytes(testPath), testString);
    }
}
