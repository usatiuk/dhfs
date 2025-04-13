package com.usatiuk.dhfs.fuse;

import com.usatiuk.dhfs.TempDataProfile;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

class Profiles {
    public static class DhfsFuseTestProfile extends TempDataProfile {
    }
}

@QuarkusTest
@TestProfile(Profiles.DhfsFuseTestProfile.class)
public class DhfsFuseTest {
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

    @Test
    void symlinkTest() throws IOException, InterruptedException {
        byte[] testString = "symlinkedfile".getBytes();
        Path testPath = Path.of(root).resolve("symlinktarget");
        Path testSymlink = Path.of(root).resolve("symlinktest");

        Assertions.assertDoesNotThrow(() -> Files.createFile(testPath));
        Assertions.assertDoesNotThrow(() -> Files.write(testPath, testString));
        Assertions.assertDoesNotThrow(() -> Files.readAllBytes(testPath));
        Assertions.assertArrayEquals(Files.readAllBytes(testPath), testString);

        Assertions.assertDoesNotThrow(() -> Files.createSymbolicLink(testSymlink, testPath));
        Assertions.assertTrue(() -> Files.isSymbolicLink(testSymlink));
        Assertions.assertEquals(testPath, Files.readSymbolicLink(testSymlink));
        Assertions.assertDoesNotThrow(() -> Files.readAllBytes(testSymlink));
        Assertions.assertArrayEquals(Files.readAllBytes(testSymlink), testString);
    }

    @Test
    void dontRemoveEmptyDirTest() throws IOException {
        byte[] testString = "dontRemoveEmptyDirTestStr".getBytes();
        Path testDir = Path.of(root).resolve("dontRemoveEmptyDirTestDir");
        Path testFile = testDir.resolve("dontRemoveEmptyDirTestFile");

        Assertions.assertDoesNotThrow(() -> Files.createDirectory(testDir));
        Assertions.assertDoesNotThrow(() -> Files.createFile(testFile));
        Assertions.assertDoesNotThrow(() -> Files.write(testFile, testString));
        Assertions.assertDoesNotThrow(() -> Files.readAllBytes(testFile));
        Assertions.assertArrayEquals(Files.readAllBytes(testFile), testString);

        Assertions.assertThrows(Exception.class, () -> Files.delete(testDir));
        Assertions.assertDoesNotThrow(() -> Files.readAllBytes(testFile));
        Assertions.assertArrayEquals(Files.readAllBytes(testFile), testString);

        Assertions.assertDoesNotThrow(() -> Files.delete(testFile));
        Assertions.assertDoesNotThrow(() -> Files.delete(testDir));
        Assertions.assertFalse(Files.exists(testDir));
        Assertions.assertFalse(Files.exists(testFile));
        Assertions.assertThrows(Exception.class, () -> Files.readAllBytes(testFile));
    }

}
