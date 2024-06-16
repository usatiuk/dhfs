package com.usatiuk.dhfs.files;

import com.usatiuk.dhfs.storage.files.objects.Chunk;
import com.usatiuk.dhfs.storage.files.objects.File;
import com.usatiuk.dhfs.storage.files.service.DhfsFileService;
import com.usatiuk.dhfs.storage.objects.jrepository.JObjectRepository;
import com.usatiuk.dhfs.storage.objects.repository.ObjectRepository;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.UUID;

class Profiles {
    public static class DhfsFileServiceSimpleTestProfile implements QuarkusTestProfile {
    }
}


@QuarkusTest
@TestProfile(Profiles.DhfsFileServiceSimpleTestProfile.class)
public class DhfsFileServiceSimpleTest {
    @Inject
    DhfsFileService fileService;
    @Inject
    ObjectRepository objectRepository;
    @Inject
    JObjectRepository jObjectRepository;

    @Test
    void readTest() {
        var fuuid = UUID.randomUUID();
        {
            Chunk c1 = new Chunk("12345".getBytes());
            Chunk c2 = new Chunk("678".getBytes());
            Chunk c3 = new Chunk("91011".getBytes());
            File f = new File(fuuid);
            Assertions.assertDoesNotThrow(() -> f.runWriteLocked((fsNodeData, fileData) -> {
                fileData.getChunks().put(0L, c1.getHash());
                fileData.getChunks().put((long) c1.getBytes().length, c2.getHash());
                fileData.getChunks().put((long) c1.getBytes().length + c2.getBytes().length, c3.getHash());
                return null;
            }));

            // FIXME: dhfs_files

            objectRepository.createNamespace("dhfs_files");

            jObjectRepository.writeJObject("dhfs_files", c1);
            jObjectRepository.writeJObject("dhfs_files", c2);
            jObjectRepository.writeJObject("dhfs_files", c3);
            jObjectRepository.writeJObject("dhfs_files", f);
        }

        String all = "1234567891011";

        {
            for (int start = 0; start < all.length(); start++) {
                for (int end = start; end <= all.length(); end++) {
                    var read = fileService.read(fuuid.toString(), start, end - start);
                    Assertions.assertArrayEquals(all.substring(start, end).getBytes(), read.await().indefinitely().get());
                }
            }
        }
    }

}
