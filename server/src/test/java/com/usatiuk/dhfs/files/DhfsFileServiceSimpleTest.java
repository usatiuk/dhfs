package com.usatiuk.dhfs.files;

import com.usatiuk.dhfs.storage.files.objects.ChunkData;
import com.usatiuk.dhfs.storage.files.objects.ChunkInfo;
import com.usatiuk.dhfs.storage.files.objects.File;
import com.usatiuk.dhfs.storage.files.service.DhfsFileService;
import com.usatiuk.dhfs.storage.objects.jrepository.JObjectManager;
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
    JObjectManager jObjectManager;

    @Test
    void readTest() {
        var fuuid = UUID.randomUUID();
        {
            ChunkData c1 = new ChunkData("12345".getBytes());
            ChunkInfo c1i = new ChunkInfo(c1.getHash(), c1.getBytes().length);
            ChunkData c2 = new ChunkData("678".getBytes());
            ChunkInfo c2i = new ChunkInfo(c2.getHash(), c2.getBytes().length);
            ChunkData c3 = new ChunkData("91011".getBytes());
            ChunkInfo c3i = new ChunkInfo(c3.getHash(), c3.getBytes().length);
            File f = new File(fuuid);
            f.getChunks().put(0L, c1.getHash());
            f.getChunks().put((long) c1.getBytes().length, c2.getHash());
            f.getChunks().put((long) c1.getBytes().length + c2.getBytes().length, c3.getHash());

            // FIXME: dhfs_files

            jObjectManager.put(c1);
            jObjectManager.put(c2);
            jObjectManager.put(c3);
            jObjectManager.put(c1i);
            jObjectManager.put(c2i);
            jObjectManager.put(c3i);
            jObjectManager.put(f);
        }

        String all = "1234567891011";

        {
            for (int start = 0; start < all.length(); start++) {
                for (int end = start; end <= all.length(); end++) {
                    var read = fileService.read(fuuid.toString(), start, end - start);
                    Assertions.assertArrayEquals(all.substring(start, end).getBytes(), read.get());
                }
            }
        }
    }

}
