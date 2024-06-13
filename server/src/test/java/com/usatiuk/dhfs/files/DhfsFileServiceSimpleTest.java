package com.usatiuk.dhfs.files;

import com.usatiuk.dhfs.storage.SimpleFileRepoTest;
import com.usatiuk.dhfs.storage.files.objects.Chunk;
import com.usatiuk.dhfs.storage.files.objects.File;
import com.usatiuk.dhfs.storage.files.service.DhfsFileService;
import com.usatiuk.dhfs.storage.objects.repository.ObjectRepository;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.apache.commons.lang3.SerializationUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.UUID;

@QuarkusTest
public class DhfsFileServiceSimpleTest extends SimpleFileRepoTest {
    @Inject
    DhfsFileService fileService;
    @Inject
    ObjectRepository objectRepository;

    @Test
    void readTest() {
        var fuuid = UUID.randomUUID();
        {
            Chunk c1 = new Chunk("12345".getBytes());
            Chunk c2 = new Chunk("678".getBytes());
            Chunk c3 = new Chunk("91011".getBytes());
            File f = new File();
            f.getChunks().put(0L, c1.getHash());
            f.getChunks().put((long) c1.getBytes().length, c2.getHash());
            f.getChunks().put((long) c1.getBytes().length + c2.getBytes().length, c3.getHash());

            // FIXME: dhfs_files

            objectRepository.createNamespace("dhfs_files");

            objectRepository.writeObject("dhfs_files", c1.getHash(), ByteBuffer.wrap(SerializationUtils.serialize(c1))).await().indefinitely();
            objectRepository.writeObject("dhfs_files", c2.getHash(), ByteBuffer.wrap(SerializationUtils.serialize(c2))).await().indefinitely();
            objectRepository.writeObject("dhfs_files", c3.getHash(), ByteBuffer.wrap(SerializationUtils.serialize(c3))).await().indefinitely();
            objectRepository.writeObject("dhfs_files", fuuid.toString(), ByteBuffer.wrap(SerializationUtils.serialize(f))).await().indefinitely();
        }

        String all = "1234567891011";

        {
            for (int start = 0; start < all.length(); start++) {
                for (int end = start; end < all.length(); end++) {
                    var read = fileService.read(fuuid.toString(), start, end - start);
                    Assertions.assertArrayEquals(all.substring(start, end).getBytes(), read.await().indefinitely().get());
                }
            }
        }
    }

}
