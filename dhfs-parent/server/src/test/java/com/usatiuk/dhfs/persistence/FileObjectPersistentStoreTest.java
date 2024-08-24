package com.usatiuk.dhfs.persistence;

import com.google.protobuf.ByteString;
import com.usatiuk.dhfs.objects.persistence.ChunkDataP;
import com.usatiuk.dhfs.objects.persistence.JObjectDataP;
import com.usatiuk.dhfs.objects.persistence.ObjectMetadataP;
import com.usatiuk.dhfs.objects.repository.persistence.FileObjectPersistentStore;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;


class Profiles {
    public static class FileObjectPersistentStoreTestProfile implements QuarkusTestProfile {
        @Override
        public Map<String, String> getConfigOverrides() {
            var ret = new HashMap<String, String>();
            ret.put("quarkus.log.category.\"com.usatiuk.dhfs\".level", "TRACE");
            ret.put("dhfs.fuse.enabled", "false");
            ret.put("dhfs.objects.ref_verification", "true");
            return ret;
        }
    }
}

@QuarkusTest
@TestProfile(Profiles.FileObjectPersistentStoreTestProfile.class)
public class FileObjectPersistentStoreTest {
    @Inject
    FileObjectPersistentStore fileObjectPersistentStore;

    @Test
    public void writeReadFullObjectSmallMeta() {
        String name = "writeReadFullObjectSmallMeta";

        var bytes = new byte[100000];
        ThreadLocalRandom.current().nextBytes(bytes);

        ObjectMetadataP meta = ObjectMetadataP.newBuilder().setName("verycoolname123456789").build();
        JObjectDataP data = JObjectDataP.newBuilder().setChunkData(ChunkDataP.newBuilder().setData(ByteString.copyFrom(bytes)).build()).build();

        fileObjectPersistentStore.writeObjectDirect(name, meta, data);
        var readMeta = fileObjectPersistentStore.readObjectMeta(name);
        var readData = fileObjectPersistentStore.readObject(name);
        Assertions.assertEquals(meta, readMeta);
        Assertions.assertEquals(data, readData);

        var bigString = RandomStringUtils.random(100000);

        var newMeta = ObjectMetadataP.newBuilder().setName(String.valueOf(bigString)).build();
        fileObjectPersistentStore.writeObjectMetaDirect(name, newMeta);
        readMeta = fileObjectPersistentStore.readObjectMeta(name);
        readData = fileObjectPersistentStore.readObject(name);
        Assertions.assertEquals(newMeta, readMeta);
        Assertions.assertEquals(data, readData);

        fileObjectPersistentStore.writeObjectDirect(name, newMeta, null);
        readMeta = fileObjectPersistentStore.readObjectMeta(name);
        Assertions.assertEquals(newMeta, readMeta);
        Assertions.assertThrows(Throwable.class, () -> fileObjectPersistentStore.readObject(name));

        fileObjectPersistentStore.writeObjectMetaDirect(name, meta);
        readMeta = fileObjectPersistentStore.readObjectMeta(name);
        Assertions.assertEquals(meta, readMeta);
        Assertions.assertThrows(Throwable.class, () -> fileObjectPersistentStore.readObject(name));

        fileObjectPersistentStore.writeObjectDirect(name, newMeta, null);
        readMeta = fileObjectPersistentStore.readObjectMeta(name);
        Assertions.assertEquals(newMeta, readMeta);
        Assertions.assertThrows(Throwable.class, () -> fileObjectPersistentStore.readObject(name));

        fileObjectPersistentStore.writeObjectDirect(name, newMeta, data);
        readMeta = fileObjectPersistentStore.readObjectMeta(name);
        readData = fileObjectPersistentStore.readObject(name);
        Assertions.assertEquals(newMeta, readMeta);
        Assertions.assertEquals(data, readData);
    }
}
