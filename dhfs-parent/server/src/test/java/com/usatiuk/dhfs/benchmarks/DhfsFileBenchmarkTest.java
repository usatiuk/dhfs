package com.usatiuk.dhfs.benchmarks;

import com.google.protobuf.UnsafeByteOperations;
import com.usatiuk.dhfs.files.service.DhfsFileService;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

class Profiles {
    public static class DhfsFuseTestProfile implements QuarkusTestProfile {
        @Override
        public Map<String, String> getConfigOverrides() {
            var ret = new HashMap<String, String>();
            ret.put("quarkus.log.category.\"com.usatiuk.dhfs\".level", "INFO");
            ret.put("dhfs.fuse.enabled", "false");
            ret.put("dhfs.objects.ref_verification", "false");
            return ret;
        }
    }
}

@QuarkusTest
@TestProfile(Profiles.DhfsFuseTestProfile.class)
public class DhfsFileBenchmarkTest {
    @Inject
    DhfsFileService dhfsFileService;

    @Test
    void openRootTest() {
        Benchmarker.runAndPrintMixSimple("dhfsFileService.open(\"\")",
                () -> {
                    return dhfsFileService.open("");
                }, 1_000_000, 5, 1000, 5, 1000);
    }

    @Test
    @Disabled
    void writeMbTest() {
        String file = dhfsFileService.create("/writeMbTest", 0777).get();
        var bb = ByteBuffer.allocateDirect(1024 * 1024);
        Benchmarker.runAndPrintMixSimple("dhfsFileService.write(\"\")",
                () -> {
                    var thing = UnsafeByteOperations.unsafeWrap(bb);
                    return dhfsFileService.write(file, dhfsFileService.size(file), thing);
                }, 1_000, 10, 100, 1, 100);
    }
}