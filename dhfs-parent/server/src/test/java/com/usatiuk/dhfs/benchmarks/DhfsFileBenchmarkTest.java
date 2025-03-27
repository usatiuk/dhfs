package com.usatiuk.dhfs.benchmarks;

import com.google.protobuf.UnsafeByteOperations;
import com.usatiuk.dhfs.TempDataProfile;
import com.usatiuk.dhfs.files.service.DhfsFileService;
import com.usatiuk.objects.JObjectKey;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Map;

class Profiles {
    public static class DhfsFuseTestProfile extends TempDataProfile {
        @Override
        protected void getConfigOverrides(Map<String, String> ret) {
            ret.put("quarkus.log.category.\"com.usatiuk.dhfs\".level", "INFO");
            ret.put("dhfs.fuse.enabled", "false");
            ret.put("dhfs.objects.ref_verification", "false");
        }
    }
}

@QuarkusTest
@TestProfile(Profiles.DhfsFuseTestProfile.class)
public class DhfsFileBenchmarkTest {
    @Inject
    DhfsFileService dhfsFileService;

    @Test
    @Disabled
    void openRootTest() {
        Benchmarker.runAndPrintMixSimple("dhfsFileService.open(\"\")",
                () -> {
                    return dhfsFileService.open("");
                }, 1_000_000, 5, 1000, 5, 1000);
    }

    @Test
    @Disabled
    void writeMbTest() {
        JObjectKey file = dhfsFileService.create("/writeMbTest", 0777).get();
        var bb = ByteBuffer.allocateDirect(1024 * 1024);
        Benchmarker.runAndPrintMixSimple("dhfsFileService.write(\"\")",
                () -> {
                    var thing = UnsafeByteOperations.unsafeWrap(bb);
                    return dhfsFileService.write(file, dhfsFileService.size(file), thing);
                }, 1_000, 10, 100, 1, 100);
    }
}
