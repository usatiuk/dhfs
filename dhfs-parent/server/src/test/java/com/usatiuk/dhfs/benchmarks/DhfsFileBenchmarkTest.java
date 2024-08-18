package com.usatiuk.dhfs.benchmarks;

import com.usatiuk.dhfs.files.service.DhfsFileService;
import io.quarkus.logging.Log;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.junit.jupiter.api.Test;

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
        Benchmarker.runThroughput(
                () -> {
                    return dhfsFileService.open("");
                }, 5, 1000
        );
        var res = Benchmarker.runLatency(
                () -> {
                    return dhfsFileService.open("");
                }, 100_000);
        DescriptiveStatistics stats = new DescriptiveStatistics();
        for (var r : res) {
            stats.addValue(r);
        }
        Log.info(
                "\n" + stats.toString() +
                        "\n 50%: " + stats.getPercentile(50) +
                        "\n 90%: " + stats.getPercentile(90) +
                        "\n 95%: " + stats.getPercentile(95) +
                        "\n 99%: " + stats.getPercentile(99) +
                        "\n 99.9%: " + stats.getPercentile(99.9) +
                        "\n 99.99%: " + stats.getPercentile(99.99)
        );
    }
}
