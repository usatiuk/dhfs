package com.usatiuk.dhfs;

import io.quarkus.test.junit.QuarkusTestProfile;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

abstract public class TempDataProfile implements QuarkusTestProfile {
    protected void getConfigOverrides(Map<String, String> toPut) {
    }

    @Override
    final public Map<String, String> getConfigOverrides() {
        Path tempDirWithPrefix;
        try {
            tempDirWithPrefix = Files.createTempDirectory("dhfs-test");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        var ret = new HashMap<String, String>();
        ret.put("dhfs.objects.persistence.files.root", tempDirWithPrefix.resolve("dhfs_root_test").toString());
        ret.put("dhfs.fuse.root", tempDirWithPrefix.resolve("dhfs_fuse_root_test").toString());
        getConfigOverrides(ret);
        return ret;
    }
}
