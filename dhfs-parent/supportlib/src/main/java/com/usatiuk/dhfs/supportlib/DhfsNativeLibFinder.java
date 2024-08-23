package com.usatiuk.dhfs.supportlib;

import java.nio.file.Path;

class DhfsNativeLibFinder {
    static private final String LibName = "libdhfs_support";

    static Path getLibPath() {
        var override = System.getProperty("com.usatiuk.dhfs.supportlib.native-path-override");
        if (override != null)
            return Path.of(override);
        return Path.of(System.getProperty("com.usatiuk.dhfs.supportlib.native-path"))
                .resolve(SysUtils.getLibPlatform() + "-" + SysUtils.getLibArch()).resolve(LibName + "." + SysUtils.getLibExtension());
    }
}
