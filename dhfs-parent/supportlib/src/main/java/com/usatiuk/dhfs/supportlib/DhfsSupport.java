package com.usatiuk.dhfs.supportlib;

import java.nio.file.Path;

public class DhfsSupport {
    static private final String LibName = "libdhfs_support";

    static private Path getLibPath() {
        return Path.of(System.getProperty("com.usatiuk.dhfs.supportlib.native-path"))
                .resolve(SysUtils.getLibPlatform() + "-" + SysUtils.getLibArch()).resolve(LibName + "." + SysUtils.getLibExtension());
    }

    static {
        System.load(getLibPath().toAbsolutePath().toString());
    }

    public static void hello() {
        DhfsSupportNative.hello();
    }
}
