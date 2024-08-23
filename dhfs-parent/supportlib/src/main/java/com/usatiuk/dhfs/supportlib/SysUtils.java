package com.usatiuk.dhfs.supportlib;

import org.apache.commons.lang3.SystemUtils;

class SysUtils {
    static String getLibPlatform() {
        if (SystemUtils.IS_OS_MAC) {
            return "Darwin";
        } else if (SystemUtils.IS_OS_LINUX) {
            return "Linux";
        } else {
            throw new IllegalStateException("Unsupported OS: " + SystemUtils.OS_NAME);
        }
    }

    static String getLibExtension() {
        if (SystemUtils.IS_OS_MAC) {
            return "dylib";
        } else if (SystemUtils.IS_OS_LINUX) {
            return "so";
        } else {
            throw new IllegalStateException("Unsupported OS: " + SystemUtils.OS_NAME);
        }
    }

    static String getLibArch() {
        if (SystemUtils.IS_OS_MAC) {
            return switch (SystemUtils.OS_ARCH) {
                case "aarch64" -> "arm64";
                default -> throw new IllegalStateException("Unsupported architecture: " + SystemUtils.OS_ARCH);
            };
        } else if (SystemUtils.IS_OS_LINUX) {
            return switch (SystemUtils.OS_ARCH) {
                case "aarch64" -> "aarch64";
                case "amd64" -> "x86_64";
                default -> throw new IllegalStateException("Unsupported architecture: " + SystemUtils.OS_ARCH);
            };
        } else {
            throw new IllegalStateException("Unsupported OS: " + SystemUtils.OS_NAME);
        }

    }
}
