package com.usatiuk.dhfs.utils;

import java.nio.ByteBuffer;

public class ByteUtils {
    public static byte[] longToBytes(long val) {
        return ByteBuffer.wrap(new byte[8]).putLong(val).array();
    }

    public static long bytesToLong(byte[] bytes) {
        return ByteBuffer.wrap(bytes).getLong();
    }

    // Returns a ByteBuffer of size 8 with position reset
    public static ByteBuffer longToBb(long val) {
        return ByteBuffer.allocate(8).putLong(val).flip();
    }
}
