package com.usatiuk.dhfs.jmap;

import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;
import com.usatiuk.dhfs.supportlib.UninitializedByteBuffer;
import org.apache.commons.lang3.StringUtils;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.nio.ByteBuffer;

public record JMapLongKey(long key) implements JMapKey, Comparable<JMapKey>, Serializable {
    public static JMapLongKey of(long key) {
        return new JMapLongKey(key);
    }

    public static JMapLongKey max() {
        return new JMapLongKey(Long.MAX_VALUE);
    }

    public ByteString value() {
        var newByteBuffer = ByteBuffer.allocate(Long.BYTES);
        newByteBuffer.putLong(key);
        newByteBuffer.flip();
        return UnsafeByteOperations.unsafeWrap(newByteBuffer);
    }

    @Override
    public int compareTo(@Nonnull JMapKey o) {
        if (!(o instanceof JMapLongKey lk)) {
            throw new IllegalArgumentException("Unknown type of JMapKey");
        }
        return Long.compare(key, lk.key);
    }
}
