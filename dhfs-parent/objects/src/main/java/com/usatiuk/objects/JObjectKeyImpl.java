package com.usatiuk.objects;

import com.usatiuk.utils.UninitializedByteBuffer;

import java.io.Serial;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * A "real" implementation of JObjectKey, containing an underlying string, and a cached lazily created byte buffer.
 */
public final class JObjectKeyImpl implements JObjectKey {
    @Serial
    private static final long serialVersionUID = 0L;
    private final String value;
    private transient ByteBuffer _bb = null;

    public JObjectKeyImpl(String value) {
        this.value = value;
    }

    public JObjectKeyImpl(byte[] bytes) {
        this.value = new String(bytes, StandardCharsets.ISO_8859_1);
    }

    @Override
    public int compareTo(JObjectKey o) {
        switch (o) {
            case JObjectKeyImpl jObjectKeyImpl -> {
                return value.compareTo(jObjectKeyImpl.value());
            }
            case JObjectKeyMax jObjectKeyMax -> {
                return -1;
            }
            case JObjectKeyMin jObjectKeyMin -> {
                return 1;
            }
        }
    }

    @Override
    public String toString() {
        return value;
    }

    @Override
    public ByteBuffer toByteBuffer() {
        if (_bb != null) return _bb;

        synchronized (this) {
            if (_bb != null) return _bb;
            var bytes = value.getBytes(StandardCharsets.ISO_8859_1);
            var directBb = UninitializedByteBuffer.allocate(bytes.length);
            directBb.put(bytes);
            directBb.flip();
            _bb = directBb;
            return directBb;
        }
    }

    @Override
    public String value() {
        return value;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (JObjectKeyImpl) obj;
        return Objects.equals(this.value, that.value);
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }

}
