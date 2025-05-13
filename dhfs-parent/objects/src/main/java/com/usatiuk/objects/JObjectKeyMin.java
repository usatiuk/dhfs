package com.usatiuk.objects;

import java.nio.ByteBuffer;

/**
 * JObjectKey implementation that compares less than all other keys.
 */
public record JObjectKeyMin() implements JObjectKey {
    @Override
    public int compareTo(JObjectKey o) {
        switch (o) {
            case JObjectKeyImpl jObjectKeyImpl -> {
                return -1;
            }
            case JObjectKeyMax jObjectKeyMax -> {
                return -1;
            }
            case JObjectKeyMin jObjectKeyMin -> {
                return 0;
            }
        }
    }

    @Override
    public ByteBuffer toByteBuffer() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String value() {
        throw new UnsupportedOperationException();
    }
}
