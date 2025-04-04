package com.usatiuk.objects;

import java.nio.ByteBuffer;

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
    public byte[] bytes() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ByteBuffer toByteBuffer() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String name() {
        throw new UnsupportedOperationException();
    }
}
