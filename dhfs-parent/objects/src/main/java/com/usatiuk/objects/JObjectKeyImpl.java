package com.usatiuk.objects;

import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;
import com.usatiuk.dhfs.supportlib.UninitializedByteBuffer;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Objects;

public record JObjectKeyImpl(SerializableByteBufferWrapper rawValue) implements JObjectKey {
    private static class SerializableByteBufferWrapper implements Serializable {
        @Serial
        private static final long serialVersionUID = 1L;
        private ByteBuffer byteBuffer;

        public SerializableByteBufferWrapper(ByteBuffer byteBuffer) {
            this.byteBuffer = byteBuffer;
        }

        public ByteBuffer getByteBuffer() {
            return byteBuffer;
        }

        @Serial
        private void writeObject(ObjectOutputStream out) throws IOException {
            var readOnlyBuffer = byteBuffer.asReadOnlyBuffer();
            var barr = new byte[readOnlyBuffer.remaining()];
            readOnlyBuffer.get(barr);
            out.writeInt(barr.length);
            out.write(barr);
        }

        @Serial
        private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
            int length = in.readInt();
            ByteBuffer byteBuffer = UninitializedByteBuffer.allocateUninitialized(length);
            var bytes = new byte[length];
            in.readFully(bytes);
            byteBuffer.put(bytes);
            byteBuffer.flip();
            this.byteBuffer = byteBuffer;
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) return false;
            SerializableByteBufferWrapper that = (SerializableByteBufferWrapper) o;
            return Objects.equals(byteBuffer, that.byteBuffer);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(byteBuffer);
        }
    }

    JObjectKeyImpl(ByteBuffer byteBuffer) {
        this(new SerializableByteBufferWrapper(byteBuffer));
    }

    @Override
    public int compareTo(JObjectKey o) {
        switch (o) {
            case JObjectKeyImpl jObjectKeyImpl -> {
                return rawValue.getByteBuffer().compareTo(jObjectKeyImpl.rawValue().getByteBuffer());
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
        var encoded = Base64.getEncoder().encode(toByteBuffer());
        return StandardCharsets.US_ASCII.decode(encoded).toString();
    }

    @Override
    public ByteString value() {
        return UnsafeByteOperations.unsafeWrap(toByteBuffer());
    }

    @Override
    public ByteBuffer toByteBuffer() {
        return rawValue.getByteBuffer().asReadOnlyBuffer();
    }

}
