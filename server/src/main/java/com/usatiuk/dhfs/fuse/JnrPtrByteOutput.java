package com.usatiuk.dhfs.fuse;

import com.google.protobuf.ByteOutput;
import jnr.ffi.Pointer;

import java.nio.ByteBuffer;

public class JnrPtrByteOutput extends ByteOutput {
    public JnrPtrByteOutput(Pointer backing, long size) {
        _backing = backing;
        _size = size;
        _pos = 0;
    }

    private final Pointer _backing;
    private final long _size;
    private long _pos;

    @Override
    public void write(byte value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void write(byte[] value, int offset, int length) {
        if (length + _pos > _size) throw new IndexOutOfBoundsException();
        _backing.put(_pos, value, offset, length);
        _pos += length;
    }

    @Override
    public void writeLazy(byte[] value, int offset, int length) {
        if (length + _pos > _size) throw new IndexOutOfBoundsException();
        _backing.put(_pos, value, offset, length);
        _pos += length;
    }

    @Override
    public void write(ByteBuffer value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeLazy(ByteBuffer value) {
        throw new UnsupportedOperationException();
    }
}
