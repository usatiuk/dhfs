package com.usatiuk.dhfs.fuse;

import com.google.protobuf.ByteOutput;
import jnr.ffi.Pointer;

import java.nio.ByteBuffer;

public class JnrPtrByteOutput extends ByteOutput {
    private final Pointer _backing;
    private final long _size;
    private long _pos;
    private final JnrPtrByteOutputAccessors _accessors;

    public JnrPtrByteOutput(JnrPtrByteOutputAccessors accessors, Pointer backing, long size) {
        _backing = backing;
        _size = size;
        _pos = 0;
        _accessors = accessors;
    }

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
        var rem = value.remaining();
        if (rem + _pos > _size) throw new IndexOutOfBoundsException();

        if (value.isDirect()) {
            long addr = _accessors.getNioAccess().getBufferAddress(value) + value.position();
            var out = _backing.address() + _pos;
            _accessors.getUnsafe().copyMemory(addr, out, rem);
        } else {
            throw new UnsupportedOperationException();
        }

        _pos += rem;
    }

    @Override
    public void writeLazy(ByteBuffer value) {
        write(value);
    }
}
