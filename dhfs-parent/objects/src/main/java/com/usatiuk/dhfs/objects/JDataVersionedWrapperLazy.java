package com.usatiuk.dhfs.objects;

import com.google.protobuf.ByteString;
import com.usatiuk.dhfs.utils.SerializationHelper;

public class JDataVersionedWrapperLazy implements JDataVersionedWrapper {
    private final long _version;
    private ByteString _rawData;
    private JData _data;

    public JDataVersionedWrapperLazy(long version, ByteString rawData) {
        _version = version;
        _rawData = rawData;
    }

    public JData data() {
        if (_data != null)
            return _data;

        synchronized (this) {
            if (_data != null)
                return _data;

            try (var is = _rawData.newInput()) {
                _data = SerializationHelper.deserialize(is);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            _rawData = null;
            return _data;
        }
    }

    public long version() {
        return _version;
    }

    @Override
    public int estimateSize() {
        if (_data != null)
            return _data.estimateSize();
        return _rawData.size();
    }
}
