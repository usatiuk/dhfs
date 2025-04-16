package com.usatiuk.dhfs.jmap;

import com.google.protobuf.ByteString;

public interface JMapKey extends Comparable<JMapKey> {
    public ByteString value();
}
