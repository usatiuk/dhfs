package com.usatiuk.autoprotomap.it;

import com.google.protobuf.ByteString;
import com.usatiuk.autoprotomap.runtime.ProtoMirror;
import lombok.AllArgsConstructor;
import lombok.Getter;

@ProtoMirror(NestedObjectProto.class)
@AllArgsConstructor
@Getter
public class NestedObject {
    public SimpleObject object;
    public String _nestedName;
    public ByteString _nestedSomeBytes;
}
