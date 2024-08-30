package com.usatiuk.autoprotomap.it;

import com.google.protobuf.ByteString;
import com.usatiuk.autoprotomap.runtime.ProtoMirror;
import lombok.AllArgsConstructor;
import lombok.Getter;

@ProtoMirror(SimpleObjectProto.class)
@AllArgsConstructor
@Getter
public class SimpleObject {
    public int numfield = 0;
    private String name;
    public ByteString someBytes;
}
