package com.usatiuk.autoprotomap.it;

import com.usatiuk.autoprotomap.runtime.ProtoMirror;
import lombok.AllArgsConstructor;

@ProtoMirror(NestedObjectProto.class)
@AllArgsConstructor
public class NestedObject {
    public SimpleObject object;
}
