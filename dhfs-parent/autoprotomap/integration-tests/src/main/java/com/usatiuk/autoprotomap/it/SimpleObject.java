package com.usatiuk.autoprotomap.it;

import com.usatiuk.autoprotomap.runtime.ProtoMirror;
import lombok.AllArgsConstructor;

@ProtoMirror(SimpleObjectProto.class)
@AllArgsConstructor
public class SimpleObject {
    public int numfield = 0;
}
