package com.usatiuk.autoprotomap.it;

import com.usatiuk.autoprotomap.runtime.ProtoMirror;

@ProtoMirror(InterfaceObjectProto.class)
public interface InterfaceObject {
    String key();
}
