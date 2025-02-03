package com.usatiuk.autoprotomap.it;

import com.usatiuk.autoprotomap.runtime.ProtoMirror;

@ProtoMirror(RecordObjectProto.class)
public record RecordObject(String key) implements InterfaceObject {
}
